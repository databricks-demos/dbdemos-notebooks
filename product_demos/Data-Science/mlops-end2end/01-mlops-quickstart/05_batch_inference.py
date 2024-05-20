# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC ## Inference with the Champion model
# MAGIC
# MAGIC With models registered in the Unity Catalog Model Registry, they can be loaded for use in batch inference pipelines. The generated predictions can used to devise customer retention strategies, or be used for analytics. The model in use is the __Champion__ model, and we will load this for use in our pipeline.
# MAGIC
# MAGIC ## Champion-Challenger testing
# MAGIC
# MAGIC In earlier steps, we have registered a __Challenger__ model. Later on in this notebook, we will look at the concept of Champion-Challenger testing, which ensures that the __Challenger__ model would not cause adverse business impact before letting it replace the Champion model.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-6.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F06_staging_inference&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Load the model from MLFLow and run inferences, in batch or realtime.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install "mlflow-skinny[databricks]>=2.11"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC
# MAGIC Now that our model is available in the Unity Catalog Model Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry !

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inferences

# COMMAND ----------

model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version # Get champion version
print(f"Champion model version for {model_name}: {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will load the model as a Spark UDF and generate predictions for our customer records.
# MAGIC
# MAGIC For simplicity, we assume that features have been extracted for the new customer records and these are already stored in the feature table. These are typically done by separate feature engineering pipelines.

# COMMAND ----------

# DBTITLE 1,In a python notebook
# Load customer features to be scored
feature_df = spark.read.table(f"{catalog}.{db}.mlops_churn_features")

# Load champion model as a Spark UDF
champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@Champion")

# Batch score
champion_pred_df = feature_df.withColumn('prediction', champion_model(*feature_df.columns))

display(champion_pred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! Our data can now be saved as a table and re-used by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!

# COMMAND ----------

# MAGIC %md
# MAGIC # Champion-Challenger testing
# MAGIC
# MAGIC When deploying new models, organizations can adopt different strategies to mitigate risks related to models not being fully tested against real-world data.
# MAGIC
# MAGIC We will illustrate one of such strategies through Champion-Challenger testing.
# MAGIC
# MAGIC Along with the predictions, we will register information about the model that is used. In particular, whether the model used is the Champion or the Challenger model. This will allow us to capture business metrics associated with the models, and promote a model from Champion to Challenger only if it produces acceptable business metrics.
# MAGIC
# MAGIC One example of such a metric could be the customer retention cost. A Challenger model may predict an exceedingly high number of customers who will churn. This may result in prohibitive customer retention costs. Furthermore, when a new model predicts a vastly different churn rate than its predecessor, or than what it predicts during development and testing, these are signs that further investigation has to be done before promoting the model to Champion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capture metadata for the model used for inference
# MAGIC
# MAGIC To keep a record on which model is used to generate the predictions, so that we can make comparisons of the Challenger against the Champion later, we capture a number of information on the model used. This includes the date the precition was made, the model name, the model version and its alias (`Champion` or `Challenger`)
# MAGIC
# MAGIC We will use the feature table retreived above to perform the Champion-Challenger evaluation. In practice, this could come from customer records that are recently collected, or a baseline/"golden" dataset.
# MAGIC
# MAGIC We score the records using both the Champion and Challenger models, and compare the business metrics.

# COMMAND ----------

import pyspark.sql.functions as F

# We have already found the predictions for the Champion model
# Add columns to record prediction time and model information
predictions_df = (
  champion_pred_df.withColumn('prediction_date', F.current_timestamp())
                  .withColumn('model', F.lit(model_name))
                  .withColumn('model_version', F.lit(model_version))
                  .withColumn('model_alias', F.lit("Champion"))
)

display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference on the Challenger model
# MAGIC
# MAGIC Next, we get the predictions using the Challenger model.

# COMMAND ----------

# Get model version of Challenger model
model_version = client.get_model_version_by_alias(name=model_name, alias="Challenger").version
print(f"Challenger model version for {model_name}: {model_version}")

# Load challenger model as a Spark UDF
challenger_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@Challenger")

# Batch score
challenger_pred_df = feature_df.withColumn('prediction', challenger_model(*feature_df.columns))

display(challenger_pred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capture metadata for the Challenger model
# MAGIC
# MAGIC Like we did with the Champion model, we capture information on the model used to produce these predictions.
# MAGIC
# MAGIC In practice, the resulting table with the predictions and model metadata can be saved as a table for ongoing analyses. We will skip writing the table in this demo for simplicity.

# COMMAND ----------

# Add columns to record prediction time and model information for the Challenger model
# Combine the information with the Champion model for analysis
predictions_all_df = predictions_df.union(
    challenger_pred_df.withColumn("prediction_date", F.current_timestamp())
                      .withColumn("model", F.lit(model_name))
                      .withColumn("model_version", F.lit(model_version))
                      .withColumn("model_alias", F.lit("Challenger"))
)

display(predictions_all_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Champion and Challenger business metrics
# MAGIC
# MAGIC Now that we have the necessary predictions and information on the models used, we can use this data to perform our Champion-Challenger analysis agsint our business KPIs.

# COMMAND ----------

from pyspark.sql import Window

window = Window.partitionBy("model_alias")

summary_df = (
    predictions_all_df.groupBy("model_alias", "prediction").agg(
        F.countDistinct("customer_id").alias("nb_customers"),
        F.sum("total_charges").alias("revenue_impacted")
    ).orderBy("model_alias", "prediction")
    .withColumn("total_customers", F.sum("nb_customers").over(window))
    .withColumn("pct_customers", F.col("nb_customers") / F.col("total_customers") * 100)
    .withColumn("total_revenue", F.sum("revenue_impacted").over(window))
    .withColumn("pct_revenue", F.col("revenue_impacted") / F.col("total_revenue") * 100)
    .drop("total_customers", "total_revenue")
)

display(summary_df)

# COMMAND ----------

# Convert to pandas for easier transformation and plotting

summary_pdf = summary_df.toPandas()

summary_val_pdf = pd.melt(
    summary_pdf,
    id_vars=["model_alias", "prediction"],
    value_vars=["nb_customers", "revenue_impacted"],
    var_name="metric",
)

summary_pct_pdf = pd.melt(
    summary_pdf,
    id_vars=["model_alias", "prediction"],
    value_vars=["pct_customers", "pct_revenue"],
    var_name="metric",
)

summary_tall_pdf = summary_val_pdf.merge(
    summary_pct_pdf, left_index=True, right_index=True, suffixes=["", "_pct"]
)

summary_tall_pdf.drop(
    columns=["model_alias_pct", "prediction_pct", "metric_pct"], inplace=True
)
summary_tall_pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize results
# MAGIC
# MAGIC Run the next cell to visualize the results. We are analyzing:
# MAGIC
# MAGIC - Churn rate / Number of churns predicted (top)
# MAGIC - Revenue impacted by predicted churners (bottom)
# MAGIC
# MAGIC The results you get on your plot may differ from the percentages stated here due to randomness in the data generated for the demo.
# MAGIC
# MAGIC The churn rate predicted by our Challenger model on the reference dataset is not drastically different from that predicted by the Champion model currently in use (churn rates are around 21% and 22%). This suggests that there is a low risk that retention costs can become prohibitive, and the two models are not giving drastically different results.
# MAGIC
# MAGIC The percentage of churn revenue predicted by the Challenger model differs by only a few percentage points, indicating that the model does not behave drastically differently from the Champion.
# MAGIC
# MAGIC Based on these findings, we can proceed to promote the Challenger model to replace the current Champion!
# MAGIC
# MAGIC Note that in practice, the business KPI and decision criteria can vary. We may consider collecting ground truth data to compare the models' ability to predict real churners, or its coverage of the real revenue affected by churn. In many cases, ground truths are either not available, or take a long time to become available. In some cases, businesses find it feasible to keep a reference dataset (or a "golden dataset") to perform such evaluation. If you have either available, you can save them as Delta tables and use them for KPI comparison.

# COMMAND ----------

# Plot to compare customer churn rate and revenue impacted
# predicted by the models

import plotly.express as px

fig = px.bar(
    summary_tall_pdf,
    x="model_alias",
    y="value",
    facet_col="metric",
    facet_col_wrap=1,
    facet_row_spacing=0.05,
    color="prediction",
    text="value_pct",
    color_discrete_sequence=px.colors.qualitative.D3,
    title="Champion-Challenger analysis",
    height=600,
    width=600,
)
fig.update_traces(texttemplate='%{text:.3s}%', textposition='inside')
fig.update_yaxes(matches=None)
fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig.update_layout(
    margin=dict(l=20, r=20, t=50, b=50),
)
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting the Challenger to Champion
# MAGIC
# MAGIC When we are satisfied with the results of the __Challenger__ model, we can then promote it to Champion. This is done by setting its alias to `@Champion`. Production pipelines that load the model using the `Champion` alias will then be loading this new model. The alias on the older Champion model will be automatically unset.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC model_version = client.get_model_version_by_alias(name=model_name, alias="Challenger").version
# MAGIC
# MAGIC client.set_registered_model_alias(
# MAGIC   name=model_name,
# MAGIC   alias="Champion",
# MAGIC   version=model_version
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC This is all for the quickstart demo! We have looked at basic concepts of MLOps and how Databricks helps you achieve them. They include:
# MAGIC
# MAGIC - Feature engineering and storing feature tables in Databricks
# MAGIC - AutoML, model training and experiement tracking in MLflow
# MAGIC - Register models in the Unity Catalog Model Registry for use by runtime systems
# MAGIC - Model validation and promotion
# MAGIC - Batch inference
# MAGIC - Champion-Challenger testing
# MAGIC
# MAGIC We hope you've enjoyed this demo. As the next step, look out for our Advanced End-to-end MLOps demo, which will include more in-depth walkthroughs on the following aspects of MLOps:
# MAGIC
# MAGIC - Feature serving and Feature Store
# MAGIC - Data and model monitoring
# MAGIC - Deployment for real-time inference
