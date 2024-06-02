# Databricks notebook source
# MAGIC %md
# MAGIC # Challenger model validation
# MAGIC
# MAGIC This notebook performs validation tasks on the candidate __Challenger__ model.
# MAGIC
# MAGIC It goes through a few steps to validate the model before labelling it (by setting its alias) to `Challenger`.
# MAGIC
# MAGIC When organizations first start to put MLOps processes in place, they should consider having a "human-in-the-loop" to perform visual analyses to validate models before promoting them. As they get more familiar with the process, they can consider automating the steps in a __Workflow__ . The benefits of automation is to ensure that these validation checks are systematically performed before new models are integrated into inference pipelines or deployed for realtime serving. Of course, organizations can opt to retain a "human-in-the-loop" in any part of the process and put in place the degree of automation that suits its business needs.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-5.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F05_job_staging_validation&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Trigger Model testing and validation job.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## General Validation Checks
# MAGIC
# MAGIC <!--img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 -->
# MAGIC
# MAGIC In the context of MLOps, there are more tests than simply how accurate a model will be.  To ensure the stability of our ML system and compliance with any regulatory requirements, we will subject each model added to the registry to a series of validation checks.  These include, but are not limited to:
# MAGIC <br>
# MAGIC * __Model documentation__
# MAGIC * __Inference on production data__
# MAGIC * __Champion-Challenger testing to ensure that business KPIs are acceptable__
# MAGIC
# MAGIC In this notebook we explore some approaches to performing these tests, and how we can add metadata to our models with tagging if they have passed a given test or not.
# MAGIC
# MAGIC This part is typically specific to your line of business and quality requirements.
# MAGIC
# MAGIC For each test, we'll add information using tags to know what has been validated in the model. We can also add Comments to a model if needed.

# COMMAND ----------

# MAGIC %pip install "mlflow-skinny[databricks]>=2.11"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $setup_inference_data=true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information
# MAGIC
# MAGIC We will fetch the model information for the __Challenger__ model from Unity Catalog.

# COMMAND ----------

# We are interested in validating the Challenger model
model_alias = "Challenger"

client = MlflowClient()
model_details = client.get_model_version_by_alias(model_name, model_alias)
model_version = int(model_details.version)

print(f"Validating {model_alias} model for {model_name} on model version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model checks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description check
# MAGIC
# MAGIC Has the data scientist provided a description of the model being submitted?

# COMMAND ----------

# If there's no description or an insufficient number of charaters, tag accordingly
if not model_details.description:
  client.set_model_version_tag(name=model_name, version=model_details.version, key="has_description", value=False)
  print("Please add model description")
elif not len(model_details.description) > 20:
  client.set_model_version_tag(name=model_name, version=model_details.version, key="has_description", value=False)
  print("Please add detailed model description (40 char min).")
else:
  client.set_model_version_tag(name=model_name, version=str(model_details.version), key="has_description", value=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Validate prediction
# MAGIC
# MAGIC We want to test to see that the model can predict on production data.  So, we will load the model and the data from a table with validation data and test making some predictions.

# COMMAND ----------

from pyspark.sql.types import StructType

# Predict on a Spark DataFrame
try:
  # Read validation table
  validation_df = spark.read.table("mlops_churn_validation")

  # Load model as a Spark UDF
  model_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@{model_alias}")

  # Batch score
  validation_w_preds = validation_df.withColumn("predictions", model_udf(*validation_df.columns))
  display(validation_w_preds)
  client.set_model_version_tag(name=model_name, version=model_details.version, key="predicts", value=True)

except Exception as e:
  print(e)
  validation_w_preds = spark.createDataFrame([], StructType([]))
  print("Unable to predict on features.")
  client.set_model_version_tag(name=model_name, version=model_details.version, key="predicts", value=False)
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Champion-Challenger testing
# MAGIC
# MAGIC When deploying new models, organizations can adopt different strategies to mitigate risks related to models not being fully tested against real-world data.
# MAGIC
# MAGIC We will illustrate one of such strategies through Champion-Challenger testing.
# MAGIC
# MAGIC From the predictions made by the model, we will calculate business metrics associated with the models. From there, we can then decide to promote a model from Champion to Challenger only if it produces acceptable business metrics.
# MAGIC
# MAGIC One example of such a metric could be the customer retention cost. A Challenger model may predict an exceedingly high number of customers who will churn. This may result in prohibitive customer retention costs. Furthermore, when a new model predicts a vastly different churn rate than its predecessor, or than what it predicts during development and testing, these are signs that further investigation has to be done before promoting the model to Champion.
# MAGIC
# MAGIC In the event that a validation dataset with ground truth is available, we can also capture metrics that are based on correct predictions made by the model. Examples of these metrics include the percentage of actual churn customers predicted, or the amount of actual churn revenue correctly captured by the model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inference on the Challenger model

# COMMAND ----------

# Load validation data
validation_df = spark.read.table("mlops_churn_validation")

# Load challenger model as a Spark UDF
challenger_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@{model_alias}")

# Batch score
challenger_pred_df = validation_df.withColumn('predictions', challenger_model(*validation_df.columns))

display(challenger_pred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capture metadata for the Challenger model used for inference
# MAGIC
# MAGIC Along with the predictions, we would like to register information about the model that is used. In particular, whether the model used is the Champion or the Challenger model. This allows us to keep a record on which model is used to generate the predictions. We can use this information later to make comparisons of the Challenger against the Champion.
# MAGIC
# MAGIC We will capture a number of information on the model used. This includes the date the prediction was made, the model name, the model version and its alias (`Champion` or `Challenger`)
# MAGIC
# MAGIC We score the records using both the Challenger and Champion (if available) models, and compare the business metrics.

# COMMAND ----------

import pyspark.sql.functions as F

# We have already found the predictions for the Challenger model
# Add columns to record prediction time and model information
predictions_df = (
  challenger_pred_df.withColumn('prediction_date', F.current_timestamp())
                    .withColumn('model', F.lit(model_name))
                    .withColumn('model_version', F.lit(model_version))
                    .withColumn('model_alias', F.lit(model_alias))
)

display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIONAL: Inference on the Champion model
# MAGIC
# MAGIC To make a comparison of the Challenger against the __Champion__, we would also have obtained the Champion's predictions on the validation data. However, since we do not have a Champion model already deployed for this demo, we will leave the code commented and skip this step.

# COMMAND ----------

# # Get model version of Champion model
# champion_model_version = int(client.get_model_version_by_alias(name=model_name, alias="Champion").version)
# print(f"Champion model version for {model_name}: {champion_model_version}")

# # Load Champion model as a Spark UDF
# champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@Champion")

# # Batch score
# champion_pred_df = validation_df.withColumn('predictions', champion_model(*validation_df.columns))

# display(champion_pred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIONAL: Capture metadata for the Champion model
# MAGIC
# MAGIC Like we did with the Challenger model, we capture information on the model used to produce these predictions.
# MAGIC
# MAGIC In practice, the resulting table with the predictions and model metadata can be saved as a table for ongoing analyses. We will skip writing the table in this demo for simplicity.

# COMMAND ----------

# # Add columns to record prediction time and model information for the Champion model
# # Combine the information with the Challenger model for analysis
# predictions_all_df = predictions_df.union(
#     champion_pred_df.withColumn("prediction_date", F.current_timestamp())
#                     .withColumn("model", F.lit(model_name))
#                     .withColumn("model_version", F.lit(champion_model_version))
#                     .withColumn("model_alias", F.lit("Champion"))
# )

# display(predictions_all_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC __NOTE:__
# MAGIC
# MAGIC Run the next cell.
# MAGIC
# MAGIC Comment it only if you modified earlier parts of this notebook to generate predictions from a Champion model.

# COMMAND ----------

# predictions_all_df should have predictions from both the Champion and Challenger models
# Set predictions_all_df to predictions_df for this demo, which only has the Challenger's predictions
# Do not run this cell if you collected predictions from the Champion model in champion_pred_df

predictions_all_df = predictions_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Champion and Challenger business metrics
# MAGIC
# MAGIC Now that we have the necessary predictions and information on the models used, we can use this data to perform our Champion-Challenger analysis agsint our business KPIs.
# MAGIC
# MAGIC First, we look at the number of customers that the model predicts to churn, as well as the predicted churn rate and the associated revenue attributed to the predicted churners. This type of checks does not require the ground truth to be known. It can be used to check if the model predicts an exceedingly high number of churners, for example, which may warrant for additional investigation before promoting the model.

# COMMAND ----------

from pyspark.sql import Window

# Calculate an aggregated summary dataframe

window = Window.partitionBy("model_alias")

summary_df = (
    predictions_all_df.groupBy("model_alias", "predictions").agg(
        F.countDistinct("customer_id").alias("nb_customers"),
        F.sum("total_charges").alias("revenue_impacted")
    ).orderBy("model_alias", "predictions")
    .withColumn("total_customers", F.sum("nb_customers").over(window))
    .withColumn("pct_customers", F.col("nb_customers") / F.col("total_customers") * 100)
    .withColumn("total_revenue", F.sum("revenue_impacted").over(window))
    .withColumn("pct_revenue", F.col("revenue_impacted") / F.col("total_revenue") * 100)
    .drop("total_customers", "total_revenue")
)

display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC In this demo, our validation dataset has infomation on whether the customers really churned (i.e. the ground truth). We can use it to find business KPIs on model performance. For example, what percentage of the actual churners does the model predict correctly? Of the predictions made by the model, how much of the actual churn revenue is impacted as a result of the model's predictions?

# COMMAND ----------

from pyspark.sql import Window

# Generate summary KPIs for only customers who churned

window_churn = Window.partitionBy("model_alias", "churn")

summary_churn_df = (
    predictions_all_df.groupBy("model_alias", "churn", "predictions").agg(
        F.countDistinct("customer_id").alias("nb_customers"),
        F.sum("total_charges").alias("revenue_impacted")
    ).orderBy("model_alias", "churn", "predictions")
    .withColumn("customers_by_truth", F.sum("nb_customers").over(window_churn))
    .withColumn("revenue_by_truth", F.sum("revenue_impacted").over(window_churn))
    .withColumn("pct_customers", F.col("nb_customers") / F.col("customers_by_truth") * 100) 
    .withColumn("pct_revenue", F.col("revenue_impacted") / F.col("revenue_by_truth") * 100)
    .filter(F.col("churn") == 'Yes')
    .drop("customers_by_truth", "revenue_by_truth")
)

display(summary_churn_df)

# COMMAND ----------

# Convert summary dataframe into a tall pandas dataframe for easier transformation and plotting

def process_summary(summary_df):
  summary_pdf = summary_df.toPandas()

  summary_val_pdf = pd.melt(
      summary_pdf,
      id_vars=["model_alias", "predictions"],
      value_vars=["nb_customers", "revenue_impacted"],
      var_name="metric",
  )

  summary_pct_pdf = pd.melt(
      summary_pdf,
      id_vars=["model_alias", "predictions"],
      value_vars=["pct_customers", "pct_revenue"],
      var_name="metric",
  )

  summary_tall_pdf = summary_val_pdf.merge(
      summary_pct_pdf, left_index=True, right_index=True, suffixes=["", "_pct"]
  )

  summary_tall_pdf.drop(
      columns=["model_alias_pct", "predictions_pct", "metric_pct"], inplace=True
  )

  return summary_tall_pdf

# COMMAND ----------

summary_pdf = process_summary(summary_df)
summary_pdf

# COMMAND ----------

summary_churn_pdf = process_summary(summary_churn_df)
summary_churn_pdf

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize results
# MAGIC
# MAGIC Run the next cells to visualize the results.
# MAGIC
# MAGIC In the first plot, we are analyzing:
# MAGIC
# MAGIC - Churn rate / Number of churns predicted (top)
# MAGIC - Revenue impacted by predicted churners (bottom)
# MAGIC
# MAGIC The results you get on your plot may differ from the percentages stated here due to randomness in the data generated for the demo.
# MAGIC
# MAGIC When you run this demo, you will only have results from the Challenger model. In actual practice, this would be compared against results from the Champion model.
# MAGIC
# MAGIC The churn rate predicted by our Challenger model on the validation dataset is around 21%. If this does not differ much from that of the Champion model, it would suggest that there is a low risk that retention costs can become prohibitive, and the two models are not giving drastically different results.
# MAGIC
# MAGIC Likewise, if the percentage of churn revenue predicted by the Challenger model (roughly 7.5%) does not differ much from the Champion model, it would indicate that the model does not behave drastically differently from the Champion.

# COMMAND ----------

# DBTITLE 1,Define plotting function
# Define plotting functions to compare champion and challenger models

import plotly.express as px

def plot_summary(summary_pdf, title):

  fig = px.bar(
      summary_pdf,
      x="model_alias",
      y="value",
      facet_col="metric",
      facet_col_wrap=1,
      facet_row_spacing=0.05,
      color="predictions",
      text="value_pct",
      color_discrete_sequence=px.colors.qualitative.D3,
      title=title,
      height=600,
      width=600,
  )
  fig.update_traces(texttemplate='%{text:.3s}%', textposition='inside')
  fig.update_yaxes(matches=None)
  fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
  fig.update_layout(
      margin=dict(l=20, r=20, t=50, b=50),
  )

  return fig

# COMMAND ----------

# Plot to compare customer churn rate and revenue impacted
# predicted by the models

plot_summary(summary_pdf, "Champion-Challenger analysis - prediction impact")

# COMMAND ----------

# MAGIC %md
# MAGIC The next analysis looks at how the model fared on its predictions on actual churners in the validation data. We look at:
# MAGIC
# MAGIC - the percentage of churners it correctly predicts, or the recacll (top)
# MAGIC - the amount of revenue attributed to the churners it correctly predicts (bottom)
# MAGIC
# MAGIC We are interested to know:
# MAGIC
# MAGIC - Is it able to predict churners better than the Champion model?
# MAGIC - Is it able to help the business retain a larger share of the churn revenue compared to the Champion model?
# MAGIC
# MAGIC The Challenger model predicts 54% of churners correctly, and the revenue attributed to these predicted churners is 27% of all the revenue lost through churners. (Your results may differ due to randomness of the data.)

# COMMAND ----------

# Plot to compare how many customers and how much of revenue
# from customers who actually churned are correctly predicted
# by the models

plot_summary(summary_churn_pdf, "Champion-Challenger analysis - Customers who really churned")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Once these tests are completed, we can set a tag on the model version to indicate if it passed or failed.
# MAGIC
# MAGIC Note that in practice, the business KPI and decision criteria can vary. We may consider collecting ground truth data to compare the models' ability to predict real churners. In many cases, ground truths are either not available, or take a long time to become available. In some cases, businesses find it feasible to keep a reference dataset (or a "golden dataset") to perform such evaluation, like the validation dataset we used in this demo. If you have either available, you can save them as Delta tables and use them for KPI comparison.

# COMMAND ----------

pass_kpi_tests = 'pass' # 'pass' or 'fail'

client.set_model_version_tag(name=model_name, version=model_details.version, key="champion_challenger_kpis", value=pass_kpi_tests)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation results
# MAGIC
# MAGIC Here's a summary of the validation results.
# MAGIC
# MAGIC Based on these findings, we can proceed to promote the Challenger model to replace the current Champion!

# COMMAND ----------

results = client.get_model_version(model_name, model_version)
results.tags

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting the Challenger to Champion
# MAGIC
# MAGIC When we are satisfied with the results of the __Challenger__ model, we can then promote it to Champion. This is done by setting its alias to `@Champion`. Inference pipelines that load the model using the `@Champion` alias will then be loading this new model. The alias on the older Champion model, if there is one, will be automatically unset. The model retains its `@Challenger` alias until a newer Challenger model is deployed with the alias to replace it.

# COMMAND ----------

client.set_registered_model_alias(
  name=model_name,
  alias="Champion",
  version=model_version
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations, our model is now validated and promoted accordingly
# MAGIC
# MAGIC We now have the certainty that our model is ready to be used in inference pipelines and in realtime serving endpoints, as it matches our validation standards.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our newly promoted Champion model]($./05_batch_inference)
