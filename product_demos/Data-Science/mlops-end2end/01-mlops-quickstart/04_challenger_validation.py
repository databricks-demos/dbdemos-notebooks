# Databricks notebook source
# MAGIC %md
# MAGIC # Model validation
# MAGIC
# MAGIC This notebook performs validation tasks on the candidate Challenger model.
# MAGIC
# MAGIC It can be automated as a **Databricks Workflow job** and will programatically validate the model before labelling it (by setting its alias) to `Challenger`. The benefits of automation is to ensure that these validation checks are systematically performed before new models are integrated into inference pipelines or deployed for realtime serving.
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
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 >
# MAGIC
# MAGIC In the context of MLOps, there are more tests than simply how accurate a model will be.  To ensure the stability of our ML system and compliance with any regulatory requirements, we will subject each model added to the registry to a series of validation checks.  These include, but are not limited to:
# MAGIC <br><br>
# MAGIC * __Inference on production data__
# MAGIC * __Input schema ("signature") compatibility with current model version__
# MAGIC * __Accuracy on multiple slices of the training data__
# MAGIC * __Model documentation__
# MAGIC
# MAGIC In this notebook we explore some approaches to performing these tests, and how we can add metadata to our models with tagging if they have passed a given test or not.
# MAGIC
# MAGIC This part is typically specific to your line of business and quality requirement.
# MAGIC
# MAGIC For each test, we'll add information using tags to know what has been validated in the model. We can also add Comments if needed.

# COMMAND ----------

# MAGIC %pip install "mlflow-skinny[databricks]>=2.11"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $setup_inference_data=true

# COMMAND ----------

# DBTITLE 1,Create job parameters input widgets
def get_latest_model_version(model_name):
  model_version_infos = MlflowClient().search_model_versions("name = '%s'" % model_name)
  return max([int(model_version_info.version) for model_version_info in model_version_infos])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information

# COMMAND ----------

# Get the model in transition, its name and version
model_version = get_latest_model_version(model_name)  # {model_name} is defined in the setup script
model_stage = "Challenger" #,["Challenger", "Champion", "Baseline", "Archived"])

print(f"Validating {model_stage} model for {model_name} on model version {model_version}")

# COMMAND ----------

client = MlflowClient()
model_details = client.get_model_version(model_name, model_version)
run_info = client.get_run(run_id=model_details.run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description check
# MAGIC
# MAGIC Has the data scientist provided a description of the model being submitted?

# COMMAND ----------

# If there's no description or an insufficient number of charaters, tag accordingly
if not model_details.description:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_description", value=False)
  print("Please add model description")
elif not len(model_details.description) > 20:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_description", value=False)
  print("Please add detailed model description (40 char min).")
else:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_description", value=True)

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
  model_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@Challenger")

  # Batch score
  validation_w_preds = validation_df.withColumn("predictions", model_udf(*validation_df.columns))
  display(validation_w_preds)
  #client.set_model_version_tag(name=model_name, version=model_version, key="predicts", value=True)

except Exception as e:
  print(e)
  validation_w_preds = spark.createDataFrame([], StructType([]))
  print("Unable to predict on features.")
  #client.set_model_version_tag(name=model_name, version=model_version, key="predicts", value=False)
  pass

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

# Load validation data
validation_df = spark.read.table("mlops_churn_validation")

# Load challenger model as a Spark UDF
champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@challenger")

# Batch score
challenger_pred_df = validation_df.withColumn('predictions', champion_model(*validation_df.columns))

display(challenger_pred_df)

# COMMAND ----------

import pyspark.sql.functions as F

# We have already found the predictions for the Challenger model
# Add columns to record prediction time and model information
predictions_df = (
  challenger_pred_df.withColumn('prediction_date', F.current_timestamp())
                    .withColumn('model', F.lit(model_name))
                    .withColumn('model_version', F.lit(model_version))
                    .withColumn('model_alias', F.lit("Challenger"))
)

display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference on the Champion model
# MAGIC
# MAGIC To make a comparison of the Challenger against the Champion, we would also have obtained the Champion's predictions on the validation data. However, since we do not have a Champion model already deployed for this demo, we will skip this step.

# COMMAND ----------

# # Get model version of Champion model
# model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version
# print(f"Champion model version for {model_name}: {model_version}")

# # Load Champion model as a Spark UDF
# champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@Champion")

# # Batch score
# champion_pred_df = validation_df.withColumn('predictions', champion_model(*validation_df.columns))

# display(champion_pred_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Capture metadata for the Champion model
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
#                     .withColumn("model_version", F.lit(model_version))
#                     .withColumn("model_alias", F.lit("Champion"))
# )

# display(predictions_all_df)

# COMMAND ----------

# predictions_all_df should have predictions from both the Champion and Challenger models
# Set predictions_all_df to predictions_df for this demo, which only has the Challenger's predictions
# Do not run this cell if you collected predictions from the Champion model in champion_pred_df
predictions_all_df = predictions_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Champion and Challenger business metrics
# MAGIC
# MAGIC Now that we have the necessary predictions and information on the models used, we can use this data to perform our Champion-Challenger analysis agsint our business KPIs.

# COMMAND ----------

from pyspark.sql import Window

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

# Convert to pandas for easier transformation and plotting

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
    color="predictions",
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
# MAGIC ## Results
# MAGIC
# MAGIC Here's a summary of the testing results:

# COMMAND ----------

results = client.get_model_version(model_name, model_version)
results.tags

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting the Challenger to Champion
# MAGIC
# MAGIC When we are satisfied with the results of the __Challenger__ model, we can then promote it to Champion. This is done by setting its alias to `@Champion`. Production pipelines that load the model using the `Champion` alias will then be loading this new model. The alias on the older Champion model, should there be one, will be automatically unset. The model retains its `@Challenger` alias until a newer Challenger model is deployed with the alias.
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

model_version = client.get_model_version_by_alias(name=model_name, alias="Challenger").version

client.set_registered_model_alias(
  name=model_name,
  alias="Champion",
  version=model_version
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: REMOVE - Move to next phase in model lifecycle or archive version
# MAGIC
# MAGIC The next phase of this models' lifecycle will be to `Challenger` or `Archived`, depending on how it fared in testing.

# COMMAND ----------

# If any checks failed, reject/set 'validation_status' tag to 'FAILED' and remove an alias
if ('False' in results.tags.values()) | ('fail' in results.tags.values()):
  print("Rejecting transition...")
  validation_status = "FAILED"
  alias = "ARCHIVED"

else:
  print("Accepting transition...")
  validation_status = "PASSED"
  alias = model_stage

# COMMAND ----------

# Update validation tag
client.set_model_version_tag(
  name=model_name,
  version=model_version,
  key='validation_status',
  value=validation_status
)

# Update/Set model alias
client.set_registered_model_alias(
  name=model_name,
  alias=alias,
  version=model_version
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulation, our model is now tested and transitioned accordingly
# MAGIC
# MAGIC We now have the certainty that our model is ready to be used in inference pipelines and in realtime serving endpoints, as it matches our validation standards.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our Challenger model]($./05_batch_inference)
