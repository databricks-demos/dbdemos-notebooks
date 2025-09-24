# Databricks notebook source
# MAGIC %md
# MAGIC # Drift detection
# MAGIC
# MAGIC In this step, we will define drift detection rules to run periodically on the inference data.
# MAGIC
# MAGIC **Drift detection** refers to the process of identifying changes in the statistical properties of input data, which can lead to a decline in model performance over time. This is crucial for maintaining the accuracy and reliability of models in dynamic environments, as it allows for timely interventions such as model retraining or adaptation to new data distributions
# MAGIC
# MAGIC In order to simulate some data drifts, we will use [_dbldatagen_ library](https://github.com/databrickslabs/dbldatagen), a Databricks Labs project which is a Python library for generating synthetic data using Spark.
# MAGIC
# MAGIC We will simulate label drift using the data generator package.
# MAGIC **Label drift** occurs when the distribution of the ground truth labels changes over time, which can happen due to shifts in labeling criteria or the introduction of labeling errors. _We will create both label and prediction drifts_.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-8-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F07_retrain_automl&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Batch to automatically retrain model on a monthly basis.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC DBR/MLR16.4LTS (Recommended)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Install needed package
# MAGIC %pip install --quiet databricks-sdk mlflow-skinny --upgrade dbldatagen
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Define drift metrics
dbutils.widgets.dropdown("perf_metric", "f1_score.macro", ["accuracy_score", "precision.weighted", "recall.weighted", "f1_score.macro"])
dbutils.widgets.dropdown("drift_metric", "js_distance", ["chi_squared_test.statistic", "chi_squared_test.pvalue", "tv_distance", "l_infinity_distance", "js_distance"])
dbutils.widgets.text("model_id", "*", "Model Id")

# COMMAND ----------

# MAGIC %md
# MAGIC Run setup notebook & generate synthetic data

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true $gen_synthetic_data=true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh the monitor 
# MAGIC
# MAGIC The previous step is writing the synthetic data to the inference table. We should refresh the monitor to recompute the metrics.
# MAGIC
# MAGIC **PS:** Refresh is only necessary if the monitored table has undergone changes

# COMMAND ----------

# DBTITLE 1,Refresh the monitor
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInfoStatus, MonitorRefreshInfoState


w = WorkspaceClient()
refresh_info = w.quality_monitors.run_refresh(table_name=f"{catalog}.{db}.advanced_churn_inference_table")

while refresh_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  refresh_info = w.quality_monitors.get_refresh(table_name=f"{catalog}.{db}.advanced_churn_inference_table", refresh_id=refresh_info.refresh_id)
  time.sleep(180)

# COMMAND ----------

# DBTITLE 1,Programmatically retrieve profile and drift table names from monitor info
monitor_info = w.quality_monitors.get(table_name=f"{catalog}.{db}.advanced_churn_inference_table")
drift_table_name = monitor_info.drift_metrics_table_name
profile_table_name = monitor_info.profile_metrics_table_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inspect dashboard
# MAGIC
# MAGIC Once the monitor is refreshed, refreshing the monitoring dashboard will show the latest model performance metrics. When evaluated against the latest labelled data, the model has poor accuracy, weighted F1 score and recall. On the other hand, it has a weighted precision of 1.
# MAGIC
# MAGIC We expect this because the model is now heavily weighted towards the `churn = Yes` class. All predictions of `Yes` are correct, leading to a weighted precision of 1.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/08_model_kpis.png?raw=true" width="1200">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC We will go ahead and illustrate how you can programatically retrieve the drift metrics and trigger model retraining.
# MAGIC
# MAGIC However, it is worthwhile to mention that by inspecting the confusion matrix in the monitoring dashboard, we can see that the latest labelled data only has the `Yes` label. i.e. all customers have churned. This is an unlikely scenario. That should lead us to question whether labelling was done correctly, or if there were data quality issues upstream. These causes of label drift do not necessitate model retraining.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/08_confusion_matrix.png?raw=true" width="1200">
# MAGIC
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve drift metrics
# MAGIC
# MAGIC Query Lakehouse Monitoring's drift metrics table for the inference table being monitored.
# MAGIC Here we're testing if these metrics have exceeded a certain threshold (defined by the business):
# MAGIC 1. Prediction drift (Jensen–Shannon distance) > 0.2
# MAGIC 2. Label drift (Jensen–Shannon distance) > 0.2
# MAGIC 3. Expected Loss (daily average per user) > 30
# MAGIC 4. Performance(i.e. F1-Score) < 0.4

# COMMAND ----------

metric = dbutils.widgets.get("perf_metric")
drift = dbutils.widgets.get("drift_metric")
model_id = dbutils.widgets.get("model_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Construct a dataframe to detect performance degradation from the profile metrics table generated by lakehouse monitoring

# COMMAND ----------

# DBTITLE 1,dataframe for performance metrics
performance_metrics_df = spark.sql(f"""
SELECT
  window.start as time,
  {metric} AS performance_metric,
  expected_loss,
  Model_Version AS `Model Id`
FROM {profile_table_name}
WHERE
  window.start >= "2025-06-28"
	AND log_type = "INPUT"
  AND column_name = ":table"
  AND slice_key is null
  AND slice_value is null
  AND Model_Version = '{model_id}'
ORDER BY
  window.start
"""
)
display(performance_metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Construct a dataframe to detect drifts from the drift metrics table generated by lakehouse monitoring.

# COMMAND ----------

drift_metrics_df = spark.sql(f"""
  SELECT
  window.start AS time,
  column_name,
  {drift} AS drift_metric,
  Model_Version AS `Model Id`
FROM {drift_table_name}
WHERE
  column_name IN ('prediction', 'churn')
  AND window.start >= "2025-06-30"
  AND slice_key is null
  AND slice_value is null
  AND Model_Version = '{model_id}'
  AND drift_type = "CONSECUTIVE"
ORDER BY
  window.start
"""
)
display(drift_metrics_df )

# COMMAND ----------

# DBTITLE 1,Unstack dataframe
from pyspark.sql.functions import first


# If no drift on the label or prediction, we skip it
if not drift_metrics_df.isEmpty():
    unstacked_drift_metrics_df = (
        drift_metrics_df.groupBy("time", "`Model Id`")
        .pivot("column_name")
        .agg(first("drift_metric"))
        .orderBy("time")
    )
    display(unstacked_drift_metrics_df)

# COMMAND ----------

# DBTITLE 1,Join all metrics together
all_metrics_df = performance_metrics_df
if not drift_metrics_df.isEmpty():
    all_metrics_df = performance_metrics_df.join(
        unstacked_drift_metrics_df, on=["time", "Model Id"], how="inner"
    )

display(all_metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Count total violations and save as task value
# MAGIC
# MAGIC Here we will define the different thresholds for the metrics we are interested in to qualify a drift:
# MAGIC - Performance metric < 0.5 
# MAGIC - Average Expected Loss per customer (our custom metric connected to business) > 30 dollars

# COMMAND ----------

# DBTITLE 1,count nr violations
from pyspark.sql.functions import col, abs


performance_violation_count = all_metrics_df.where(
    (col("performance_metric") < 0.5) & (abs(col("expected_loss")) > 30)
).count()

drift_violation_count = 0
if not drift_metrics_df.isEmpty():
    drift_violation_count = all_metrics_df.where(
        (col("churn") > 0.19) & (col("prediction") > 0.19)
    ).count()

all_violations_count = drift_violation_count + performance_violation_count

# COMMAND ----------

print(f"Total number of joint violations: {all_violations_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Trigger model retraining
# MAGIC
# MAGIC Upon detecting the number of violations, we should automate some actions, such as:
# MAGIC - Retrain the machine learning model
# MAGIC - Send an alert to owners via Slack or email
# MAGIC
# MAGIC One way of performing this in Databricks is to add branching logic to your job with [the If/else condition task](https://docs.databricks.com/en/jobs/conditional-tasks.html#add-branching-logic-to-your-job-with-the-ifelse-condition-task). 
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/mlops/advanced/08_view_retraining_workflow.png?raw=true" width="1200">
# MAGIC
# MAGIC In order to do that, we should save the number of violations in a [task value](https://docs.databricks.com/en/jobs/share-task-context.html) to be consumed in the If/else condition. 
# MAGIC
# MAGIC In our workflow, we will trigger model training, which will be a job run task for the train model job.

# COMMAND ----------

# DBTITLE 1,Exit notebook by setting a task value
dbutils.jobs.taskValues.set(key = 'all_violations_count', value = all_violations_count)

# COMMAND ----------


