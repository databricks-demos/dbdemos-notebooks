# Databricks notebook source
# MAGIC %md
# MAGIC # Verify drift metric(s)
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-7.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F07_retrain_automl&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Batch to automatically retrain model on a monthly basis.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.28.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

dbutils.widgets.dropdown("perf_metric", "f1_score.macro", ["accuracy_score", "precision.weighted", "recall.weighted", "f1_score.macro"])
dbutils.widgets.dropdown("drift_metric", "js_distance", ["chi_squared_test.statistic", "chi_squared_test.pvalue", "tv_distance", "l_infinity_distance", "js_distance"])
dbutils.widgets.text("model_id", "*", "Model Id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## First retrieve drift metrics
# MAGIC
# MAGIC Query Lakehouse Monitoring's drift metrics table for the inference table being monitored.
# MAGIC Here we're testing if these metrics have exceeded a certain threshold (defined by the business):
# MAGIC 1. prediction drift (Jensen–Shannon distance) > 0.2
# MAGIC 2. label drift (Jensen–Shannon distance) > 0.2
# MAGIC 3. expected_loss (daily) > 100
# MAGIC 4. performance(i.e. f1_score) < 0.6

# COMMAND ----------

inference_table_name = "mlops_churn_advanced_inference"

# COMMAND ----------

# DBTITLE 1,Refresh the monitor
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInfoStatus, MonitorRefreshInfoState
import time

w = WorkspaceClient()

run_info = w.quality_monitors.run_refresh(table_name=f"{catalog}.{db}.{inference_table_name}")
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=f"{catalog}.{db}.{inference_table_name}", refresh_id=run_info.refresh_id)
  time.sleep(30)


# COMMAND ----------

monitor_info = w.quality_monitors.get(table_name=f"{catalog}.{db}.{inference_table_name}")
drift_table_name = monitor_info.drift_metrics_table_name
profile_table_name = monitor_info.profile_metrics_table_name

# COMMAND ----------

metric = dbutils.widgets.get("perf_metric")
drift = dbutils.widgets.get("drift_metric")
model_id = dbutils.widgets.get("model_id")

# COMMAND ----------

performance_metrics_df = spark.sql(f"""
SELECT
  window.start as time,
  {metric} AS performance_metric,
  expected_loss,
  Model_Version AS `Model Id`
FROM {profile_table_name}
WHERE
  window.start >= "2024-06-01"
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

drift_metrics_df = spark.sql(f"""
  SELECT
  window.start AS time,
  column_name,
  {drift} AS drift_metric,
  Model_Version AS `Model Id`
FROM {drift_table_name}
WHERE
  column_name IN ('prediction', 'churn')
  AND window.start >= "2024-06-01"
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

# DBTITLE 1,Unstack
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
# MAGIC Count total violations and save as task value

# COMMAND ----------

from pyspark.sql.functions import col, abs

performance_violation_count = all_metrics_df.where(
    (col("performance_metric") < 0.5) & (abs(col("expected_loss")) > 40)
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

# DBTITLE 1,Exit notebook by setting a task value
dbutils.jobs.taskValues.set(key = 'all_violations_count', value = all_violations_count)
