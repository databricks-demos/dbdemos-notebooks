# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor Model using Lakehouse Monitoring
# MAGIC This feature([AWS](https://docs.databricks.com/en/lakehouse-monitoring/index.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-monitoring/)) is in **Public Preview**.
# MAGIC
# MAGIC Given the inference tables we can monitor stats and drifts on table containing:
# MAGIC * batch scoring inferences
# MAGIC * request logs from Model Serving endpoint (Public Preview [AWS](https://docs.databricks.com/en/machine-learning/model-serving/inference-tables.html) |[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/inference-tables))
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-0.png" width="1200">

# COMMAND ----------

# DBTITLE 1,Install latest databricks-sdk package (>=0.28.0)
# MAGIC %pip install "databricks-sdk>=0.28.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

### TODO: fix setup script to correctly define the variables
offline_inference_table_name = "mlops_churn_advanced_offline_inference"
baseline_table_name = "mlops_churn_advanced_baseline"
timestamp_col = "inference_timestamp"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create monitor
# MAGIC One-time setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Inference Table
# MAGIC
# MAGIC This can serve as a union for offline & online processed inference

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TABLE {catalog}.{db}.{inference_table_name} AS
          SELECT * EXCEPT (split) FROM {catalog}.{db}.{offline_inference_table_name} LEFT JOIN {catalog}.{db}.{advanced_label_table_name} USING(customer_id, transaction_ts)"""
)
spark.sql(f"ALTER TABLE {catalog}.{db}.{inference_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create baseline table
# MAGIC
# MAGIC For simplification purposes, we will create the baseline table from the pre-existing `mlops_churn_advanced_offline_inference` table

# COMMAND ----------

### TODO: understand why we need model version in the baseline table
spark.sql( f"""
          CREATE OR REPLACE TABLE {catalog}.{db}.{baseline_table_name} AS
          SELECT * EXCEPT (customer_id, transaction_ts, model_alias, inference_timestamp) FROM {catalog}.{db}.{inference_table_name}"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a custom metric

# COMMAND ----------

# DBTITLE 1,Define expected loss metric
from pyspark.sql.types import DoubleType, StructField
from databricks.sdk.service.catalog import MonitorMetric, MonitorMetricType


expected_loss_metric = [
  MonitorMetric(
    type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
    name="expected_loss",
    input_columns=[":table"],
    definition="""avg(CASE
    WHEN {{prediction_col}} != {{label_col}} AND {{label_col}} = 'Yes' THEN -monthly_charges
    ELSE 0 END
    )""",
    output_data_type= StructField("output", DoubleType()).json()
  )
]

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Monitor
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType


print(f"Creating monitor for {inference_table_name}")
w = WorkspaceClient()

info = w.quality_monitors.create(
  table_name=f"{catalog}.{db}.{inference_table_name}",
  inference_log=MonitorInferenceLog(
        problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
        prediction_col="prediction",
        timestamp_col=timestamp_col,
        granularities=["1 day"],
        model_id_col="model_version",
        label_col="churn", # optional
  ),
  assets_dir=f"/Workspace/Users/{current_user}/databricks_lakehouse_monitoring/{catalog}.{db}.{inference_table_name}",
  output_schema_name=f"{catalog}.{db}",
  baseline_table_name=f"{catalog}.{db}.{baseline_table_name}",
  slicing_exprs=["senior_citizen='Yes'", "contract"], # Slicing dimension
  custom_metrics=expected_loss_metric
)

# COMMAND ----------

# MAGIC %md Wait/Verify that monitor was created

# COMMAND ----------

import time
from databricks.sdk.service.catalog import MonitorInfoStatus, MonitorRefreshInfoState


# Wait for monitor to be created
while info.status == MonitorInfoStatus.MONITOR_STATUS_PENDING:
  info = w.quality_monitors.get(table_name=f"{catalog}.{db}.{inference_table_name}")
  time.sleep(10)

assert info.status == MonitorInfoStatus.MONITOR_STATUS_ACTIVE, "Error creating monitor"

# COMMAND ----------

# MAGIC %md Monitor creation for the first time will also trigger an initial refresh so fetch/wait or trigger a monitoring job and wait until completion

# COMMAND ----------

refreshes = w.quality_monitors.list_refreshes(table_name=f"{catalog}.{db}.{inference_table_name}").refreshes
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=f"{catalog}.{db}.{inference_table_name}", refresh_id=run_info.refresh_id)
  time.sleep(30)

assert run_info.state == MonitorRefreshInfoState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

w.quality_monitors.get(table_name=f"{catalog}.{db}.{inference_table_name}")

# COMMAND ----------

# DBTITLE 1,Delete existing monitor [OPTIONAL]
# w.quality_monitors.delete(table_name=f"{catalog}.{dbName}.{inference_table_name}", purge_artifacts=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Automate model retrain [OPTIONAL]
# MAGIC
# MAGIC Automate model retraining using the automl python API and using drift metrics information
# MAGIC
# MAGIC Next steps:
# MAGIC * [Automate model re-training]($./08_retrain_churn_automl)
