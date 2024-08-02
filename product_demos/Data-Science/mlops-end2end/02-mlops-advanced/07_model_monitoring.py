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
# MAGIC
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create monitor
# MAGIC One-time setup

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

# DBTITLE 1,Create Monitor
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType


print(f"Creating monitor for {inference_table_name}")
w = WorkspaceClient()

info = w.quality_monitors.create(
  table_name=f"{catalog}.{dbName}.{inference_table_name}",
  inference_log=MonitorInferenceLog(
        problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
        prediction_col="prediction",
        timestamp_col=timestamp_col,
        granularities=["1 day"],
        model_id_col="Model_Version",
        label_col=label_col, # optional
  ),
  assets_dir=f"/Workspace/Users/{current_user}/databricks_lakehouse_monitoring/{catalog}.{dbName}.{inference_table_name}",
  output_schema_name=f"{catalog}.{dbName}",
  baseline_table_name=f"{catalog}.{dbName}.{inference_table_name}_baseline",
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
  info = w.quality_monitors.get(table_name=f"{catalog}.{dbName}.{inference_table_name}")
  time.sleep(10)

assert info.status == MonitorInfoStatus.MONITOR_STATUS_ACTIVE, "Error creating monitor"

# COMMAND ----------

# MAGIC %md Monitor creation for the first time will also trigger an initial refresh so fetch/wait or trigger a monitoring job and wait until completion

# COMMAND ----------

refreshes = w.quality_monitors.list_refreshes(table_name=f"{catalog}.{dbName}.{inference_table_name}").refreshes
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=f"{catalog}.{dbName}.{inference_table_name}", refresh_id=run_info.refresh_id)
  time.sleep(30)

assert run_info.state == MonitorRefreshInfoState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

from pprint import pprint


info = w.quality_monitors.get(table_name=f"{catalog}.{dbName}.{inference_table_name}")
pprint(info)

# COMMAND ----------

# DBTITLE 1,Trigger a refresh [OPTIONAL]
run_info = w.quality_monitors.run_refresh(table_name=f"{catalog}.{dbName}.{inference_table_name}")

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

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
