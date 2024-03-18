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

# DBTITLE 1,Install Lakehouse Monitoring wheel
# MAGIC %pip install "https://ml-team-public-read.s3.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_lakehouse_monitoring-0.4.6-py3-none-any.whl"
# MAGIC
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="dbdemos"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create monitor
# MAGIC One-time setup

# COMMAND ----------

import databricks.lakehouse_monitoring as lm

# COMMAND ----------

# DBTITLE 1,Define expected loss metric
from pyspark.sql.types import DoubleType


expected_loss_metric = [
  lm.Metric(
    type="aggregate",
    name="expected_loss",
    input_columns=[":table"],
    definition="""avg(CASE
    WHEN {{prediction_col}} != {{label_col}} AND {{label_col}} = 'Yes' THEN -monthly_charges
    ELSE 0 END
    )""",
    output_data_type= DoubleType()
  )
]

# COMMAND ----------

# DBTITLE 1,Create Monitor
print(f"Creating monitor for {inference_table_name}")

info = lm.create_monitor(
  table_name=f"{catalog}.{dbName}.{inference_table_name}",
  profile_type=lm.InferenceLog(
    timestamp_col=timestamp_col,
    granularities=["1 day"], # Daily granularity
    model_id_col="Model_Version",
    prediction_col="prediction",
    problem_type="classification",
    label_col=label_col,
    schedule=["0 0 0/12 * * ?"], # 12hours CRON synthax
  ),
  baseline_table_name=baseline_table_name,
  slicing_exprs=["senior_citizen='Yes'"], # Slicing dimension
  output_schema_name=f"{catalog}.{dbName}",
  custom_metrics=expected_loss_metric
)

# COMMAND ----------

# DBTITLE 1,Update monitor
# info = lm.update_monitor(
#   table_name=f"{catalog}.{dbName}.{inference_table_name}",
#   updated_params={"custom_metrics" : expected_loss_metric}
# )

# COMMAND ----------

# MAGIC %md Wait/Verify that monitor was created

# COMMAND ----------

import time


# Wait for monitor to be created
while info.status == lm.MonitorStatus.PENDING:
  info = lm.get_monitor(table_name=f"{catalog}.{dbName}.{inference_table_name}")
  time.sleep(10)

assert info.status == lm.MonitorStatus.ACTIVE, "Error creating monitor"

# COMMAND ----------

# MAGIC %md Monitor creation for the first time will also trigger an initial refresh so fetch/wait or trigger a monitoring job and wait until completion

# COMMAND ----------

refresh_info = lm.list_refreshes(table_name=f"{catalog}.{dbName}.{inference_table_name}") # List all refreshes
run_info = refresh_info[-1] # Get latest refresh status
# run_info = lm.run_refresh(table_name=f"{catalog}.{dbName}.{inference_table_name}") # OR Trigger a new refresh

# Wait until monitoring job ends [OPTIONAL]
while run_info.state in (lm.RefreshState.PENDING, lm.RefreshState.RUNNING):
  run_info = lm.get_refresh(table_name=f"{catalog}.{dbName}.{inference_table_name}", refresh_id=run_info.refresh_id)
  time.sleep(30)

assert(run_info.state == lm.RefreshState.SUCCESS)

# COMMAND ----------

lm.get_monitor(table_name=f"{catalog}.{dbName}.{inference_table_name}")

# COMMAND ----------

# DBTITLE 1,Delete existing monitor [OPTIONAL]
# lm.delete_monitor(table_name=f"{catalog}.{dbName}.{inference_table_name}", purge_artifacts=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Automate model retrain [OPTIONAL]
# MAGIC
# MAGIC Automate model retraining using the automl python API and using drift metrics information
# MAGIC
# MAGIC Next steps:
# MAGIC * [Automate model re-training]($./08_retrain_churn_automl)
