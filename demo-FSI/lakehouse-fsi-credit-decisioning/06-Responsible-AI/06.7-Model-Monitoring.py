# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse monitoring
# MAGIC
# MAGIC **User requirements**
# MAGIC - You must have access to run commands on a cluster with access to Unity Catalog.
# MAGIC - You must have `USE CATALOG` privilege on at least one catalog, and you must have `USE SCHEMA` privileges on at least one schema. This notebook creates tables in the `main.default` schema. If you do not have the required privileges on the `main.default` schema, you must edit the notebook to change the default catalog and schema to ones that you do have privileges on.
# MAGIC
# MAGIC **System requirements:**
# MAGIC - Unity-Catalog enabled workspace.
# MAGIC - Databricks Runtime 12.2 LTS ML or above.
# MAGIC - Single-User/Assigned cluster (Preferred if you want to run the notebook as is to train the model and create the tables)
# MAGIC
# MAGIC This notebook illustrates how to train and deploy a classification model and monitor its corresponding batch inference table.

# COMMAND ----------

# DBTITLE 1,Install Lakehouse Monitoring client wheel
# MAGIC %pip install "databricks-sdk>=0.28.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

TABLE_NAME = f"{catalog}.{db}.credit_decisioning_inferencelogs"
BASELINE_TABLE = f"{catalog}.{db}.credit_decisioning_baseline_predictions"
MODEL_NAME = f"{model_name}" # Name of (registered) model in mlflow registry
TIMESTAMP_COL = "timestamp"
MODEL_ID_COL = "model_id" # Name of column to use as model identifier (here we'll use the model_name+version)
PREDICTION_COL = "prediction"  # What to name predictions in the generated tables
LABEL_COL = "defaulted" # Name of ground-truth labels column
ID_COL = "cust_id"
new_model_version = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Background
# MAGIC The following are required to create an inference log monitor:
# MAGIC - A Delta table in Unity Catalog that you own.
# MAGIC - The data can be batch scored data or inference logs. The following columns are required:  
# MAGIC   - `timestamp` (TimeStamp): Used for windowing and aggregation when calculating metrics
# MAGIC   - `model_id` (String): Model version/id used for each prediction.
# MAGIC   - `prediction` (String): Value predicted by the model.
# MAGIC   
# MAGIC - The following column is optional:  
# MAGIC   - `label` (String): Ground truth label.
# MAGIC
# MAGIC You can also provide an optional baseline table to track performance changes in the model and drifts in the statistical characteristics of features. 
# MAGIC - To track performance changes in the model, consider using the test or validation set.
# MAGIC - To track drifts in feature distributions, consider using the training set or the associated feature tables. 
# MAGIC - The baseline table must use the same column names as the monitored table, and must also have a `model_version` column.
# MAGIC
# MAGIC Databricks recommends enabling Delta's Change-Data-Feed ([AWS](https://docs.databricks.com/delta/delta-change-data-feed.html#enable-change-data-feed)|[Azure](https://learn.microsoft.com/azure/databricks/delta/delta-change-data-feed#enable-change-data-feed)) table property for better metric computation performance for all monitored tables, including the baseline table. This notebook shows how to enable Change Data Feed when you create the Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a sample inference table
# MAGIC
# MAGIC Example pre-processing step
# MAGIC * Extract ground-truth labels (in practice, labels might arrive later)
# MAGIC * Split into two batches
# MAGIC * Add `model_version` column and write to the table that we will attach a monitor to
# MAGIC * Add ground-truth `label_col` column with empty/NaN values
# MAGIC
# MAGIC Set `mergeSchema` to `True` to enable appending dataframes without label column available

# COMMAND ----------

from datetime import timedelta, datetime
import random
import mlflow

mlflow.set_registry_uri('databricks-uc')

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@production", result_type='double', env_manager="virtualenv")
features = loaded_model.metadata.get_input_schema().input_names()

# Simulate inferences for n days
n_days = 10

feature_df = spark.table("credit_decisioning_features").orderBy(F.rand()).limit(10)
feature_df = feature_df.withColumn(TIMESTAMP_COL, F.lit(datetime.now().timestamp()).cast("timestamp"))

for n in range(1, n_days):
  temp_df = spark.table("credit_decisioning_features").orderBy(F.rand()).limit(random.randint(5, 20))
  timestamp = (datetime.now() - timedelta(days = n)).timestamp()
  temp_df = temp_df.withColumn(TIMESTAMP_COL, F.lit(timestamp).cast("timestamp"))
  feature_df = feature_df.union(temp_df)

feature_df = feature_df.fillna(0)

# Introducing synthetic drift into few columns
feature_df = feature_df.withColumn('total_deposits_amount', F.col('total_deposits_amount') + F.rand() * 100000) \
                       .withColumn('total_equity_amount', F.col('total_equity_amount') + F.rand() * 100000) \
                       .withColumn('total_UT', F.col('total_UT') + F.rand() * 100000) \
                       .withColumn('customer_revenue', F.col('customer_revenue') + F.rand() * 100000)

pred_df =  feature_df.withColumn(PREDICTION_COL, loaded_model(*features).cast("integer")) \
                     .withColumn(MODEL_ID_COL, F.lit(new_model_version))

(pred_df
  .withColumn(MODEL_ID_COL, F.lit(new_model_version))
  .withColumn(LABEL_COL, F.lit(None).cast("integer"))
  .withColumn("cust_id", col("cust_id").cast("bigint"))
  .write.format("delta").mode("overwrite") 
  .option("mergeSchema",True) 
  .option("delta.enableChangeDataFeed", "true") 
  .saveAsTable(TABLE_NAME)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join ground-truth labels to inference table
# MAGIC **Note: If ground-truth value can change for a given id through time, then consider also joining/merging on timestamp column**

# COMMAND ----------

# DBTITLE 1,Using MERGE INTO (Recommended)
# Step 1: Create temporary view using synthetic labels
df = spark.table(TABLE_NAME).select(ID_COL, PREDICTION_COL)
df = df.withColumn("temp", F.rand())
df = df.withColumn(LABEL_COL, 
                   F.when(df["temp"] < 0.14, 1 - df[PREDICTION_COL]).otherwise(df[PREDICTION_COL]))
df = df.drop("temp", PREDICTION_COL)
ground_truth_df = df.withColumnRenamed(PREDICTION_COL, LABEL_COL)
late_labels_view_name = f"credit_decisioning_late_labels"
ground_truth_df.createOrReplaceTempView(late_labels_view_name)

# Step 2: Merge into inference table
merge_info = spark.sql(
  f"""
  MERGE INTO {TABLE_NAME} AS i
  USING {late_labels_view_name} AS l
  ON i.{ID_COL} == l.{ID_COL}
  WHEN MATCHED THEN UPDATE SET i.{LABEL_COL} == l.{LABEL_COL}
  """
)
display(merge_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a custom metric
# MAGIC
# MAGIC Customer metrics can be defined and will automatically be calculated by lakehouse monitoring. They often serve as a mean to capture some aspect of business logic or use a custom model quality score. See the documentation for more details about how to create custom metrics ([AWS](https://docs.databricks.com/lakehouse-monitoring/custom-metrics.html)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/custom-metrics)).
# MAGIC
# MAGIC In this example, we will calculate the business impact (the overdraft balance amount) of a bad model performance.

# COMMAND ----------

from pyspark.sql.types import DoubleType, StructField
from databricks.sdk.service.catalog import MonitorMetric, MonitorMetricType

CUSTOM_METRICS = [
  MonitorMetric(
    type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
    name="avg_overdraft_balance_amt",
    input_columns=[":table"],
    definition="""avg(CASE
    WHEN {{prediction_col}} != {{label_col}} AND {{label_col}} = 1 THEN overdraft_balance_amount
    ELSE 0 END
    )""",
    output_data_type= StructField("output", DoubleType()).json()
  )
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the monitor
# MAGIC Use `InferenceLog` type analysis.
# MAGIC
# MAGIC **Make sure to drop any column that you don't want to track or which doesn't make sense from a business or use-case perspective**, otherwise create a VIEW with only columns of interest and monitor it.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType, MonitorInfoStatus, MonitorRefreshInfoState, MonitorMetric

w = WorkspaceClient()

# COMMAND ----------

# Delete any axisting monitor

try:
  w.quality_monitors.delete(table_name=TABLE_NAME)
except:
  print("Monitor doesn't exist.")

# COMMAND ----------

import os

# ML problem type, either "classification" or "regression"
PROBLEM_TYPE = MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION

# Window sizes to analyze data over
GRANULARITIES = ["1 day"]   

# Directory to store generated dashboard
ASSETS_DIR = f"{os.getcwd()}/monitoring"

# Optional parameters
SLICING_EXPRS = ["age<25", "age>60"]   # Expressions to slice data with

# COMMAND ----------

# DBTITLE 1,Create Monitor
print(f"Creating monitor for {TABLE_NAME}")

info = w.quality_monitors.create(
  table_name=TABLE_NAME,
  inference_log=MonitorInferenceLog(
    timestamp_col=TIMESTAMP_COL,
    granularities=GRANULARITIES,
    model_id_col=MODEL_ID_COL, # Model version number 
    prediction_col=PREDICTION_COL,
    problem_type=PROBLEM_TYPE,
    label_col=LABEL_COL # Optional
  ),
  baseline_table_name=BASELINE_TABLE,
  slicing_exprs=SLICING_EXPRS,
  output_schema_name=f"{catalog}.{db}",
  custom_metrics=CUSTOM_METRICS,
  assets_dir=ASSETS_DIR
)

# COMMAND ----------

import time

# Wait for monitor to be created
while info.status ==  MonitorInfoStatus.MONITOR_STATUS_PENDING:
  info = w.quality_monitors.get(table_name=TABLE_NAME)
  time.sleep(10)

assert info.status == MonitorInfoStatus.MONITOR_STATUS_ACTIVE, "Error creating monitor"

# COMMAND ----------

# A metric refresh will automatically be triggered on creation
refreshes = w.quality_monitors.list_refreshes(table_name=TABLE_NAME).refreshes
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=TABLE_NAME, refresh_id=run_info.refresh_id)
  time.sleep(30)

assert run_info.state == MonitorRefreshInfoState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

# MAGIC %md
# MAGIC To view the dashboard, click **Dashboards** in the left nav bar.  
# MAGIC
# MAGIC You can also navigate to the dashboard from the primary table in the Catalog Explorer UI. On the **Quality** tab, click the **View dashboard** button.
# MAGIC
# MAGIC For details, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-dashboard.html) | [Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-dashboard)).
# MAGIC

# COMMAND ----------

w.quality_monitors.get(table_name=TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect the metrics tables
# MAGIC
# MAGIC By default, the metrics tables are saved in the default database.  
# MAGIC
# MAGIC The `create_monitor` call created two new tables: the profile metrics table and the drift metrics table. 
# MAGIC
# MAGIC These two tables record the outputs of analysis jobs. The tables use the same name as the primary table to be monitored, with the suffixes `_profile_metrics` and `_drift_metrics`.

# COMMAND ----------

# MAGIC %md ### Orientation to the profile metrics table
# MAGIC
# MAGIC The profile metrics table has the suffix `_profile_metrics`. For a list of statistics that are shown in the table, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-output.html#profile-metrics-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-output#profile-metrics-table)).
# MAGIC
# MAGIC - For every column in the primary table, the profile table shows summary statistics for the baseline table and for the primary table. The column `log_type` shows `INPUT` to indicate statistics for the primary table, and `BASELINE` to indicate statistics for the baseline table. The column from the primary table is identified in the column `column_name`.
# MAGIC - For `TimeSeries` type analysis, the `granularity` column shows the granularity corresponding to the row. For baseline table statistics, the `granularity` column shows `null`.
# MAGIC - The table shows statistics for each value of each slice key in each time window, and for the table as whole. Statistics for the table as a whole are indicated by `slice_key` = `slice_value` = `null`.
# MAGIC - In the primary table, the `window` column shows the time window corresponding to that row. For baseline table statistics, the `window` column shows `null`.  
# MAGIC - Some statistics are calculated based on the table as a whole, not on a single column. In the column `column_name`, these statistics are identified by `:table`.

# COMMAND ----------

# Display profile metrics table
profile_table = f"{TABLE_NAME}_profile_metrics"
profile_df = spark.sql(f"SELECT * FROM {profile_table}")
display(profile_df)

# COMMAND ----------

# MAGIC %md ### Orientation to the drift metrics table
# MAGIC
# MAGIC The drift metrics table has the suffix `_drift_metrics`. For a list of statistics that are shown in the table, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-output.html#drift-metrics-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-output#drift-metrics-table)).
# MAGIC
# MAGIC - For every column in the primary table, the drift table shows a set of metrics that compare the current values in the table to the values at the time of the previous analysis run and to the baseline table. The column `drift_type` shows `BASELINE` to indicate drift relative to the baseline table, and `CONSECUTIVE` to indicate drift relative to a previous time window. As in the profile table, the column from the primary table is identified in the column `column_name`.
# MAGIC   - At this point, because this is the first run of this monitor, there is no previous window to compare to. So there are no rows where `drift_type` is `CONSECUTIVE`. 
# MAGIC - For `TimeSeries` type analysis, the `granularity` column shows the granularity corresponding to that row.
# MAGIC - The table shows statistics for each value of each slice key in each time window, and for the table as whole. Statistics for the table as a whole are indicated by `slice_key` = `slice_value` = `null`.
# MAGIC - The `window` column shows the the time window corresponding to that row. The `window_cmp` column shows the comparison window. If the comparison is to the baseline table, `window_cmp` is `null`.  
# MAGIC - Some statistics are calculated based on the table as a whole, not on a single column. In the column `column_name`, these statistics are identified by `:table`.

# COMMAND ----------

# Display the drift metrics table
drift_table = f"{TABLE_NAME}_drift_metrics"
display(spark.sql(f"SELECT * FROM {drift_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look at fairness and bias metrics
# MAGIC Fairness and bias metrics are calculated for boolean type slices that were defined. The group defined by `slice_value=true` is considered the protected group ([AWS](https://docs.databricks.com/en/lakehouse-monitoring/fairness-bias.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-monitoring/fairness-bias)).

# COMMAND ----------

fb_cols = ["window", "model_id", "slice_key", "slice_value", "predictive_parity", "predictive_equality", "equal_opportunity", "statistical_parity"]
fb_metrics_df = profile_df.select(fb_cols).filter(f"column_name = ':table' AND slice_value = 'true'")
display(fb_metrics_df)

# COMMAND ----------


