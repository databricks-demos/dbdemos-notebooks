# Databricks notebook source
# MAGIC %md
# MAGIC # Explore data quality metrics from the pipeline event log
# MAGIC
# MAGIC Each pipeline can be configured to save out the metrics to a table in Unity Catalog. From this table we can see what is happening and the quality of the data passing through it.
# MAGIC You can leverage the expecations directly as a SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
# MAGIC This notebook extracts and analyses expectation metrics to build such KPIS.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=02-Pipeline-event-monitoring&demo_name=declarative-pipelines&event=VIEW">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your event log table is now available as a Table within your schema!
# MAGIC This is simply set as an option in your pipeline configuration menu.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   main__build.dbdemos_pipeline_bike.event_logs
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC The `details` column contains metadata about each Event sent to the Event Log in a JSON blob. Using `parse_json` and the `VARIANT` data type we can explore it as if it was an object. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC * `user_action` Events occur when taking actions like creating the pipeline
# MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
# MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
# MAGIC   * `flow_type` - whether this is a complete or append flow
# MAGIC   * `explain_text` - the Spark explain plan
# MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
# MAGIC   * `metrics` - currently contains `num_output_rows`
# MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
# MAGIC     * `dropped_records`
# MAGIC     * `expectations`
# MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   details:flow_definition.output_dataset,
# MAGIC   details:flow_definition.input_datasets,
# MAGIC   details:flow_definition.flow_type,
# MAGIC   details:flow_definition.schema,
# MAGIC   details:flow_definition
# MAGIC FROM main__build.dbdemos_pipeline_bike.event_logs
# MAGIC WHERE details:flow_definition IS NOT NULL
# MAGIC ORDER BY timestamp
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   e.origin.update_id,
# MAGIC   ex.value:name::string,
# MAGIC   ex.value:dataset::string,
# MAGIC   ex.value:passed_records::long as passed_records,
# MAGIC   ex.value:failed_records::long as failed_records
# MAGIC from
# MAGIC   main__build.dbdemos_pipeline_bike.event_logs e,
# MAGIC   lateral variant_explode(parse_json(e.details:flow_progress:data_quality:expectations:[ * ])) as ex
# MAGIC where
# MAGIC   e.event_type = "flow_progress"
# MAGIC   and details:flow_progress:status = "RUNNING"
# MAGIC   and details:flow_progress:data_quality:expectations IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tracking data quality as an AI/BI dashboard
# MAGIC
# MAGIC Let's leverage Databricks AI/BI dashboard to monitor our pipeline and data ingestion. 
# MAGIC
# MAGIC - Open the <a  dbdemos-dashboard-id="data-quality" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Bike Rental Data Monitoring Dashboard</a> to track all your data quality, and add alerts based on your requirements.
# MAGIC
# MAGIC - Open the <a  dbdemos-dashboard-id="operational" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Bike Rental Operational Pipeline Dashboard</a> to track all your pipeline event and cost!
