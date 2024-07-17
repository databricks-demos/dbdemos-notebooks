-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # DLT pipeline log analysis
-- MAGIC
-- MAGIC <img style="float:right" width="500" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-dlt-stat.png?raw=true">
-- MAGIC
-- MAGIC
-- MAGIC Each DLT Pipeline saves events and expectations metrics in the Storage Location defined on the pipeline. From this table we can see what is happening and the quality of the data passing through it.
-- MAGIC
-- MAGIC You can leverage the expecations directly as a SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
-- MAGIC
-- MAGIC This notebook extracts and analyses expectation metrics to build such KPIS.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.4-DLT-churn-expectation-dashboard-data-prep&demo_name=lakehouse-retail-c360&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Accessing the Delta Live Table pipeline events with Unity Catalog
-- MAGIC
-- MAGIC Databricks provides an `event_log` function which is automatically going to lookup the event log table. You can specify any table to get access to the logs:
-- MAGIC
-- MAGIC `SELECT * FROM event_log(TABLE(catalog.schema.my_table))`
-- MAGIC
-- MAGIC #### Using Legacy hive_metastore
-- MAGIC *Note: If you are not using Unity Catalog (legacy hive_metastore), you can find your event log location opening the Settings of your DLT pipeline, under `storage` :*
-- MAGIC
-- MAGIC ```
-- MAGIC {
-- MAGIC     ...
-- MAGIC     "name": "lakehouse_churn_dlt",
-- MAGIC     "storage": "/demos/dlt/loans",
-- MAGIC     "target": "quentin_lakehouse_churn_dlt"
-- MAGIC }
-- MAGIC ```
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Accessing the Event log table from Unity Catalog.
SELECT * FROM event_log(TABLE(main__build.dbdemos_retail_c360.churn_features)) 

-- COMMAND ----------

-- DBTITLE 1,Adding our DLT system table to the metastore
CREATE OR REPLACE TEMPORARY VIEW demo_dlt_loans_system_event_log_raw 
  as SELECT * FROM event_log(TABLE(main__build.dbdemos_retail_c360.churn_features));
SELECT * FROM demo_dlt_loans_system_event_log_raw order by timestamp desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Analyzing dlt_system_event_log_raw table structure
-- MAGIC
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
-- MAGIC * `user_action` Events occur when taking actions like creating the pipeline
-- MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
-- MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
-- MAGIC   * `flow_type` - whether this is a complete or append flow
-- MAGIC   * `explain_text` - the Spark explain plan
-- MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
-- MAGIC   * `metrics` - currently contains `num_output_rows`
-- MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
-- MAGIC     * `dropped_records`
-- MAGIC     * `expectations`
-- MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
-- MAGIC   

-- COMMAND ----------

-- DBTITLE 1,Lineage Information
SELECT
  details:flow_definition.output_dataset,
  details:flow_definition.input_datasets,
  details:flow_definition.flow_type,
  details:flow_definition.schema,
  details:flow_definition
FROM demo_dlt_loans_system_event_log_raw
WHERE details:flow_definition IS NOT NULL
ORDER BY timestamp

-- COMMAND ----------

-- DBTITLE 1,Data Quality Results
SELECT
  id,
  expectations.dataset,
  expectations.name,
  expectations.failed_records,
  expectations.passed_records
FROM(
  SELECT 
    id,
    timestamp,
    details:flow_progress.metrics,
    details:flow_progress.data_quality.dropped_records,
    explode(from_json(details:flow_progress:data_quality:expectations
             ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
  FROM demo_dlt_loans_system_event_log_raw
  WHERE details:flow_progress.metrics IS NOT NULL) data_quality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## That's it! Our data quality metrics are ready! 
-- MAGIC
-- MAGIC Our datable is now ready be queried using DBSQL. Open the <a dbdemos-dashboard-id="dlt-quality-stat" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Data Quality Dashboard</a>
