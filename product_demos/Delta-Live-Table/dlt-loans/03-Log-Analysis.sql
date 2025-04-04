-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # DLT pipeline log analysis
-- MAGIC
-- MAGIC <img style="float:right" width="500" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/dlt/dlt-loans-dashboard.png?raw=true">
-- MAGIC
-- MAGIC Each DLT Pipeline can be configured to save out the metrics to a table in Unity Catalog. From this table we can see what is happening and the quality of the data passing through it.
-- MAGIC
-- MAGIC You can leverage the expecations directly as a SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
-- MAGIC
-- MAGIC This notebook extracts and analyses expectation metrics to build such KPIS.
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&org_id=984752964297111&notebook=%2F03-Log-Analysis&demo_name=dlt-loans&event=VIEW&path=%2F_dbdemos%2Fdata-engineering%2Fdlt-loans%2F03-Log-Analysis&version=1">

-- COMMAND ----------

SELECT
  event_type,
  parse_json(details) as details
FROM
  main__build.dbdemos_dlt_loan.event_logs

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW demo_dlt_loans_system_event_log_raw 
  as SELECT * FROM event_log(TABLE(main__build.dbdemos_dlt_loan.raw_txs));
SELECT * FROM demo_dlt_loans_system_event_log_raw order by timestamp desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log in a JSON blob. Using `parse_json` and the `VARIANT` data type we can explore it as if it was an object. There are different fields depending on what type of Event it is. Some examples include:
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
FROM main__build.dbdemos_dlt_loan.event_logs
WHERE details:flow_definition IS NOT NULL
ORDER BY timestamp

-- COMMAND ----------

-- DBTITLE 1,Data Quality Results
select
  e.origin.update_id,
  ex.value:name::string,
  ex.value:dataset::string,
  ex.value:passed_records::long as passed_records,
  ex.value:failed_records::long as failed_records
from
  main__build.dbdemos_dlt_loan.event_logs e,
  lateral variant_explode(parse_json(e.details:flow_progress:data_quality:expectations:[ * ])) as ex
where
  e.event_type = "flow_progress"
  and details:flow_progress:status = "RUNNING"
  and details:flow_progress:data_quality:expectations IS NOT NULL



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your expectations are ready to be queried in SQL! Open the <a dbdemos-dashboard-id="dlt-expectations" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">data Quality Dashboard example</a> for more details.
