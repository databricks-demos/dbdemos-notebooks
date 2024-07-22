# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Delta Live Tables - Monitoring  
# MAGIC   
# MAGIC
# MAGIC <img style="float:right" width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC
# MAGIC Each DLT Pipeline saves events and expectations metrics in the Storage Location defined on the pipeline. From this table we can see what is happening and the quality of the data passing through it.
# MAGIC
# MAGIC You can leverage the expecations directly as a SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
# MAGIC
# MAGIC This notebook extracts and analyses expectation metrics to build such KPIS.
# MAGIC
# MAGIC ## Accessing the Delta Live Table pipeline events with Unity Catalog
# MAGIC
# MAGIC Databricks provides an `event_log` function which is automatically going to lookup the event log table. You can specify any table to get access to the logs:
# MAGIC
# MAGIC `SELECT * FROM event_log(TABLE(catalog.schema.my_table))`
# MAGIC
# MAGIC #### Using Legacy hive_metastore
# MAGIC *Note: If you are not using Unity Catalog (legacy hive_metastore), you can find your event log location opening the Settings of your DLT pipeline, under `storage` :*
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     ...
# MAGIC     "name": "lakehouse_churn_dlt",
# MAGIC     "storage": "/demos/dlt/loans",
# MAGIC     "target": "your schema"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=03-Retail_DLT_CDC_Monitoring&demo_name=dlt-cdc&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Load DLT system table 
# MAGIC %sql
# MAGIC SELECT * FROM event_log(TABLE(main__build.dbdemos_dlt_cdc.customers)) 

# COMMAND ----------

# MAGIC %md ## System table setup
# MAGIC We'll create a table based on the events log being saved by DLT. The system tables are stored under the storage path defined in your DLT settings (the one defined in the widget):

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW demo_dlt_loans_system_event_log_raw 
# MAGIC   as SELECT * FROM event_log(TABLE(main__build.dbdemos_dlt_cdc.customers));
# MAGIC SELECT * FROM demo_dlt_loans_system_event_log_raw order by timestamp desc;

# COMMAND ----------

# MAGIC %md #Delta Live Table expectation analysis
# MAGIC Delta live table tracks our data quality through expectations. These expectations are stored as technical tables without the DLT log events. We can create a view to simply analyze this information
# MAGIC
# MAGIC **Make sure you set your DLT storage path in the widget!**
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_quality_expectations&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Notebook extracting DLT expectations as delta tables used to build DBSQL data quality Dashboard.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["DLT Data Quality Stats"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing dlt_system_event_log_raw table structure
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
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
# MAGIC We can leverage this information to track our table quality using SQL

# COMMAND ----------

# DBTITLE 0,Event Log - Raw Sequence of Events by Timestamp
# MAGIC %sql
# MAGIC SELECT 
# MAGIC        id,
# MAGIC        timestamp,
# MAGIC        sequence,
# MAGIC        event_type,
# MAGIC        message,
# MAGIC        level, 
# MAGIC        details
# MAGIC   FROM demo_cdc_dlt_system_event_log_raw
# MAGIC  ORDER BY timestamp ASC;  

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view cdc_dlt_expectations as (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows as output_records,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     details:flow_progress.status as status_update,
# MAGIC     explode(from_json(details:flow_progress.data_quality.expectations
# MAGIC              ,'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) expectations
# MAGIC   FROM demo_cdc_dlt_system_event_log_raw
# MAGIC   where details:flow_progress.data_quality.expectations is not null
# MAGIC   ORDER BY timestamp);
# MAGIC select * from cdc_dlt_expectations

# COMMAND ----------

# MAGIC %md ## 3 - Visualizing the Quality Metrics
# MAGIC
# MAGIC Let's run a few queries to show the metrics we can display. Ideally, we should be using Databricks SQL to create SQL Dashboard and track all the data, but for this example we'll run a quick query in the dashboard directly:

# COMMAND ----------

# MAGIC %sql 
# MAGIC select sum(expectations.failed_records) as failed_records, sum(expectations.passed_records) as passed_records, expectations.name from cdc_dlt_expectations group by expectations.name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plotting failed record per expectations

# COMMAND ----------

import plotly.express as px
expectations_metrics = spark.sql("select sum(expectations.failed_records) as failed_records, sum(expectations.passed_records) as passed_records, expectations.name from cdc_dlt_expectations group by expectations.name").toPandas()
px.bar(expectations_metrics, x="name", y=["passed_records", "failed_records"], title="DLT expectations metrics")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### What's next?
# MAGIC
# MAGIC We now have our data ready to be used for more advanced.
# MAGIC
# MAGIC We can start creating our first <a dbdemos-dashboard-id="dlt-expectations" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1'  target="_blank">DBSQL Dashboard</a> monitoring our data quality & DLT pipeline health.
