# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **Your Lakeflow Declarative Pipeline has been installed and started for you!** Open the <a dbdemos-pipeline-id="pipeline-bike" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Bike Rental Declarative Pipeline</a> to see it in action.<br/>
# MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Streaming Tables
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-1.png?raw=true" width="400px" style="width:400px; float: right;" />
# MAGIC
# MAGIC A streaming table is a Delta table with additional support for streaming or incremental data processing. A streaming table can be targeted by one or more flows in an ETL pipeline.
# MAGIC
# MAGIC Streaming tables are a good choice for data ingestion for the following reasons:
# MAGIC * Each input row is handled only once, which models the vast majority of ingestion workloads (that is, by appending or upserting rows into a table).
# MAGIC * They can handle large volumes of append-only data.
# MAGIC
# MAGIC Streaming tables are also a good choice for low-latency streaming transformations for the following reasons:
# MAGIC * Reason over rows and windows of time
# MAGIC * Handle high volumes of data
# MAGIC * Low latency
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=00-pipeline-tutorial&demo_name=declarative-pipelines&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating our bronze streaming tables
# MAGIC Take a look at [bronze.sql]($./01-bronze.sql) to see how we create our bronze tables `maintenance_logs_raw`, `rides_raw`, `weather_raw`, and `customers_cdc_raw`.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Enriching our data with AI functions
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-2.png?raw=true" width="400px" style="width:400px; float: right;" />
# MAGIC
# MAGIC Now that we've got our raw data loaded let's enrich it in our silver layer.
# MAGIC
# MAGIC Our maintenance logs include an unstructured field `issue_description`, as is this isn't very useful for analytics. Let's use the `ai_classify` function to categorize each of these issues into categories for reporting.

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   date(reported_time) as maintenance_date,
# MAGIC   maintenance_id,
# MAGIC   bike_id,
# MAGIC   reported_time,
# MAGIC   resolved_time,
# MAGIC   issue_description,
# MAGIC   -- Classify issues as either related to brakes, chains/pedals, tires or something else
# MAGIC   ai_classify(issue_description, array("brakes", "chains_pedals", "tires", "other")) as issue_type
# MAGIC from
# MAGIC   main__build.dbdemos_pipeline_bike.maintenance_logs_raw
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating our silver Streaming Tables enriched with AI
# MAGIC Take a look at [silver.sql]($./02-silver.sql) to see how we create our silver tables `maintenance_logs`, `rides`, `weather`, `customers` (SCD Type 1), and `customers_history` (SCD Type 2 using Auto CDC).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Incrementally process aggregations with Materialized Views
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-3.png?raw=true" width="400px" style="width:400px; float: right;" />
# MAGIC
# MAGIC
# MAGIC Like standard views, materialized views are the results of a query and you access them the same way you would a table. Unlike standard views, which recompute results on every query, materialized views cache the results and refreshes them on a specified interval. Because a materialized view is precomputed, queries against it can run much faster than against regular views.
# MAGIC
# MAGIC A materialized view is a declarative pipeline object. It includes a query that defines it, a flow to update it, and the cached results for fast access. A materialized view:
# MAGIC * Tracks changes in upstream data.
# MAGIC * On trigger, incrementally processes the changed data and applies the necessary transformations.
# MAGIC * Maintains the output table, in sync with the source data, based on a specified refresh interval.
# MAGIC
# MAGIC Materialized views are a good choice for many transformations:
# MAGIC * You apply reasoning over cached results instead of rows. In fact, you simply write a query.
# MAGIC * They are always correct. All required data is processed, even if it arrives late or out of order.
# MAGIC * They are often incremental. Databricks will try to choose the appropriate strategy that minimizes the cost of updating a materialized view. 
# MAGIC
# MAGIC
# MAGIC When the pipeline defining a materialized view is triggered, the view is automatically kept up to date, often incrementally. Databricks attempts to process only the data that must be processed to keep the materialized view up to date. A materialized view always shows the correct result, even if it requires fully recomputing the query result from scratch, but often Databricks makes only incremental updates to a materialized view, which can be far less costly than a full recomputation.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating our gold Materialized Views
# MAGIC Take a look at [gold.sql]($../transformations/03-gold.sql) to see how we create our gold tables `bikes`, `stations` and `maintenance_events`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing your business metrics
# MAGIC You have everything you need! Once ready, open your <a  dbdemos-dashboard-id="bike-rental" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Bike Rental Business Dashboard</a> to track all your insights in realtime ! 
# MAGIC
# MAGIC
# MAGIC ### Next: tracking data quality
# MAGIC
# MAGIC Lakeflow Declarative Pipelines makes it easy to track your data quality and set alerts when something is wrong! Open the [02-Pipeline-event-monitoring]($../explorations/02-Pipeline-event-monitoring) notebook for more details.
