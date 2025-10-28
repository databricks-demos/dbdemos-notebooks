-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Implement CDC with Spark Declarative Pipelines
-- MAGIC
-- MAGIC ## Importance of Change Data Capture (CDC)
-- MAGIC
-- MAGIC Change Data Capture (CDC) is the process that captures the changes in records made to transactional Database (Mysql, Postgre) or Data Warehouse. CDC captures operations like data deletion, append and updating, typically as a stream to re-materialize the table in external systems.
-- MAGIC
-- MAGIC CDC enables incremental loading while eliminating the need for bulk load updating.
-- MAGIC
-- MAGIC By capturing CDC events, we can re-materialize the source table as Delta Table in our Lakehouse and start running Analysis on top of it (Data Science, BI), merging the data with external system.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=01-Retail_DLT_CDC_SQL&demo_name=dlt-cdc&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Capturing CDC  
-- MAGIC
-- MAGIC A variety of **CDC tools** are available. One of the open source leader solution is Debezium, but other implementation exists simplifying the datasource, such as Fivetran, Qlik Replicate, Streamset, Talend, Oracle GoldenGate, AWS DMS.
-- MAGIC
-- MAGIC In this demo we are using CDC data coming from an external system like Debezium or DMS. 
-- MAGIC
-- MAGIC Debezium takes care of capturing every changed row. It typically sends the history of data changes to Kafka logs or save them as file. To simplify the demo, we'll consider that our external CDC system is up and running and saving the CDC as JSON file in our blob storage (S3, ADLS, GCS). 
-- MAGIC
-- MAGIC Our job is to CDC informations from the `customer` table (json format), making sure they're correct, and then materializing the customer table in our Lakehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Materializing table from CDC events with Spark Declarative Pipelines
-- MAGIC
-- MAGIC In this example, we'll synchronize data from the Customers table in our MySQL database.
-- MAGIC
-- MAGIC - We extract the changes from our transactional database using Debezium or any other tool and save them in a cloud object storage (S3 folder, ADLS, GCS).
-- MAGIC - Using Autoloader we incrementally load the messages from cloud object storage, and stores the raw messages them in the `customers_cdc`. Autoloader will take care of infering the schema and handling schema evolution for us.
-- MAGIC - Then we'll add a view `customers_cdc_clean` to check the quality of our data, using expectation, and then build dashboards to track data quality. As example the ID should never be null as we'll use it to run our upsert operations.
-- MAGIC - Finally we perform the APPLY CHANGES INTO (doing the upserts) on the cleaned cdc data to apply the changes to the final `customers` table
-- MAGIC - Extra: we'll also see how Spark Declarative Pipelines can simply create Slowly Changing Dimention of type 2 (SCD2) to keep track of all the changes 
-- MAGIC
-- MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_0.png" width="1100"/>
-- MAGIC
-- MAGIC ## Accessing the Spark Declarative Pipelines
-- MAGIC
-- MAGIC Your pipeline has been created! You can directly access the <a dbdemos-pipeline-id="sdp-cdc" href="/#joblist/pipelines/c1ccc647-74e6-4754-9c61-6f2691456a73">Spark Declarative Pipelines for CDC</a>.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### CDC input from tools like Debezium
-- MAGIC
-- MAGIC For each change, we receive a JSON message containing all the fields of the row being updated (customer name, email, address...). In addition, we have extra metadata informations including:
-- MAGIC
-- MAGIC - operation: an operation code, typically (DELETE, APPEND, UPDATE)
-- MAGIC - operation_date: the date and timestamp for the record came for each operation action
-- MAGIC
-- MAGIC Tools like Debezium can produce more advanced output such as the row value before the change, but we'll exclude them for the clarity of the demo

-- COMMAND ----------

-- DBTITLE 1,Input data from CDC
-- MAGIC %python
-- MAGIC display(spark.read.json("/Volumes/main__build/dbdemos_dlt_cdc/raw_data/customers"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Data Exploration
-- MAGIC
-- MAGIC All Data projects start with some exploration. Open the [/1-sdp-sql/explorations/sample_exploration]($./1-sdp-sql/explorations/sample_exploration) notebook to get started and discover the data made available to you

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Ready to implement your pipeline ? 
-- MAGIC
-- MAGIC Open [/1-sdp-sql/transformations/01-sql_cdc_pipeline.sql]($./1-sdp-sql/transformations/01-sql_cdc_pipeline.sql) to get started and explore how to do CDC with SDP!
-- MAGIC
-- MAGIC The sql script implements the following steps:

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Ingesting data with Autoloader
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_1.png" width="700" style="float: right" />
-- MAGIC
-- MAGIC Our first step is to ingest the data from the cloud storage. Again, this could be from any other source like (message queue etc).
-- MAGIC
-- MAGIC This can be challenging for multiple reason. We have to:
-- MAGIC
-- MAGIC - operate at scale, potentially ingesting millions of small files
-- MAGIC - infer schema and json type
-- MAGIC - handle bad record with incorrect json schema
-- MAGIC - take care of schema evolution (ex: new column in the customer table)
-- MAGIC
-- MAGIC Databricks Autoloader solves all these challenges out of the box.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Cleanup & expectations to track data quality
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_2.png" width="700" style="float: right" />
-- MAGIC
-- MAGIC Next, we'll add expectations to controle data quality. To do so, we'll create a view (we don't need to duplicate the data) and check the following conditions:
-- MAGIC
-- MAGIC - ID must never be null
-- MAGIC - the cdc operation type must be valid
-- MAGIC - the json must have been properly read by the autoloader
-- MAGIC
-- MAGIC If one of these conditions isn't respected, we'll drop the row.
-- MAGIC
-- MAGIC These expectations metrics are saved as technical tables and can then be re-used with Databricks SQL to track data quality over time.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Materializing the silver table with APPLY CHANGES
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_3.png" width="700" style="float: right" />
-- MAGIC
-- MAGIC The silver `customer` table will contains the most up to date view. It'll be a replicate of the original table.
-- MAGIC
-- MAGIC This is non trivial to implement manually. You need to consider things like data deduplication to keep the most recent row.
-- MAGIC
-- MAGIC Thanksfully Spark Declarative Pipelines solve theses challenges out of the box with the `APPLY CHANGE` operation

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### 4/ Slowly Changing Dimension of type 2 (SCD2)
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_4.png" width="700" style="float: right" />
-- MAGIC
-- MAGIC #### Why SCD2
-- MAGIC
-- MAGIC It's often required to create a table tracking all the changes resulting from APPEND, UPDATE and DELETE:
-- MAGIC
-- MAGIC * History: you want to keep an history of all the changes from your table
-- MAGIC * Traceability: you want to see which operation
-- MAGIC
-- MAGIC #### SCD2 with Spark Declarative Pipelines
-- MAGIC
-- MAGIC Delta support CDF (Change Data Flow) and `table_change` can be used to query the table modification in a SQL/python. However, CDF main use-case is to capture changes in a pipeline and not create a full view of the table changes from the begining. 
-- MAGIC
-- MAGIC Things get especially complex to implement if you have out of order events. If you need to sequence your changes by a timestamp and receive a modification which happened in the past, then you not only need to append a new entry in your SCD table, but also update the previous entries.  
-- MAGIC
-- MAGIC Delta
-- MAGIC  Live Table makes all this logic super simple and let you create a separate table containing all the modifications, from the begining of the time. This table can then be used at scale, with specific partitions / zorder columns if required. Out of order fields will be handled out of the box based on the _sequence_by 
-- MAGIC
-- MAGIC To create a SCD2 table, all we have to do is leverage the `APPLY CHANGES` with the extra option: `STORED AS {SCD TYPE 1 | SCD TYPE 2 [WITH {TIMESTAMP|VERSION}}]`
-- MAGIC
-- MAGIC *Note: you can also limit the columns being tracked with the option: `TRACK HISTORY ON {columnList |* EXCEPT(exceptColumnList)}*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Conclusion 
-- MAGIC We now have <a dbdemos-pipeline-id="sdp-cdc" href="/#joblist/pipelines/c1ccc647-74e6-4754-9c61-6f2691456a73">our Spark Declarative Pipelines</a> up & ready! Our `customers` table is materialize and we can start building BI report to analyze and improve our business. It also open the door to Data Science and ML use-cases such as customer churn, segmentation etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Going further: implementing a CDC pipeline using Declarative Pipeline for N tables
-- MAGIC
-- MAGIC We saw previously how to setup a CDC pipeline for a single table. However, real-life database typically involve multiple tables, with 1 CDC folder per table.
-- MAGIC
-- MAGIC Operating and ingesting all these tables at scale is quite challenging. You need to start multiple table ingestion at the same time, working with threads, handling errors, restart where you stopped, deal with merge manually.
-- MAGIC
-- MAGIC Thankfully, your pipeline takes care of that for you. We can leverage python loops to naturally iterate over the folders (see the [documentation](https://docs.databricks.com/aws/en/dlt/) for more details)
-- MAGIC
-- MAGIC SDP engine will handle the parallelization whenever possible, and autoscale based on your data volume.
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt_pipeline_full.png" width="1000"/>
-- MAGIC
-- MAGIC
-- MAGIC Open the [/2-sdp-python/transformations/01-full_python_pipeline.py]($./2-sdp-python/transformations/01-full_python_pipeline.py) to see how it's done!
