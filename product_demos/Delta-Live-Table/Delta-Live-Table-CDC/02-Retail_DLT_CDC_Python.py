# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
# MAGIC 
# MAGIC ## Importance of Change Data Capture (CDC)
# MAGIC 
# MAGIC Change Data Capture (CDC) is the process that captures the changes in records made to transactional Database (Mysql, Postgre) or Data Warehouse. CDC captures operations like data deletion, append and updating, typically as a stream to re-materialize the table in external systems.
# MAGIC 
# MAGIC CDC enables incremental loading while eliminating the need for bulk load updating.
# MAGIC 
# MAGIC By capturing CDC events, we can re-materialize the source table as Delta Table in our Lakehouse and start running Analysis on top of it (Data Science, BI), merging the data with external system.
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_cdc%2Fnotebook_dlt_python&dt=DLT_CDC">
# MAGIC <!-- [metadata={"description":"Process CDC from external system and save them as a Delta Table. BRONZE/SILVER.<br/><i>Usage: demo CDC flow.</i>",
# MAGIC  "authors":["mojgan.mazouchi@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into", "cdc", "cdf"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Capturing CDC  
# MAGIC 
# MAGIC A variety of **CDC tools** are available. One of the open source leader solution is Debezium, but other implementation exists simplifying the datasource, such as Fivetran, Qlik Replicate, Streamset, Talend, Oracle GoldenGate, AWS DMS.
# MAGIC 
# MAGIC In this demo we are using CDC data coming from an external system like Debezium or DMS. 
# MAGIC 
# MAGIC Debezium takes care of capturing every changed row. It typically sends the history of data changes to Kafka logs or save them as file. To simplify the demo, we'll consider that our external CDC system is up and running and saving the CDC as JSON file in our blob storage (S3, ADLS, GCS). 
# MAGIC 
# MAGIC Our job is to CDC informations from the `customer` table (json format), making sure they're correct, and then materializing the customer table in our Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materializing table from CDC events with Delta Live Table
# MAGIC 
# MAGIC In this example, we'll synchronize data from the Customers table in our MySQL database.
# MAGIC 
# MAGIC - We extract the changes from our transactional database using Debezium or any other tool and save them in a cloud object storage (S3 folder, ADLS, GCS).
# MAGIC - Using Autoloader we incrementally load the messages from cloud object storage, and stores the raw messages them in the `customers_cdc`. Autoloader will take care of infering the schema and handling schema evolution for us.
# MAGIC - Then we'll add a view `customers_cdc_clean` to check the quality of our data, using expectation, and then build dashboards to track data quality. As example the ID should never be null as we'll use it to run our upsert operations.
# MAGIC - Finally we perform the APPLY CHANGES INTO (doing the upserts) on the cleaned cdc data to apply the changes to the final `customers` table
# MAGIC - Extra: we'll also see how DLT can simply create Slowly Changing Dimention of type 2 (SCD2) to keep track of all the changes 
# MAGIC 
# MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_0.png" width="1100"/>
# MAGIC 
# MAGIC ## Accessing the DLT pipeline
# MAGIC 
# MAGIC Your pipeline has been created! You can directly access the <a dbdemos-pipeline-id="dlt-cdc" href="/#joblist/pipelines/c1ccc647-74e6-4754-9c61-6f2691456a73">Delta Live Table Pipeline for CDC</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CDC input from tools like Debezium
# MAGIC 
# MAGIC For each change, we receive a JSON message containing all the fields of the row being updated (customer name, email, address...). In addition, we have extra metadata informations including:
# MAGIC 
# MAGIC - operation: an operation code, typically (DELETE, APPEND, UPDATE)
# MAGIC - operation_date: the date and timestamp for the record came for each operation action
# MAGIC 
# MAGIC Tools like Debezium can produce more advanced output such as the row value before the change, but we'll exclude them for the clarity of the demo

# COMMAND ----------

# DBTITLE 1,Input data from CDC
display(spark.read.json("/demos/dlt/cdc_raw/customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC with DLT & Python Syntax
# MAGIC 
# MAGIC DLT tables, views, and their associated settings are configured using [decorators](https://www.python.org/dev/peps/pep-0318/#current-syntax).
# MAGIC 
# MAGIC If you're unfamiliar with Python decorators, just note that they are functions or classes preceded with the `@` sign that interact with the next function present in a Python script.
# MAGIC 
# MAGIC The `@dlt.table` decorator is the basic method for turning a Python function into a Delta Live Table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Ingesting data with Autoloader
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_1.png" width="700" style="float: right" />
# MAGIC 
# MAGIC Our first step is to ingest the data from the cloud storage. Again, this could be from any other source like (message queue etc).
# MAGIC 
# MAGIC This can be challenging for multiple reason. We have to:
# MAGIC 
# MAGIC - operate at scale, potentially ingesting millions of small files
# MAGIC - infer schema and json type
# MAGIC - handle bad record with incorrect json schema
# MAGIC - take care of schema evolution (ex: new column in the customer table)
# MAGIC 
# MAGIC Databricks Autoloader solves all these challenges out of the box.

# COMMAND ----------

# DBTITLE 1,Let's ingest our incoming data using Autoloader (cloudFiles)
import dlt
from pyspark.sql.functions import *
##Create the bronze information table containing the raw JSON data taken from the storage path printed in Cmd5 in 00_Retail_Data_CDC_Generator notebook
@dlt.create_table(name="customers_cdc",
                  comment = "New customer data incrementally ingested from cloud object storage landing zone")
def customers_cdc():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/demos/dlt/cdc_raw/customers"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2/ Cleanup & expectations to track data quality
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_2.png" width="700" style="float: right" />
# MAGIC 
# MAGIC Next, we'll add expectations to controle data quality. To do so, we'll create a view (we don't need to duplicate the data) and check the following conditions:
# MAGIC 
# MAGIC - ID must never be null
# MAGIC - the cdc operation type must be valid
# MAGIC - the json must have been properly read by the autoloader
# MAGIC 
# MAGIC If one of these conditions isn't respected, we'll drop the row.
# MAGIC 
# MAGIC These expectations metrics are saved as technical tables and can then be re-used with Databricks SQL to track data quality over time.

# COMMAND ----------

# DBTITLE 1,Silver Layer - Cleansed Table (Impose Constraints)
#This could also be a view: create_view
@dlt.create_table(name="customers_cdc_clean",
                  comment="Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type")
@dlt.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'DELETE', 'UPDATE')")
def customers_cdc_clean():
  return dlt.read_stream("customers_cdc") \
            .select("address", "email", "id", "firstname", "lastname", "operation", "operation_date", "_rescued_data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ Materializing the silver table with APPLY CHANGES
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_3.png" width="700" style="float: right" />
# MAGIC 
# MAGIC The silver `customer` table will contains the most up to date view. It'll be a replicate of the original table.
# MAGIC 
# MAGIC This is non trivial to implement manually. You need to consider things like data deduplication to keep the most recent row.
# MAGIC 
# MAGIC Thanksfully Delta Live Table solve theses challenges out of the box with the `APPLY CHANGE` operation

# COMMAND ----------

# DBTITLE 1,Create the target customers table 
dlt.create_target_table(name="customers", comment="Clean, materialized customers")

# COMMAND ----------

dlt.apply_changes(
  target = "customers", #The customer table being materilized
  source = "customers_cdc_clean", #the incoming CDC
  keys = ["id"], #what we'll be using to match the rows to upsert
  sequence_by = col("operation_date"), #we deduplicate by operation date getting the most recent value
  ignore_null_updates = False,
  apply_as_deletes = expr("operation = 'DELETE'"), #DELETE condition
  except_column_list = ["operation", "operation_date", "_rescued_data"]) #in addition we drop metadata columns

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### 4/ Slowly Changing Dimention of type 2 (SCD2)
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt/cdc_dlt_pipeline_4.png" width="700" style="float: right" />
# MAGIC 
# MAGIC #### Why SCD2
# MAGIC 
# MAGIC It's often required to create a table tracking all the changes resulting from APPEND, UPDATE and DELETE:
# MAGIC 
# MAGIC * History: you want to keep an history of all the changes from your table
# MAGIC * Traceability: you want to see which operation
# MAGIC 
# MAGIC #### SCD2 with DLT
# MAGIC 
# MAGIC Delta support CDF (Change Data Flow) and `table_change` can be used to query the table modification in a SQL/python. However, CDF main use-case is to capture changes in a pipeline and not create a full view of the table changes from the begining. 
# MAGIC 
# MAGIC Things get especially complex to implement if you have out of order events. If you need to sequence your changes by a timestamp and receive a modification which happened in the past, then you not only need to append a new entry in your SCD table, but also update the previous entries.  
# MAGIC 
# MAGIC Delta Live Table makes all this logic super simple and let you create a separate table containing all the modifications, from the begining of the time. This table can then be used at scale, with specific partitions / zorder columns if required. Out of order fields will be handled out of the box based on the _sequence_by 
# MAGIC 
# MAGIC To create a SCD2 table, all we have to do is leverage the `APPLY CHANGES` with the extra option: `STORED AS {SCD TYPE 1 | SCD TYPE 2 [WITH {TIMESTAMP|VERSION}}]`
# MAGIC 
# MAGIC *Note: you can also limit the columns being tracked with the option: `TRACK HISTORY ON {columnList |* EXCEPT(exceptColumnList)}*

# COMMAND ----------

#create the table
dlt.create_target_table(name="SCD2_customers", comment="Slowly Changing Dimension Type 2 for customers")

#store all changes as SCD2
dlt.apply_changes(
  target = "SCD2_customers", 
  source = "customers_cdc_clean",
  keys = ["id"], 
  sequence_by = col("operation_date"),
  ignore_null_updates = False,
  apply_as_deletes = expr("operation = 'DELETE'"), 
  except_column_list = ["operation", "operation_date", "_rescued_data"],
  stored_as_scd_type = "2") #Enable SCD2 and store individual updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion 
# MAGIC We now have <a dbdemos-pipeline-id="dlt-cdc" href="/#joblist/pipelines/c1ccc647-74e6-4754-9c61-6f2691456a73">our DLT pipeline</a> up & ready! Our `customers` table is materialize and we can start building BI report to analyze and improve our business. It also open the door to Data Science and ML use-cases such as customer churn, segmentation etc.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Monitoring your data quality metrics with Delta Live Table
# MAGIC 
# MAGIC <img style="float:right" width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC 
# MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
# MAGIC 
# MAGIC This let you build custom dashboards to track those metrics.
# MAGIC 
# MAGIC <a href="/sql/dashboards/b732d677-477b-4b5e-84b6-672174440e7d" target="_blank">Data Quality Dashboard</a>

# COMMAND ----------

# MAGIC %md
# MAGIC For more detail on how to analyse Expectation metrics, open the [03-Retail_DLT_CDC_Monitoring]($./03-Retail_DLT_CDC_Monitoring) notebook.
