# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Implement CDC: Change Data Capture
# MAGIC ## Use-case: Synchronize your SQL Database with your Lakehouse
# MAGIC
# MAGIC Delta Lake is an <a href="https://delta.io/" target="_blank">open-source</a> storage layer with Transactional capabilities and increased Performances. 
# MAGIC
# MAGIC Delta lake is designed to support CDC workload by providing support for UPDATE / DELETE and MERGE operation.
# MAGIC
# MAGIC In addition, Delta table can support CDC to capture internal changes and propagate the changes downstream.
# MAGIC
# MAGIC Note that this is a fairly advaned demo. Before going into this content, we recommend you get familiar with Delta Lake `dbdemos.install('delta-lake')`.
# MAGIC
# MAGIC ## Simplifying CDC with Delta Live Table
# MAGIC
# MAGIC As you'll see, implementing a CDC pipeline from scratch is slightly advanced. 
# MAGIC
# MAGIC To simplify these operation & implement a full CDC flow with SQL expression, we strongly advise to use Delta Live Table with `APPLY CHANGES`: `dbdemos.install('delta-live-table')` (including native SCDT2 support)
# MAGIC
# MAGIC As you'll see, `APPLY CHANGES` handles the MERGE INTO + DEDUPLICATION complexity for you. 
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fcdc_cdf%2Fcdc_notebook&dt=DELTA">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/delta_cdf.png" alt='Delta Lake Change Data Feed'/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC flow
# MAGIC
# MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-0.png" alt='Make all your data ready for BI and ML'/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Bronze: Incremental data loading using Auto Loader
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-1.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC
# MAGIC Working with external system can be challenging due to schema update. The external database can have schema update, adding or modifying columns, and our system must be robust against these changes.
# MAGIC
# MAGIC Databricks Autoloader (`cloudFiles`) handles schema inference and evolution out of the box.
# MAGIC
# MAGIC For more details on Auto Loader, run `dbdemos.install('auto-loader')`

# COMMAND ----------

from pyspark.sql.functions import input_file_name, col
from pyspark.sql import DataFrame
import time

# COMMAND ----------

# DBTITLE 1,Let's explore our incoming data. We receive CSV files with client information
cdc_raw_data = spark.read.option('header', "true").csv(volume_folder+'/user_csv')
display(cdc_raw_data)

# COMMAND ----------

# DBTITLE 1,Our CDC is sending 3 type of operation: APPEND, DELETE and UPDATE
display(cdc_raw_data.dropDuplicates(['operation']))

# COMMAND ----------

# DBTITLE 1,We need to keep the cdc information, however csv isn't a efficient storage. Let's put that in a Delta table instead:
bronzeDF = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation",  volume_folder+"/schema_cdc_raw")
        .option("cloudFiles.schemaHints", "id bigint, operation_date timestamp")
        .load(volume_folder+'/user_csv'))

(bronzeDF.withColumn("file_name", col("_metadata.file_path")).writeStream
        .option("checkpointLocation", volume_folder+"/checkpoint_cdc_raw")
        .trigger(availableNow=True)
        .table(f"`{catalog}`.`{db}`.clients_cdc")
        .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's make sure our table has the proper compaction settings to support streaming
# MAGIC ALTER TABLE clients_cdc SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
# MAGIC
# MAGIC SELECT * FROM clients_cdc order by id asc ;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Silver: Materialize the table
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-2.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC
# MAGIC The silver `retail_client_silver` table will contains the most up to date view. It'll be a replicat of the original MYSQL table.
# MAGIC
# MAGIC Because we'll propagate the `MERGE` operations downstream to the `GOLD` layer, we need to enable Delta Lake CDF: `delta.enableChangeDataFeed = true`

# COMMAND ----------

# DBTITLE 1,We can now create our client table using a standard SQL command
# MAGIC %sql 
# MAGIC -- we can add NOT NULL in our ID field (or even more advanced constraint)
# MAGIC CREATE TABLE IF NOT EXISTS retail_client_silver (
# MAGIC   id BIGINT NOT NULL,    
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   email STRING,
# MAGIC   operation STRING,
# MAGIC   CONSTRAINT id_pk PRIMARY KEY(id))
# MAGIC TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true, 
# MAGIC   delta.autoOptimize.optimizeWrite = true, 
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,And run our MERGE statement the upsert the CDC information in our final table
def merge_stream(df: DataFrame, i):
  """
    Processes a microbatch of CDC (Change Data Capture) data to merge it into the 'retail_client_silver' table. 
    This method performs deduplication and upserts or deletes records based on the operation specified in each row.

    Args:
    df (DataFrame): The DataFrame representing the microbatch of CDC data.
    i (int): The batch ID, not directly used in this process.
    
    The method performs these steps:
    1. Temporarily registers the DataFrame as 'clients_cdc_microbatch' to allow SQL operations.
    2. Deduplicates the incoming data by 'id', keeping the latest operation for each 'id'.
    3. Executes a MERGE SQL operation on 'retail_client_silver':
       - Deletes records if the latest operation for an 'id' is 'DELETE'.
       - Updates records for an 'id' if the latest operation is not 'DELETE'.
       - Inserts new records if an 'id' does not exist in 'retail_client_silver' and the operation is not 'DELETE'.
  """
  
  df.createOrReplaceTempView("clients_cdc_microbatch")

  df.sparkSession.sql("""MERGE INTO retail_client_silver target
                                USING
                                (select id, name, address, email, operation from 
                                  (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY operation_date DESC) as rank from clients_cdc_microbatch) 
                                 where rank = 1
                                ) as source
                                ON source.id = target.id
                                WHEN MATCHED AND source.operation = 'DELETE' THEN DELETE
                                WHEN MATCHED AND source.operation != 'DELETE' THEN UPDATE SET *
                                WHEN NOT MATCHED AND source.operation != 'DELETE' THEN INSERT *""")
  
def trigger_silver_stream():
  """
    Initiates a structured streaming process that reads change data capture (CDC) records from a specified table and processes them in batches using a custom merge function. The process is designed to handle streaming updates efficiently, applying changes to a 'silver' table based on the incoming stream.
  """
  (spark.readStream
        .table(f"`{catalog}`.`{db}`.clients_cdc")
      .writeStream
        .foreachBatch(merge_stream)
        .option("checkpointLocation", volume_folder+"/checkpoint_clients_cdc")
        .trigger(availableNow=True)
      .start()
      .awaitTermination())

trigger_silver_stream()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail_client_silver order by id asc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing the first CDC layer
# MAGIC Let's send a new CDC entry to simulate an update and a DELETE for the ID 1000 and 2000

# COMMAND ----------

# DBTITLE 1,Let's UPDATE id=1000 and DELETE the row with id=2000
# MAGIC %sql 
# MAGIC insert into clients_cdc  (id, name, address, email, operation_date, operation, _rescued_data, file_name) values 
# MAGIC     (1000, "Quentin", "123 Paper Street, UT 75020", "quentin.ambard@databricks.com", now(), "UPDATE", null, null),
# MAGIC     (2000, null, null, null, now(), "DELETE", null, null);
# MAGIC     
# MAGIC select * from clients_cdc where id in (1000, 2000);

# COMMAND ----------

# explicitly trigger the stream in our example; It's equally easy to just have the stream run 24/7
trigger_silver_stream()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail_client_silver where id in (1000, 2000);
# MAGIC -- Note that ID 1000 has been updated, and ID 2000 is deleted

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Gold: capture and propagate Silver modifications downstream
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-3.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC
# MAGIC We need to add a final Gold layer based on the data from the Silver table. If a row is DELETED or UPDATED in the SILVER layer, we want to apply the same modification in the GOLD layer.
# MAGIC
# MAGIC To do so, we need to capture all the tables changes from the SILVER layer and incrementally replicate the changes to the GOLD layer.
# MAGIC
# MAGIC This is very simple using Delta Lake CDF from our SILVER table!
# MAGIC
# MAGIC Delta Lake CDF provides the `table_changes('< table_name >', < delta_version >)` that you can use to select all the tables modifications from a specific Delta version to another one:

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Delta Lake CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC --Remember, CDC must be enabled in the silver table to capture the change. Let's make sure it's properly enabled:
# MAGIC ALTER TABLE retail_client_silver SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Delta Lake CDF works using table_changes function:
# MAGIC SELECT * FROM table_changes('retail_client_silver', 1) order by id

# COMMAND ----------

# MAGIC %md #### Delta CDF table_changes output
# MAGIC Table Changes provides back 4 cdc types in the "_change_type" column:
# MAGIC
# MAGIC | CDC Type             | Description                                                               |
# MAGIC |----------------------|---------------------------------------------------------------------------|
# MAGIC | **update_preimage**  | Content of the row before an update                                       |
# MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
# MAGIC | **delete**           | Content of a row that has been deleted                                    |
# MAGIC | **insert**           | Content of a new row that has been inserted                               |
# MAGIC
# MAGIC Therefore, 1 update will result in 2 rows in the cdc stream (one row with the previous values, one with the new values)

# COMMAND ----------

# DBTITLE 1,Getting the last modifications with the Python API
from delta.tables import *

#Let's get the last table version to only see the last update mofications
last_version = str(DeltaTable.forName(spark, "retail_client_silver").history(1).head()["version"])
print(f"our Delta table last version is {last_version}, let's select the last changes to see our DELETE and UPDATE operations (last 2 versions):")

changes = spark.read.format("delta") \
                    .option("readChangeData", "true") \
                    .option("startingVersion", int(last_version) -1) \
                    .table(f"`{catalog}`.`{db}`.retail_client_silver")
display(changes)

# COMMAND ----------

# MAGIC %md ### Synchronizing our downstream GOLD table based from the Silver changes
# MAGIC
# MAGIC Let's now say that we want to perform another table enhancement and propagate these changes downstream.
# MAGIC
# MAGIC To keep this example simple, we'll just add a column name `gold_data` with random data, but in real world this could be an aggregation, a join with another datasource, an ML model etc.
# MAGIC
# MAGIC The same logic as the Silver layer must be implemented. Since we now consume the CDF data, we also need to perform a deduplication stage. Let's do it using the python APIs this time for the example.
# MAGIC
# MAGIC *Note: Streaming operations with CDC are supported from DBR 8.1+*

# COMMAND ----------

# DBTITLE 1,Let's create or final GOLD table: retail_client_gold
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_client_gold (
# MAGIC   id BIGINT NOT NULL, 
# MAGIC   name STRING, 
# MAGIC   address STRING, 
# MAGIC   email STRING, 
# MAGIC   gold_data STRING,
# MAGIC   CONSTRAINT gold_id_pk PRIMARY KEY(id));

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now we can create our initial Gold table using the latest version of our Silver table. Keep in mind that we are **not** looking at the Change Data Feed (CDF) here. We are utilizing the latest version of our siler table that is synced with our external table. Also note that some of these states are not real, and only for demonstration.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, regexp_replace, lit

#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(data, batchId):
  #First we need to deduplicate based on the id and take the most recent update
  windowSpec = Window.partitionBy("id").orderBy(col("_commit_version").desc())
  #Select only the first value 
  #getting the latest change is still needed if the cdc contains multiple time the same id. We can rank over the id and get the most recent _commit_version
  data_deduplicated = data.withColumn("rank", dense_rank().over(windowSpec)).where("rank = 1 and _change_type!='update_preimage'").drop("_commit_version", "rank")

  #Add some data cleaning for the gold layer to remove quotes from the address
  data_deduplicated = data_deduplicated.withColumn("address", regexp_replace(col("address"), "\"", ""))
  
  #run the merge in the gold table directly
  (DeltaTable.forName(data.sparkSession, "retail_client_gold").alias("target")
      .merge(data_deduplicated.alias("source"), "source.id = target.id")
      .whenMatchedDelete("source._change_type = 'delete'")
      .whenMatchedUpdateAll("source._change_type != 'delete'")
      .whenNotMatchedInsertAll("source._change_type != 'delete'")
      .execute())


(spark.readStream
       .option("readChangeData", "true")
       .option("startingVersion", 1)
       .table("retail_client_silver")
       .withColumn("gold_data", lit("Delta CDF is Awesome"))
      .writeStream
        .foreachBatch(upsertToDelta)
      .start())

time.sleep(20)

# COMMAND ----------

# DBTITLE 1,Start the gold stream
# MAGIC %sql SELECT * FROM retail_client_gold

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Support for data sharing and Datamesh organization
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
# MAGIC
# MAGIC As we've seen during this demo, you can track all the changes (INSERT/UPDATE/DELETE) from any Detlta table using the CDC option.
# MAGIC
# MAGIC It's then easy to subscribe the table modifications as an incremental process.
# MAGIC
# MAGIC This makes the Data Mesh implementation easy: each Mesh can publish a set of tables, and other meshes can subscribe the original changes.
# MAGIC
# MAGIC They are then in charge of propagating the changes (ex GDPR DELETE) to their own Data Mesh

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Data is now ready for BI & ML use-case !
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-4.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC
# MAGIC We now have our final table, updated based on the initial CDC information we receive.
# MAGIC
# MAGIC As next step, we can leverage Databricks Lakehouse platform to start creating SQL queries / dashboards or ML models

# COMMAND ----------

# MAGIC %md
# MAGIC Next step: [Implement a CDC pipeline for multiple tables]($./02-CDC-CDF-full-multi-tables)

# COMMAND ----------

# DBTITLE 1,Make sure we stop all actives streams
DBDemos.stop_all_streams()
