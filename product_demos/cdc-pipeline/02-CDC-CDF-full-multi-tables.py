# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Full demo: Change Data Capture on multiple tables
# MAGIC ## Use-case: Synchronize all your ELT tables with your Lakehouse
# MAGIC
# MAGIC We previously saw how to synchronize a single table. However, real use-case typically includes multiple tables that we need to ingest and synch.
# MAGIC
# MAGIC These tables are stored on different folder having the following layout:
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-full.png">
# MAGIC
# MAGIC **A note on Spark Declarative Pipelines**:<br/>
# MAGIC *Spark Declarative Pipelines has been designed to simplify this process and handle concurrent execution properly, without having you to start multiple stream in parallel.*<br/>
# MAGIC *We strongly advise to have a look at the SDP CDC demo to simplify such pipeline implementation: `dbdemos.instal('dlt-cdc')`*
# MAGIC
# MAGIC In this notebook, we'll see how this can be done using Python & standard streaming APIs (without SDP).
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=02-CDC-CDF-full-multi-tables&demo_name=cdc-pipeline&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running the streams in parallel
# MAGIC
# MAGIC Each table will be save as a distinct table, using a distinct Spark Structured Streaming strem.
# MAGIC
# MAGIC To implement an efficient pipeline, we should process multiple streams at the same time. To do that, we'll use a ThreadPoolExecutor and start multiple thread, each of them processing and waiting for a stream.
# MAGIC
# MAGIC We're using Trigger Once to refresh all the tables once and then shutdown the cluster, typically every hour. For lower latencies we can keep the streams running (depending of the number of tables & cluster size), or keep the Trigger Once but loop forever.
# MAGIC
# MAGIC *Note that for a real workload the exact number of streams depends of the total number of tables, table sizes and cluster size. We can also use several clusters to split the load if required*

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Schema evolution
# MAGIC
# MAGIC By organizing the raw incoming cdc files with 1 folder by table, we can easily iterate over the folders and pickup any new tables without modification.
# MAGIC
# MAGIC Schema evolution will be handled my the Autoloader and Delta `mergeSchema` option at the bronze layer. Schema evolution for MERGE (Silver Layer) are supported using `spark.databricks.delta.schema.autoMerge.enabled`
# MAGIC
# MAGIC Using these options, we'll be able to capture new tables and table schema evolution without having to change our code.
# MAGIC
# MAGIC *Note: that autoloader will trigger an error in a stream if a schema change happens, and will automatically recover during the next run. See Autoloader demo for a complete example*
# MAGIC
# MAGIC *Note: another common pattern is to redirect all the CDC events to a single message queue (the table name being a message attribute), and then dispatch the message in different Silver Tables*

# COMMAND ----------

# DBTITLE 1,Let's explore our raw cdc data. We have 2 tables we want to sync (transactions and users)
base_folder = f"{raw_data_location}/cdc"
display(dbutils.fs.ls(base_folder))

# COMMAND ----------

# MAGIC %md ## Silver and bronze transformations

# COMMAND ----------

# DBTITLE 1,let's reset all checkpoints
dbutils.fs.rm(f"{raw_data_location}/cdc_full", True)

# COMMAND ----------

# DBTITLE 1,Bronze ingestion with autoloader

#Stream using the autoloader to ingest raw files and load them in a delta table
def update_bronze_layer(path, bronze_table):
  print(f"ingesting RAW cdc data for {bronze_table} and building bronze layer...")
  (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", f"{raw_data_location}/cdc_full/schemas/{bronze_table}")
          .option("cloudFiles.schemaHints", "id bigint, operation_date timestamp")
          .option("cloudFiles.inferColumnTypes", "true")
          .load(path)
       .withColumn("file_name", col("_metadata.file_path"))
       .writeStream
          .option("checkpointLocation", f"{raw_data_location}/cdc_full/checkpoints/{bronze_table}")
          .option("mergeSchema", "true")
          #.trigger(processingTime='10 seconds')
          .trigger(availableNow=True)
          .table(bronze_table).awaitTermination())

# COMMAND ----------

# DBTITLE 1,Silver step: materialize tables with MERGE based on CDC events
#Stream incrementally loading new data from the bronze CDC table and merging them in the Silver table
def update_silver_layer(bronze_table, silver_table):
  print(f"ingesting {bronze_table} update and materializing silver layer using a MERGE statement...")
  #First create the silver table if it doesn't exists:
  if not spark.catalog.tableExists(silver_table):
    print(f"Table {silver_table} doesn't exist, creating it using the same schema as the bronze one...")
    spark.read.table(bronze_table).drop("operation", "operation_date", "_rescued_data", "file_name").write.saveAsTable(silver_table)

  #for each batch / incremental update from the raw cdc table, we'll run a MERGE on the silver table
  def merge_stream(updates, i):
    #First we need to deduplicate based on the id and take the most recent update
    windowSpec = Window.partitionBy("id").orderBy(col("operation_date").desc())
    #Select only the first value 
    #getting the latest change is still needed if the cdc contains multiple time the same id. We can rank over the id and get the most recent _commit_version
    updates_deduplicated = updates.withColumn("rank", row_number().over(windowSpec)).where("rank = 1").drop("operation_date", "_rescued_data", "file_name", "rank")
    #Remove the "operation" field from the column to update in the silver table (we don't want the technical "operation" field to appear here)
    columns_to_update = {c: f"updates.{c}"  for c in spark.read.table(silver_table).columns if c != "operation"}
    #run the merge in the silver table directly
    DeltaTable.forName(spark, silver_table).alias("target") \
        .merge(updates_deduplicated.alias("updates"), "updates.id = target.id") \
        .whenMatchedDelete("updates.operation = 'DELETE'") \
        .whenMatchedUpdate("updates.operation != 'DELETE'", set=columns_to_update) \
        .whenNotMatchedInsert("updates.operation != 'DELETE'", values=columns_to_update) \
        .execute()
    
  (spark.readStream
         .table(bronze_table)
       .writeStream
         .foreachBatch(merge_stream)
         .option("checkpointLocation", f"{raw_data_location}/cdc_full/checkpoints/{silver_table}")
          #.trigger(processingTime='10 seconds')
          .trigger(availableNow=True)
          .start().awaitTermination())

# COMMAND ----------

# MAGIC %md ## Starting all the streams
# MAGIC
# MAGIC We can now iterate over the folders to start the bronze & silver streams for each table.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from collections import deque
from delta.tables import *
 
def refresh_cdc_table(table):
  try:
    #update the bronze table
    bronze_table = f'bronze_{table}'
    update_bronze_layer(f"{base_folder}/{table}", bronze_table)

    #then refresh the silver layer
    silver_table = f'silver_{table}'
    update_silver_layer(bronze_table, silver_table)
  except Exception as e:
    #prod workload should properly process errors
    print(f"couldn't properly process {bronze_table}")
    raise e
  
#Enable Schema evolution during merges (to capture new columns)  
#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

#iterate over all the tables folders
tables = [table_path.name[:-1] for table_path in dbutils.fs.ls(base_folder)]
#Let's start 3 CDC flow at the same time in 3 different thread to speed up ingestion
with ThreadPoolExecutor(max_workers=3) as executor:
  deque(executor.map(refresh_cdc_table, tables))
  print(f"Database refreshed!")

# COMMAND ----------

# MAGIC %sql select * from bronze_users

# COMMAND ----------

# MAGIC %sql select * from silver_users

# COMMAND ----------

# MAGIC %sql select * from silver_transactions

# COMMAND ----------

# MAGIC %md 
# MAGIC ## What's next
# MAGIC
# MAGIC All our silver tables are now materialized using the CDC events! We can then work extra transformation (gold layer) based on your business requirement.
# MAGIC
# MAGIC ### Production readiness
# MAGIC Error and exception in each stream should be properly captured. Multiple strategy exist: send a notification when a table has some error and continue processing the others, stop the entire job, define table "priorities" etc.
# MAGIC
# MAGIC ### Spark Declarative Pipelines
# MAGIC To simplify these operations & error handling, we strongly advise you to run your CDC pipelines on top of Spark Declarative Pipelines: `dbdemos.install('pipeline-bike')`

# COMMAND ----------

# DBTITLE 1,Make sure we stop all actives streams
DBDemos.stop_all_streams()
