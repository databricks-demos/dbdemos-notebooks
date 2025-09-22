# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Full demo: Change Data Capture on multiple tables with Serverless
# MAGIC ## Use-case: Synchronize all your ELT tables with your Lakehouse using Serverless Compute
# MAGIC
# MAGIC We previously saw how to synchronize a single table. However, real use-cases typically include multiple tables that need to be ingested and synchronized.
# MAGIC
# MAGIC These tables are stored in different folders having the following layout:
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-full.png">
# MAGIC
# MAGIC **A note on Delta Live Tables**:<br/>
# MAGIC *Delta Live Tables have been designed to simplify this process and handle concurrent execution properly, without requiring you to start multiple streams in parallel.*<br/>
# MAGIC *We strongly recommend looking at the DLT CDC demo to simplify such pipeline implementation: `dbdemos.install('dlt-cdc')`*
# MAGIC
# MAGIC In this notebook, we'll see how this can be accomplished using **Serverless Compute** with Python & standard streaming APIs (without DLT) for cost-effective, auto-scaling processing.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=02-CDC-CDF-full-multi-tables&demo_name=cdc-pipeline&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running streams in parallel with Serverless Compute
# MAGIC
# MAGIC Each table will be saved as a distinct table, using a distinct Spark Structured Streaming stream.
# MAGIC
# MAGIC To implement an efficient pipeline, we process multiple streams in parallel using a ThreadPoolExecutor with multiple threads, each processing a stream.
# MAGIC
# MAGIC **Serverless Optimization**: We're using `availableNow=True` triggers to process all available data and then shut down automatically. This approach is ideal for:
# MAGIC - **Cost efficiency**: Pay only for actual compute time used
# MAGIC - **Auto-scaling**: Serverless automatically scales resources based on workload
# MAGIC - **Batch processing**: Process all available data efficiently without continuous resource usage
# MAGIC
# MAGIC For scheduled processing (e.g., hourly), you can trigger this notebook via Databricks Jobs or Workflows.
# MAGIC
# MAGIC *Note: The exact number of parallel streams depends on your table count, data volumes, and desired processing time. Serverless compute handles resource scaling automatically.*

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Schema evolution with Serverless
# MAGIC
# MAGIC By organizing the raw incoming CDC files with one folder per table, we can easily iterate over folders and pick up new tables without modification.
# MAGIC
# MAGIC **Schema evolution is handled automatically by**:
# MAGIC - **Auto Loader**: Automatically detects and handles schema changes at the bronze layer
# MAGIC - **Delta `mergeSchema` option**: Enables schema evolution during writes
# MAGIC - **`spark.databricks.delta.schema.autoMerge.enabled`**: Supports schema evolution for MERGE operations (Silver layer)
# MAGIC
# MAGIC **Serverless Benefits for Schema Evolution**:
# MAGIC - Auto Loader with serverless handles schema inference efficiently
# MAGIC - No cluster configuration required for schema evolution
# MAGIC - Automatic recovery from schema change events
# MAGIC
# MAGIC Using these options, we'll capture new tables and table schema evolution without code changes.
# MAGIC
# MAGIC *Note: Auto Loader may pause a stream if a schema change occurs and will automatically recover during the next trigger. This works perfectly with serverless `availableNow` triggers.*
# MAGIC
# MAGIC *Alternative pattern: Redirect all CDC events to a single message queue (with table name as message attribute), then dispatch messages to different Silver tables.*

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

# Stream using Auto Loader to ingest raw files and load them into Delta tables with serverless compute
def update_bronze_layer(path, bronze_table):
  print(f"Ingesting RAW CDC data for {bronze_table} and building bronze layer with serverless...")
  (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", f"{raw_data_location}/cdc_full/schemas/{bronze_table}")
          .option("cloudFiles.schemaHints", "id bigint, operation_date timestamp")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.useNotifications", "false")  # Optimized for serverless
          .option("cloudFiles.includeExistingFiles", "true")  # Process all files on first run
          .load(path)
       .withColumn("file_name", col("_metadata.file_path"))
       .writeStream
          .option("checkpointLocation", f"{raw_data_location}/cdc_full/checkpoints/{bronze_table}")
          .option("mergeSchema", "true")
          .trigger(availableNow=True)  # Serverless trigger for cost-effective processing
          .table(bronze_table).awaitTermination())

# COMMAND ----------

# DBTITLE 1,Silver step: materialize tables with MERGE based on CDC events
# Stream incrementally loading new data from the bronze CDC table and merging them in the Silver table
def update_silver_layer(bronze_table, silver_table):
  print(f"Ingesting {bronze_table} updates and materializing silver layer using MERGE statement with serverless...")
  # First create the silver table if it doesn't exist with optimized properties:
  if not spark.catalog.tableExists(silver_table):
    print(f"Table {silver_table} doesn't exist, creating it with optimized properties...")
    # Create table with sample schema and then optimize properties
    spark.read.table(bronze_table).drop("operation", "operation_date", "_rescued_data", "file_name").write.saveAsTable(silver_table)
    # Add optimized properties for serverless and performance
    spark.sql(f"""
      ALTER TABLE {silver_table} SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true, 
        delta.autoOptimize.autoCompact = true,
        delta.targetFileSize = '128MB',
        delta.tuneFileSizesForRewrites = true
      )
    """)

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
         .trigger(availableNow=True)  # Serverless trigger for cost-effective processing
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
  """
  Process a single CDC table using serverless compute.
  Updates both bronze and silver layers with optimized settings.
  """
  try:
    # Update the bronze table using Auto Loader with serverless optimization
    bronze_table = f'bronze_{table}'
    print(f"Processing table: {table} -> {bronze_table}")
    update_bronze_layer(f"{base_folder}/{table}", bronze_table)

    # Then refresh the silver layer with MERGE operations
    silver_table = f'silver_{table}'
    print(f"Materializing silver table: {silver_table}")
    update_silver_layer(bronze_table, silver_table)
    
    print(f"Successfully processed table: {table}")
  except Exception as e:
    # Production workloads should implement comprehensive error handling
    error_msg = f"Failed to process table {table}: {str(e)}"
    print(error_msg)
    # In production, consider:
    # - Logging to external monitoring systems
    # - Sending alerts/notifications
    # - Continuing with other tables vs stopping entire pipeline
    raise Exception(error_msg) from e
  
# Enable Schema evolution during merges (to capture new columns)  
# Uncomment the line below if you need automatic schema merging during MERGE operations
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Iterate over all table folders and process them in parallel using serverless compute
tables = [table_path.name[:-1] for table_path in dbutils.fs.ls(base_folder)]
print(f"Found {len(tables)} tables to process: {tables}")

# Process multiple CDC flows simultaneously using ThreadPoolExecutor
# Serverless compute automatically scales resources based on workload
max_parallel_tables = min(len(tables), 3)  # Adjust based on your data volume and processing requirements
print(f"Processing {max_parallel_tables} tables in parallel with serverless compute...")

with ThreadPoolExecutor(max_workers=max_parallel_tables) as executor:
  deque(executor.map(refresh_cdc_table, tables))
  print(f"Successfully refreshed all {len(tables)} tables using serverless compute!")

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
# MAGIC All our silver tables are now materialized using CDC events with **Serverless Compute**! You can now build additional transformations (gold layer) based on your business requirements.
# MAGIC
# MAGIC ### Production readiness with Serverless
# MAGIC 
# MAGIC **Error Handling Strategies**:
# MAGIC - Capture and handle exceptions in each stream properly
# MAGIC - Send notifications when a table encounters errors while continuing to process others
# MAGIC - Define table processing priorities and dependencies
# MAGIC - Use Databricks Jobs/Workflows for orchestration and monitoring
# MAGIC
# MAGIC **Serverless Production Benefits**:
# MAGIC - **Cost Optimization**: Pay only for actual processing time
# MAGIC - **Auto-scaling**: Automatically scales based on data volume
# MAGIC - **Reliability**: Built-in fault tolerance and automatic restarts
# MAGIC - **Monitoring**: Integrated with Databricks monitoring and alerting
# MAGIC
# MAGIC **Scheduling Options**:
# MAGIC - Use Databricks Jobs to schedule this notebook regularly (hourly, daily)
# MAGIC - Trigger via external orchestration tools (Apache Airflow, etc.)
# MAGIC - Event-driven execution using file arrival notifications
# MAGIC
# MAGIC ### Delta Live Tables
# MAGIC To simplify these operations & error handling even further, we strongly recommend running your CDC pipelines using Delta Live Tables: `dbdemos.install('delta-live-table')`
# MAGIC
# MAGIC DLT provides native CDC support with `APPLY CHANGES` and automatic error handling, monitoring, and scaling.

# COMMAND ----------

# DBTITLE 1,Make sure we stop all actives streams
DBDemos.stop_all_streams()
