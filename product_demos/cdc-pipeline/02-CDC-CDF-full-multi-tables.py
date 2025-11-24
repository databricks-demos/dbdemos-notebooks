# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Multi-Table CDC Pipeline Demo: Change Data Capture with Serverless Compute
# MAGIC ## Step-by-Step Guide to Building a Scalable Multi-Table CDC Pipeline
# MAGIC
# MAGIC This demo shows you how to build a **multi-table Change Data Capture (CDC)** pipeline using **Databricks Serverless Compute** for cost-effective, auto-scaling data processing.
# MAGIC
# MAGIC ### What You'll Learn:
# MAGIC 1. **🔄 Step 1**: Set up multi-table CDC data simulation
# MAGIC 2. **🥉 Step 2**: Build parallel Bronze layers with Auto Loader
# MAGIC 3. **🥈 Step 3**: Create parallel Silver layers with MERGE operations
# MAGIC 4. **🚀 Step 4**: Implement Gold layer with Change Data Feed (CDF)
# MAGIC 5. **📊 Step 5**: Test Continuous multi-table CDC Data processing
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Key Benefits of Serverless Multi-Table CDC:
# MAGIC - 💰 **Cost-effective**: Pay only for compute time used across all tables
# MAGIC - 🚀 **Auto-scaling**: Automatically scales based on total workload
# MAGIC - ⚡ **Parallel Processing**: Process multiple tables simultaneously
# MAGIC - 🔄 **Incremental**: Only processes new/changed data per table
# MAGIC - 📊 **Monitoring**: Track processing across all tables
# MAGIC
# MAGIC ### Prerequisites:
# MAGIC - Completed the single-table CDC demo: `01-CDC-CDF-simple-pipeline.py`
# MAGIC - Understanding of parallel processing concepts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **💡 Alternative Approach**: For production multi-table CDC pipelines, consider using **Delta Live Tables** with `APPLY CHANGES` for simplified implementation: `dbdemos.install('dlt-cdc')`
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=02-CDC-CDF-full-multi-tables&demo_name=cdc-pipeline&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Import Required Functions
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Multi-Table CDC Pipeline Architecture Overview
# MAGIC
# MAGIC Here's the complete multi-table CDC pipeline we'll build using **Serverless Compute**:
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-full.png">
# MAGIC
# MAGIC ### Pipeline Flow:
# MAGIC 1. **📥 Data Sources**: Multiple CDC streams from different tables
# MAGIC 2. **🥉 Bronze Layers**: Parallel raw data ingestion with Auto Loader
# MAGIC 3. **🥈 Silver Layers**: Parallel data cleaning and deduplication
# MAGIC 4. **📊 Analytics**: Real-time insights across all tables
# MAGIC
# MAGIC ### Key Serverless Benefits:
# MAGIC - 💰 **Cost Efficiency**: Pay only for actual compute time used
# MAGIC - 🚀 **Auto-scaling**: Serverless automatically scales resources based on workload
# MAGIC - ⚡ **Parallel Processing**: Process multiple tables simultaneously
# MAGIC - 🔄 **Batch Processing**: Process all available data efficiently without continuous resource usage
# MAGIC
# MAGIC **💡 Note**: For scheduled processing (e.g., hourly), trigger this notebook via Databricks Jobs or Workflows.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 🔄 Step 1: Set up multi-table CDC data simulation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1: Explore Multi-Table CDC Data Structure

# COMMAND ----------

print("🔍 Exploring our multi-table CDC data structure...")
print("We have 2 tables we want to sync: transactions and users")
base_folder = f"{raw_data_location}/cdc"
display(dbutils.fs.ls(base_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Set Up Data Simulation
# MAGIC
# MAGIC To demonstrate serverless processing of multiple CDC streams simultaneously, we'll create data generators for multiple tables that simulate incoming CDC events every 60 seconds.
# MAGIC
# MAGIC ### Why This Matters:
# MAGIC - 🚀 **Parallel Processing**: Shows how serverless handles multiple streams simultaneously
# MAGIC - 💰 **Cost Efficiency**: Demonstrates auto-scaling for varying workloads
# MAGIC - 🔄 **Real-world Simulation**: Mimics multi-table CDC scenarios
# MAGIC - 📊 **Monitoring**: Enables cross-table processing visualization

# COMMAND ----------

# DBTITLE 1,🎯 Step 1.2: Multi-Table CDC Data Generator Implementation
import threading
import time
import random
from datetime import datetime
import pandas as pd

# Global variable to control the data generators
generators_running = False

def generate_user_cdc_record(operation_type="UPDATE", user_id=None):
    """Generate a single user CDC record"""
    if user_id is None:
        user_id = random.randint(1, 500)
    
    operations = {
        "INSERT": {
            "id": user_id,
            "name": f"user_{user_id}_{random.randint(1,99)}",
            "email": f"user{user_id}@company{random.randint(1,10)}.com",
            "address": f"Address_{random.randint(1,999)} Street",
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "INSERT"
        },
        "UPDATE": {
            "id": user_id,
            "name": f"updated_user_{user_id}",
            "email": f"updated.user{user_id}@newcompany{random.randint(1,5)}.com",
            "address": f"Updated_Address_{random.randint(1,999)} Avenue",
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "UPDATE"
        },
        "DELETE": {
            "id": user_id,
            "name": None,
            "email": None,
            "address": None,
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "DELETE"
        }
    }
    return operations[operation_type]

def generate_transaction_cdc_record(operation_type="INSERT", transaction_id=None):
    """Generate a single transaction CDC record"""
    if transaction_id is None:
        transaction_id = random.randint(1000, 9999)
    
    user_id = random.randint(1, 500)  # Reference to users table
    
    operations = {
        "INSERT": {
            "id": transaction_id,
            "user_id": user_id,
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "transaction_type": random.choice(["purchase", "refund", "transfer"]),
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "INSERT"
        },
        "UPDATE": {
            "id": transaction_id,
            "user_id": user_id,
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "transaction_type": random.choice(["purchase", "refund", "transfer", "adjustment"]),
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "UPDATE"
        },
        "DELETE": {
            "id": transaction_id,
            "user_id": None,
            "amount": None,
            "currency": None,
            "transaction_type": None,
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "DELETE"
        }
    }
    return operations[operation_type]

def continuous_multi_table_generator():
    """Background function that generates CDC data for multiple tables every 60 seconds"""
    global generators_running
    file_counter = 0
    
    while generators_running:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Generate user CDC events
            user_events = []
            num_user_events = random.randint(2, 4)
            for _ in range(num_user_events):
                operation = random.choices(
                    ["INSERT", "UPDATE", "DELETE"], 
                    weights=[40, 50, 10]
                )[0]
                user_events.append(generate_user_cdc_record(operation))
            
            # Generate transaction CDC events
            transaction_events = []
            num_transaction_events = random.randint(3, 6)
            for _ in range(num_transaction_events):
                operation = random.choices(
                    ["INSERT", "UPDATE", "DELETE"], 
                    weights=[70, 25, 5]  # More inserts for transactions
                )[0]
                transaction_events.append(generate_transaction_cdc_record(operation))
            
            # Save user events
            user_df = pd.DataFrame(user_events)
            user_filename = f"users_cdc_{timestamp}_{file_counter}.csv"
            user_file_path = f"{base_folder}/users/{user_filename}"
            
            spark_user_df = spark.createDataFrame(user_df)
            spark_user_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(user_file_path)
            
            # Save transaction events
            transaction_df = pd.DataFrame(transaction_events)
            transaction_filename = f"transactions_cdc_{timestamp}_{file_counter}.csv"
            transaction_file_path = f"{base_folder}/transactions/{transaction_filename}"
            
            spark_transaction_df = spark.createDataFrame(transaction_df)
            spark_transaction_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(transaction_file_path)
            
            print(f"Generated CDC events at {datetime.now()}:")
            print(f"  📁 Users: {num_user_events} events -> {user_filename}")
            print(f"  📁 Transactions: {num_transaction_events} events -> {transaction_filename}")
            
            file_counter += 1
            
            # Wait 60 seconds before next batch
            time.sleep(60)
            
        except Exception as e:
            print(f"Error in multi-table CDC generator: {e}")
            time.sleep(60)

def start_multi_table_generators():
    """Start the multi-table CDC data generators in background"""
    global generators_running
    if not generators_running:
        generators_running = True
        generator_thread = threading.Thread(target=continuous_multi_table_generator, daemon=True)
        generator_thread.start()
        print("🚀 Multi-Table CDC Data Generators started!")
        print("📊 Users and Transactions CDC events will arrive every 60 seconds.")
        print("💡 This simulates continuous multi-table CDC for serverless processing demo.")
        return generator_thread
    else:
        print("Multi-Table CDC Generators are already running!")
        return None

def stop_multi_table_generators():
    """Stop the multi-table CDC data generators"""
    global generators_running
    generators_running = False
    print("🛑 Multi-Table CDC Data Generators stopped.")

# Start the data generators for continuous multi-table simulation
print("Starting multi-table CDC simulation...")
multi_table_generator_thread = start_multi_table_generators()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## **🥉 Step 2**: Build parallel Bronze layers with Auto Loader

# COMMAND ----------

# DBTITLE 1,🥉 Step 2.1: Reset Checkpoints
dbutils.fs.rm(f"{raw_data_location}/cdc_full", True)

# COMMAND ----------

# DBTITLE 1,🥉 Step 2.2: Bronze Ingestion with Auto Loader

# Stream using Auto Loader to ingest raw files and load them into Delta tables with serverless compute
def update_bronze_layer(path, bronze_table):
  print(f"Ingesting RAW CDC data for {bronze_table} and building bronze layer with serverless...")
  
  # Drop existing table if it exists to avoid schema conflicts
  try:
    spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
    print(f"🔄 Dropped existing {bronze_table} table to avoid schema conflicts")
  except:
    pass
  
  (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", f"{raw_data_location}/cdc_full/schemas/{bronze_table}")
          .option("cloudFiles.schemaHints", "id bigint, operation_date timestamp")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.useNotifications", "false")  # Optimized for serverless
          .option("cloudFiles.includeExistingFiles", "false")  # Only new files after checkpoint
          .option("cloudFiles.maxFilesPerTrigger", "10")  # Process in batches for efficiency
          .load(path)
       .withColumn("file_name", col("_metadata.file_path"))
       .withColumn("processing_time", current_timestamp())  # Track when processed
       .writeStream
          .option("checkpointLocation", f"{raw_data_location}/cdc_full/checkpoints/{bronze_table}")
          .option("mergeSchema", "true")  # Enable schema evolution for new columns
          .trigger(availableNow=True)  # Process only new data since last checkpoint
          .table(bronze_table).awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ## **🥈 Step 3**: Create parallel Silver layers with MERGE operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Understanding CDF vs Non-CDF Processing in Multi-Table Scenarios
# MAGIC
# MAGIC **🔍 Key Difference**: CDF only processes **actual changes** per table, while non-CDF processes **all data** across all tables.
# MAGIC
# MAGIC #### **Non-CDF Multi-Table Approach (Inefficient)**:
# MAGIC - 📊 **Processes**: Entire tables every time
# MAGIC - 💰 **Cost**: Very High - reprocesses unchanged data across all tables
# MAGIC - ⏱️ **Time**: Slow - scans all records in all tables
# MAGIC - 🔄 **Example**: If you have 5 tables with 1M records each, processes all 5M even for 1 change in 1 table
# MAGIC
# MAGIC #### **CDF Multi-Table Approach (Efficient)**:
# MAGIC - 📊 **Processes**: Only changed records per table
# MAGIC - 💰 **Cost**: Low - only pays for actual changes per table
# MAGIC - ⏱️ **Time**: Fast - processes only deltas per table
# MAGIC - 🔄 **Example**: If you have 5 tables with 1M records each but only 1 table has 5 changes, processes only 5 records
# MAGIC
# MAGIC **💡 Multi-Table CDF Benefits**: Up to 99.9%+ reduction in processing volume for incremental changes across multiple tables!
# MAGIC
# MAGIC ### 3.2 Silver Layer with MERGE Operations 
# MAGIC

# COMMAND ----------

# Stream incrementally loading new data from the bronze CDC table and merging them in the Silver table
# This function demonstrates CDF efficiency by processing only changed records per table
def update_silver_layer(bronze_table, silver_table):
  print(f"🔄 Processing {bronze_table} updates with CDF efficiency...")
  
  # Get total records in bronze table to show processing volume
  try:
    total_bronze_records = spark.sql(f"SELECT COUNT(*) as count FROM {bronze_table}").collect()[0]['count']
    print(f"   📊 Total records in {bronze_table}: {total_bronze_records:,}")
  except:
    total_bronze_records = 0
    print(f"   📊 Total records in {bronze_table}: {total_bronze_records:,}")
  
  # First create the silver table if it doesn't exist with optimized properties:
  if not spark.catalog.tableExists(silver_table):
    print(f"   🏗️ Creating {silver_table} with optimized properties...")
    # Create table with sample schema and then optimize properties
    spark.read.table(bronze_table).drop("operation", "operation_date", "_rescued_data", "file_name").write.saveAsTable(silver_table)
    # Add optimized properties for serverless and performance
    spark.sql(f"""
      ALTER TABLE {silver_table} SET TBLPROPERTIES (
        delta.enableChangeDataFeed = true,
        delta.autoOptimize.optimizeWrite = true, 
        delta.autoOptimize.autoCompact = true,
        delta.targetFileSize = '128MB',
        delta.tuneFileSizesForRewrites = true
      )
    """)

  # Process only new records since last checkpoint (CDF efficiency)
  print(f"   🔄 Processing only new records from {bronze_table}...")

  #for each batch / incremental update from the raw cdc table, we'll run a MERGE on the silver table
  def merge_stream(updates, i):
    records_in_batch = updates.count()
    print(f"   📊 Batch {i}: Processing {records_in_batch:,} records")
    
    if records_in_batch > 0 and total_bronze_records > 0:
      # Show processing efficiency
      efficiency = ((total_bronze_records - records_in_batch) / total_bronze_records * 100)
      print(f"   💰 Processing efficiency: {efficiency:.1f}% reduction vs full table scan")
      print(f"   ⚡ Speed improvement: {total_bronze_records / max(records_in_batch, 1):.1f}x faster")
    
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
    
    print(f"   ✅ Batch {i} completed - processed {records_in_batch:,} records efficiently")
    
  print(f"🚀 Starting {silver_table} processing with CDF efficiency...")
  (spark.readStream
         .table(bronze_table)
       .writeStream
         .foreachBatch(merge_stream)
         .option("checkpointLocation", f"{raw_data_location}/cdc_full/checkpoints/{silver_table}")
         .option("mergeSchema", "true")  # Enable schema evolution for silver layer
         .trigger(availableNow=True)  # Process only new data since last checkpoint
          .start().awaitTermination())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3.3 Multi-Table CDF Processing Volume Summary
# MAGIC
# MAGIC **🎯 What We Just Demonstrated**:
# MAGIC - **CDF Processing**: Only processed actual changes per table
# MAGIC - **Volume Efficiency**: Dramatically reduced processing volume across multiple tables
# MAGIC - **Cost Savings**: Significant reduction in compute costs per table
# MAGIC - **Performance**: Much faster processing times per table
# MAGIC
# MAGIC **📊 Key Metrics Per Table**:
# MAGIC - **Total Bronze Records**: Shows full table size per table
# MAGIC - **CDF Records Processed**: Shows only changed records per table
# MAGIC - **Efficiency Gain**: Percentage reduction in processing volume per table
# MAGIC - **Speed Improvement**: Multiplier for processing speed per table
# MAGIC
# MAGIC **💡 Multi-Table Impact**: In production, this can mean processing 1,000 records across 5 tables instead of 5,000,000 records for incremental updates!
# MAGIC
# MAGIC ### 3.4 Starting all the streams
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
  
# Schema evolution is handled automatically by:
# - Auto Loader with mergeSchema=true option
# - Delta table mergeSchema=true in writeStream operations
# - No additional configuration needed for modern Databricks Runtime

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

# MAGIC %md
# MAGIC ### 3.3 Check the Resulting Silver Tables

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

# MAGIC %md
# MAGIC ## 5. Test Continuous Multi-Table Serverless CDC Processing
# MAGIC
# MAGIC With multiple data generators running, we can demonstrate how serverless compute handles continuous multi-table CDC processing efficiently and cost-effectively. The pipeline processes **only newly arrived data** across all tables.
# MAGIC
# MAGIC **Multi-Table Incremental Processing:**
# MAGIC - ✅ **Per-Table Checkpoints**: Each table tracks its own processing progress
# MAGIC - ✅ **Parallel Incremental Processing**: Multiple tables process only new data simultaneously
# MAGIC - ✅ **Independent Scaling**: Each table scales based on its own data volume
# MAGIC - ✅ **No Cross-Table Reprocessing**: Changes in one table don't affect others
# MAGIC - ✅ **Efficient Resource Usage**: Pay only for actual new data processing

# COMMAND ----------

# DBTITLE 1,🚀 Step 4.1: Multi-Table Pipeline Trigger Function
def trigger_multi_table_cdc_pipeline():
    """
    Trigger all multi-table CDC streams to process new data with serverless compute.
    This processes all tables in parallel for maximum efficiency.
    """
    print(f"🔄 Triggering multi-table CDC pipeline at {datetime.now()}")
    
    # Enable automatic schema merging for MERGE operations across all tables
    # Schema evolution is handled automatically by mergeSchema=true in writeStream operations
    
    # Get all table folders
    tables = [table_path.name[:-1] for table_path in dbutils.fs.ls(base_folder)]
    print(f"📊 Processing {len(tables)} tables: {tables}")
    
    # Process all tables in parallel using ThreadPoolExecutor
    max_parallel_tables = min(len(tables), 3)
    print(f"⚡ Processing {max_parallel_tables} tables in parallel with serverless compute...")
    
    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=max_parallel_tables) as executor:
        deque(executor.map(refresh_cdc_table, tables))
    
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    print(f"✅ Multi-table CDC pipeline completed in {processing_time:.2f} seconds")
    return processing_time

# COMMAND ----------

# DBTITLE 1,🚀 Step 4: Complete Multi-Table CDC Pipeline Demo
print("🎯 Running multi-table serverless CDC processing demonstration...")
print("💡 In production, schedule this via Databricks Jobs/Workflows")

# Give generators time to create files for both tables
print("⏳ Waiting 65 seconds for multi-table data generators to create new files...")
time.sleep(65)

# Process all tables and measure performance
start_time = datetime.now()
processing_time = trigger_multi_table_cdc_pipeline()
total_time = (datetime.now() - start_time).total_seconds()

print(f"\n📈 Performance Metrics:")
print(f"🔹 Total processing time: {total_time:.2f} seconds")
print(f"🔹 Parallel execution efficiency: {(processing_time/total_time)*100:.1f}%")

# Show results with multi-table growth monitoring
print("\n📊 Monitoring multi-table growth over time...")
print("💡 Watch how serverless compute handles growing data across multiple tables")

# Function to get all table sizes
def get_all_table_sizes():
    sizes = {}
    tables = ["users", "transactions"]
    
    for table in tables:
        bronze_table = f"bronze_{table}"
        silver_table = f"silver_{table}"
        
        try:
            sizes[f"{table}_bronze"] = spark.sql(f"SELECT COUNT(*) as count FROM {bronze_table}").collect()[0]['count']
        except:
            sizes[f"{table}_bronze"] = 0
            
        try:
            sizes[f"{table}_silver"] = spark.sql(f"SELECT COUNT(*) as count FROM {silver_table}").collect()[0]['count']
        except:
            sizes[f"{table}_silver"] = 0
    
    return sizes

# Monitor multi-table growth over multiple iterations
print("🔍 Multi-Table Growth Monitoring:")
print("=" * 80)

for iteration in range(1, 4):  # Monitor 3 iterations
    print(f"\n📈 Iteration {iteration} - {datetime.now().strftime('%H:%M:%S')}")
    
    # Get current sizes
    sizes = get_all_table_sizes()
    
    print("🥉 Bronze Tables (Raw CDC):")
    print(f"   👥 Users: {sizes['users_bronze']:,} records")
    print(f"   💳 Transactions: {sizes['transactions_bronze']:,} records")
    print(f"   📊 Total Bronze: {sizes['users_bronze'] + sizes['transactions_bronze']:,} records")
    
    print("🥈 Silver Tables (Materialized):")
    print(f"   👥 Users: {sizes['users_silver']:,} records")
    print(f"   💳 Transactions: {sizes['transactions_silver']:,} records")
    print(f"   📊 Total Silver: {sizes['users_silver'] + sizes['transactions_silver']:,} records")
    
    # Calculate growth if not first iteration
    if iteration > 1:
        users_bronze_growth = sizes['users_bronze'] - previous_sizes['users_bronze']
        users_silver_growth = sizes['users_silver'] - previous_sizes['users_silver']
        transactions_bronze_growth = sizes['transactions_bronze'] - previous_sizes['transactions_bronze']
        transactions_silver_growth = sizes['transactions_silver'] - previous_sizes['transactions_silver']
        
        print("   📊 Growth Since Last Check:")
        print(f"      👥 Users: Bronze +{users_bronze_growth}, Silver +{users_silver_growth}")
        print(f"      💳 Transactions: Bronze +{transactions_bronze_growth}, Silver +{transactions_silver_growth}")
        
        total_growth = (users_bronze_growth + users_silver_growth + 
                       transactions_bronze_growth + transactions_silver_growth)
        print(f"      🎯 Total Growth: +{total_growth} records across all tables")
    
    # Show recent activity details
    print("   🔍 Recent Activity:")
    try:
        # Users operations
        users_ops = spark.sql("""
            SELECT operation, COUNT(*) as count 
            FROM bronze_users 
            GROUP BY operation 
            ORDER BY operation
        """).collect()
        users_summary = {row['operation']: row['count'] for row in users_ops}
        print(f"      👥 Users Operations: {users_summary}")
        
        # Transactions operations  
        trans_ops = spark.sql("""
            SELECT operation, COUNT(*) as count 
            FROM bronze_transactions 
            GROUP BY operation 
            ORDER BY operation
        """).collect()
        trans_summary = {row['operation']: row['count'] for row in trans_ops}
        print(f"      💳 Transactions Operations: {trans_summary}")
        
        # Show latest silver records
        print("   📝 Latest Records:")
        latest_users = spark.sql("""
            SELECT id, name, email 
            FROM silver_users 
            ORDER BY id DESC 
            LIMIT 2
        """).collect()
        if latest_users:
            print("      👥 Latest Users:")
            for row in latest_users:
                print(f"         ID: {row['id']}, User: {row['name']}, Email: {row['email']}")
        
        latest_transactions = spark.sql("""
            SELECT id, amount, item_count 
            FROM silver_transactions 
            ORDER BY id DESC 
            LIMIT 2
        """).collect()
        if latest_transactions:
            print("      💳 Latest Transactions:")
            for row in latest_transactions:
                print(f"         ID: {row['id']}, Amount: {row['amount']}, Items: {row['item_count']}")
                
    except Exception as e:
        print(f"   ⚠️ Error showing details: {e}")
    
    previous_sizes = sizes
    
    # Wait for next iteration (except on last one)
    if iteration < 3:
        print(f"   ⏳ Waiting 65 seconds for more multi-table CDC data...")
        print("   💰 Serverless compute: Zero cost during wait - only pay for processing!")
        time.sleep(65)
        
        # Process new data across all tables
        print(f"   🔄 Processing new multi-table data (Iteration {iteration + 1})...")
        trigger_multi_table_cdc_pipeline()

print("\n" + "=" * 80)
print("✅ Multi-table growth monitoring completed!")
print("📈 Key Multi-Table Observations:")
print("   🔹 Multiple tables grow independently with different patterns")
print("   🔹 Serverless compute scales automatically across all tables")
print("   🔹 Parallel processing efficiency demonstrated")
print("   🔹 Cost optimization: Pay only for actual multi-table processing")
print("   🔹 Real-world enterprise CDC patterns with table relationships")

# COMMAND ----------

# DBTITLE 1,📊 Step 5.1: Cleanup and Stop Generators
stop_multi_table_generators()
DBDemos.stop_all_streams()

print("🎉 Multi-table CDC demo completed!")
print("\n💰 Serverless Benefits Demonstrated:")
print("✅ Cost Optimization: Pay only for actual processing time")
print("✅ Auto-scaling: Handled varying workloads across multiple tables")
print("✅ Parallel Processing: Efficiently processed multiple CDC streams")
print("✅ Zero Infrastructure: No cluster management required")
print("✅ Fault Tolerance: Built-in error handling and recovery")

print(f"\n🚀 Ready for production:")
print("• Schedule via Databricks Jobs/Workflows")
print("• Set up monitoring and alerting")
print("• Configure auto-scaling policies")
print("• Implement error handling strategies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Advantages:
# MAGIC - 🔄 **Parallel Processing**: Multiple tables processed simultaneously
# MAGIC - 📊 **Scalable Architecture**: Easy to add new tables to the pipeline
# MAGIC - 💰 **Cost Efficient**: Pay only for actual processing across all tables
# MAGIC - 🚀 **Auto-scaling**: Serverless handles varying workloads automatically
# MAGIC - 🛡️ **Fault Tolerance**: Isolated processing prevents cross-table failures

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deployment Options:
# MAGIC - 📅 **Scheduled Jobs**: Use Databricks Jobs for automated processing
# MAGIC - 🔄 **Workflows**: Orchestrate complex multi-table pipelines
# MAGIC - 📊 **Monitoring**: Set up alerts and dashboards for all tables
# MAGIC - 🔒 **Security**: Implement proper access controls and data governance
# MAGIC - 💰 **Cost Optimization**: Monitor and optimize serverless compute usage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 8: Next Steps
# MAGIC
# MAGIC ### Continue Your CDC Journey:
# MAGIC - 🏗️ **[Delta Live Tables]($./dlt-cdc)**: Simplified multi-table CDC with `APPLY CHANGES`
# MAGIC - 📚 **[Delta Lake Demo]($./delta-lake)**: Deep dive into Delta Lake features
# MAGIC - 🚀 **[Auto Loader Demo]($./auto-loader)**: Advanced file ingestion patterns
# MAGIC
# MAGIC ### Advanced Patterns:
# MAGIC - 🔄 **Cross-Table Dependencies**: Handle table relationships and dependencies
# MAGIC - 📊 **Data Quality**: Implement validation and quality checks
# MAGIC - 🛡️ **Error Handling**: Advanced retry and recovery strategies
# MAGIC - 📈 **Performance Tuning**: Optimize for large-scale multi-table processing