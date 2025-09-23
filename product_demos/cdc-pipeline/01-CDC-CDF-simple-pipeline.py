# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CDC Pipeline Demo: Change Data Capture with Serverless Compute
# MAGIC ## Step-by-Step Guide to Building a Cost-Effective CDC Pipeline
# MAGIC
# MAGIC This demo shows you how to build a **Change Data Capture (CDC)** pipeline using **Databricks Serverless Compute** for cost-effective, auto-scaling data processing.
# MAGIC
# MAGIC ### What You'll Learn:
# MAGIC 1. **🥉 Step 1**: Set up CDC data simulation
# MAGIC 2. **🥈 Step 2**: Build Bronze layer with Auto Loader
# MAGIC 3. **🥇 Step 3**: Create Silver layer with MERGE operations
# MAGIC 4. **🚀 Step 4**: Implement Gold layer with Change Data Feed (CDF)
# MAGIC 5. **📊 Step 5**: Monitor and optimize with serverless compute
# MAGIC 6. **📊 Step 6**: Data sharing and Datamesh organization
# MAGIC 7. **📊 Step 7**: Data ready for BI & ML use cases
# MAGIC 8. **📊 Step 8**: Next steps and production deployment
# MAGIC
# MAGIC ### Progress Tracking:
# MAGIC - ✅ **Step 1**: CDC data simulation setup
# MAGIC - ⏳ **Step 2**: Bronze layer implementation
# MAGIC - ⏳ **Step 3**: Silver layer implementation
# MAGIC - ⏳ **Step 4**: Gold layer implementation
# MAGIC - ⏳ **Step 5**: Monitoring and optimization
# MAGIC - ⏳ **Step 6**: Data sharing and Datamesh
# MAGIC - ⏳ **Step 7**: BI & ML readiness
# MAGIC - ⏳ **Step 8**: Next steps and deployment
# MAGIC
# MAGIC ### Key Benefits of Serverless CDC:
# MAGIC - 💰 **Cost-effective**: Pay only for compute time used
# MAGIC - 🚀 **Auto-scaling**: Automatically scales based on workload
# MAGIC - ⚡ **Fast processing**: Optimized for batch processing with `availableNow` triggers
# MAGIC - 🔄 **Incremental**: Only processes new/changed data
# MAGIC
# MAGIC ### Prerequisites:
# MAGIC - Basic understanding of Delta Lake: `dbdemos.install('delta-lake')`
# MAGIC - Familiarity with Structured Streaming concepts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **💡 Alternative Approach**: For production CDC pipelines, consider using **Delta Live Tables** with `APPLY CHANGES` for simplified implementation: `dbdemos.install('delta-live-table')` 
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=01-CDC-CDF-simple-pipeline&demo_name=cdc-pipeline&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Import Required Functions
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# DBTITLE 1,Configure Schema Evolution for CDC Processing
# Enable automatic schema merging for all Delta operations to handle schema changes
# Schema evolution is handled automatically by mergeSchema=true in writeStream operations
# Schema inference is handled automatically by Auto Loader with cloudFiles.inferColumnTypes=true

# COMMAND ----------

# MAGIC %md
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/delta_cdf.png" alt='Delta Lake Change Data Feed'/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 CDC Pipeline Architecture Overview
# MAGIC
# MAGIC Here's the complete CDC pipeline we'll build using **Serverless Compute**:
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-0.png" alt='CDC Pipeline Architecture'/>
# MAGIC
# MAGIC ### Pipeline Flow:
# MAGIC 1. **📥 Data Source**: CDC events from external database (simulated)
# MAGIC 2. **🥉 Bronze Layer**: Raw CDC data ingestion with Auto Loader
# MAGIC 3. **🥈 Silver Layer**: Cleaned, deduplicated data with MERGE operations
# MAGIC 4. **🥇 Gold Layer**: Business-ready data with Change Data Feed (CDF)
# MAGIC 5. **📊 Analytics**: Real-time insights and reporting
# MAGIC
# MAGIC **💡 Note**: The incoming data could be any format, including message queues like Kafka.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Step 1: Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-1.png" alt='Bronze Layer' style='float: right' width='600'/>
# MAGIC
# MAGIC ### What We're Building:
# MAGIC - **Purpose**: Ingest raw CDC data from external sources
# MAGIC - **Technology**: Auto Loader with serverless compute
# MAGIC - **Benefits**: Automatic schema evolution and incremental processing
# MAGIC
# MAGIC ### Key Features:
# MAGIC - 🔄 **Schema Evolution**: Handles database schema changes automatically
# MAGIC - 📈 **Incremental Processing**: Only processes new files
# MAGIC - ⚡ **Serverless Scaling**: Auto-scales based on data volume
# MAGIC - 💰 **Cost Efficient**: Pay only for processing time
# MAGIC
# MAGIC **💡 Learn More**: For detailed Auto Loader concepts, run `dbdemos.install('auto-loader')`

# COMMAND ----------

# DBTITLE 1,📊 Step 1.1: Explore Incoming CDC Data
print("🔍 Exploring our incoming CDC data structure...")
cdc_raw_data = spark.read.option('header', "true").csv(raw_data_location+'/user_csv')
display(cdc_raw_data)

# COMMAND ----------

# DBTITLE 1,📊 Step 1.2: Understand CDC Operation Types
print("🔍 Understanding CDC operation types...")
print("Our CDC system sends 3 types of operations:")
display(cdc_raw_data.dropDuplicates(['operation']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Step 1.3: Set Up Continuous CDC Data Simulation
# MAGIC
# MAGIC To demonstrate serverless compute capabilities, we'll create a data generator that simulates incoming CDC events every 60 seconds.
# MAGIC
# MAGIC ### Why This Matters:
# MAGIC - 🚀 **Auto-scaling**: Shows how serverless scales with workload
# MAGIC - 💰 **Cost Efficiency**: Demonstrates `availableNow` trigger benefits
# MAGIC - 🔄 **Real-world Simulation**: Mimics continuous CDC scenarios
# MAGIC - 📊 **Monitoring**: Enables table growth visualization

# COMMAND ----------

# DBTITLE 1,🎯 Step 1.3: CDC Data Generator Implementation
import threading
import time
import random
from datetime import datetime
import pandas as pd

# Global variable to control the data generator
generator_running = False

def generate_cdc_record(operation_type="UPDATE", user_id=None):
    """Generate a single CDC record"""
    if user_id is None:
        user_id = random.randint(1, 1000)
    
    operations = {
        "INSERT": {
            "id": user_id,
            "name": f"User_{user_id}_{random.randint(1,99)}",
            "address": f"Address_{random.randint(1,999)} Street",
            "email": f"user{user_id}@company{random.randint(1,10)}.com",
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "INSERT"
        },
        "UPDATE": {
            "id": user_id,
            "name": f"Updated_User_{user_id}",
            "address": f"New_Address_{random.randint(1,999)} Avenue",
            "email": f"updated.user{user_id}@newcompany{random.randint(1,5)}.com",
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "UPDATE"
        },
        "DELETE": {
            "id": user_id,
            "name": None,
            "address": None,
            "email": None,
            "operation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "operation": "DELETE"
        }
    }
    return operations[operation_type]

def continuous_cdc_generator():
    """Background function that generates CDC data every 120 seconds"""
    global generator_running
    file_counter = 0
    
    while generator_running:
        try:
            # Generate 3-5 random CDC events
            num_events = random.randint(3, 5)
            cdc_events = []
            
            for _ in range(num_events):
                # Random operation type with weighted probability
                operation = random.choices(
                    ["INSERT", "UPDATE", "DELETE"], 
                    weights=[50, 40, 10]  # More inserts/updates than deletes
                )[0]
                cdc_events.append(generate_cdc_record(operation))
            
            # Create DataFrame and save as CSV
            df = pd.DataFrame(cdc_events)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cdc_events_{timestamp}_{file_counter}.csv"
            file_path = f"{raw_data_location}/user_csv/{filename}"
            
            # Convert to Spark DataFrame and save
            spark_df = spark.createDataFrame(df)
            spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
            
            print(f"Generated {num_events} CDC events at {datetime.now()}: {filename}")
            file_counter += 1
            
            # Wait 60 seconds before next batch
            time.sleep(60)
            
        except Exception as e:
            print(f"Error in CDC generator: {e}")
            time.sleep(60)  # Continue even if there's an error

def start_cdc_generator():
    """Start the CDC data generator in background"""
    global generator_running
    if not generator_running:
        generator_running = True
        generator_thread = threading.Thread(target=continuous_cdc_generator, daemon=True)
        generator_thread.start()
        print("🚀 CDC Data Generator started! New data will arrive every 60 seconds.")
        print("💡 This simulates continuous CDC events for serverless processing demonstration.")
        return generator_thread
    else:
        print("CDC Generator is already running!")
        return None

def stop_cdc_generator():
    """Stop the CDC data generator"""
    global generator_running
    generator_running = False
    print("🛑 CDC Data Generator stopped.")

# Start the data generator for continuous simulation
data_generator_thread = start_cdc_generator()

# COMMAND ----------

# DBTITLE 1,🥉 Step 1.4: Create Bronze Delta Table
# Drop existing table if it exists to avoid schema conflicts
try:
    spark.sql("DROP TABLE IF EXISTS clients_cdc")
    print("🔄 Dropped existing clients_cdc table to avoid schema conflicts")
except:
    pass

bronzeDF = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaLocation",  raw_data_location+"/stream/schema_cdc_raw")
                .option("cloudFiles.schemaHints", "id bigint, operation_date timestamp")
                .option("cloudFiles.useNotifications", "false")  # Optimized for serverless
                .option("cloudFiles.includeExistingFiles", "true")  # Process all files on first run
                .load(raw_data_location+'/user_csv'))

(bronzeDF.withColumn("file_name", col("_metadata.file_path"))
        .withColumn("processing_time", current_timestamp())  # Add processing timestamp
        .writeStream
        .option("checkpointLocation", raw_data_location+"/stream/checkpoint_cdc_raw")
        .option("mergeSchema", "true")  # Enable schema evolution
        .trigger(availableNow=True)  # Serverless trigger for cost-effective processing
        .table("clients_cdc"))

time.sleep(20)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize table properties for serverless streaming and performance
# MAGIC ALTER TABLE clients_cdc SET TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true, 
# MAGIC   delta.autoOptimize.autoCompact = true,
# MAGIC   delta.targetFileSize = '128MB',
# MAGIC   delta.tuneFileSizesForRewrites = true
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM clients_cdc order by id asc ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Step 2: Silver Layer - Data Cleaning and Deduplication
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-2.png" alt='Silver Layer' style='float: right' width='600'/>
# MAGIC
# MAGIC ### What We're Building:
# MAGIC - **Purpose**: Clean, deduplicate, and standardize CDC data
# MAGIC - **Technology**: Delta MERGE operations with serverless compute
# MAGIC - **Benefits**: Idempotent processing and data quality
# MAGIC
# MAGIC ### Key Features:
# MAGIC - 🔄 **Idempotent**: Safe to run multiple times
# MAGIC - ⚡ **Serverless**: Auto-scales with data volume
# MAGIC - 💰 **Cost Efficient**: Only processes new/changed data
# MAGIC - 📊 **CDF Enabled**: Tracks changes for downstream processing
# MAGIC
# MAGIC **💡 Note**: We enable Change Data Feed (CDF) to track modifications for the Gold layer.

# COMMAND ----------

# DBTITLE 1,🥈 Step 2.1: Create Silver Table with CDF Enabled
# MAGIC %sql 
# MAGIC -- Create silver table with optimized settings for serverless and CDC
# MAGIC CREATE TABLE IF NOT EXISTS retail_client_silver (id BIGINT NOT NULL, name STRING, address STRING, email STRING, operation STRING) 
# MAGIC   TBLPROPERTIES (
# MAGIC     delta.enableChangeDataFeed = true, 
# MAGIC     delta.autoOptimize.optimizeWrite = true, 
# MAGIC     delta.autoOptimize.autoCompact = true,
# MAGIC     delta.targetFileSize = '128MB',
# MAGIC     delta.tuneFileSizesForRewrites = true
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,🥈 Step 2.2: Implement MERGE Operations
#for each batch / incremental update from the raw cdc table, we'll run a MERGE on the silver table
def merge_stream(df, i):
  df.createOrReplaceTempView("clients_cdc_microbatch")
  #First we need to dedup the incoming data based on ID (we can have multiple update of the same row in our incoming data)
  #Then we run the merge (upsert or delete). We could do it with a window and filter on rank() == 1 too
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
  
(spark.readStream
       .table("clients_cdc")
     .writeStream
       .foreachBatch(merge_stream)
       .option("checkpointLocation", raw_data_location+"/stream/checkpoint_clients_cdc")
       .option("mergeSchema", "true")  # Enable schema evolution for silver layer
       .trigger(availableNow=True)  # Serverless trigger for cost-effective processing
     .start())

time.sleep(20)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail_client_silver order by id asc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥈 Step 2.3: Test CDC Layer
# MAGIC Let's send a new CDC entry to simulate an update and a DELETE for the ID 1 and 2

# COMMAND ----------

# DBTITLE 1,🥈 Step 2.4: Simulate CDC Operations
# MAGIC %sql 
# MAGIC insert into clients_cdc  (id, name, address, email, operation_date, operation, _rescued_data, file_name) values 
# MAGIC             (1000, "Quentin", "Paris 75020", "quentin.ambard@databricks.com", now(), "UPDATE", null, null),
# MAGIC             (2000, null, null, null, now(), "DELETE", null, null);
# MAGIC select * from clients_cdc where id in (1000, 2000);

# COMMAND ----------

#wait for the stream to get the new data
time.sleep(20)

# COMMAND ----------

# DBTITLE 1,🥈 Step 2.5: Verify CDC Processing Results
# MAGIC %sql 
# MAGIC select * from retail_client_silver where id in (1000, 2000);
# MAGIC -- Note that ID 1000 has been updated, and ID 2000 is deleted

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Step 3: Gold Layer - Business-Ready Data with Change Data Feed
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-3.png" alt='Gold Layer' style='float: right' width='600'/>
# MAGIC
# MAGIC ### What We're Building:
# MAGIC - **Purpose**: Create business-ready data from Silver layer changes
# MAGIC - **Technology**: Change Data Feed (CDF) with serverless compute
# MAGIC - **Benefits**: Real-time propagation of Silver layer modifications
# MAGIC
# MAGIC ### How It Works:
# MAGIC - 📊 **CDF Tracking**: Monitors all changes in Silver table
# MAGIC - 🔄 **Real-time Sync**: Applies DELETEs and UPDATEs to Gold layer
# MAGIC - ⚡ **Serverless**: Auto-scales based on change volume
# MAGIC - 💰 **Cost Efficient**: Only processes actual changes
# MAGIC
# MAGIC To do so, we need to capture all the tables changes from the SILVER layer and incrementally replicate the changes to the GOLD layer.
# MAGIC
# MAGIC This is very simple using Delta Lake CDF from our SILVER table!
# MAGIC
# MAGIC Delta Lake CDF provides the `table_changes('< table_name >', < delta_version >)` that you can use to select all the tables modifications from a specific Delta version to another one:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥇 Step 3.1: Working with Delta Lake CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC --Remember, CDC must be enabled in the silver table to capture the change. Let's make sure it's properly enabled:
# MAGIC ALTER TABLE retail_client_silver SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Delta Lake CDF works using table_changes function:
# MAGIC SELECT * FROM table_changes('retail_client_silver', 1)  order by id

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🥇 Step 3.2: Delta CDF table_changes Output
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

# DBTITLE 1,🥇 Step 3.3: Get Modifications with Python API
from delta.tables import *

#Let's get the last table version to only see the last update mofications
last_version = str(DeltaTable.forName(spark, "retail_client_silver").history(1).head()["version"])
print(f"our Delta table last version is {last_version}, let's select the last changes to see our DELETE and UPDATE operations (last 2 versions):")

changes = spark.read.format("delta") \
                    .option("readChangeFeed", "true") \
                    .option("startingVersion", int(last_version) -1) \
                    .table("retail_client_silver")
display(changes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥇 Step 3.1: Synchronize Gold Table with Silver Changes
# MAGIC
# MAGIC Let's now say that we want to perform another table enhancement and propagate these changes downstream.
# MAGIC
# MAGIC To keep this example simple, we'll just add a column name `gold_data` with random data, but in real world this could be an aggregation, a join with another datasource, an ML model etc.
# MAGIC
# MAGIC The same logic as the Silver layer must be implemented. Since we now consume the CDF data, we also need to perform a deduplication stage. Let's do it using the python APIs this time for the example.
# MAGIC
# MAGIC *Note: Streaming operations with CDC are supported from DBR 8.1+*

# COMMAND ----------

# DBTITLE 1,🥇 Step 3.4: Create Gold Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_client_gold (id BIGINT NOT NULL, name STRING, address STRING, email STRING, gold_data STRING)
# MAGIC   TBLPROPERTIES (
# MAGIC     delta.autoOptimize.optimizeWrite = true, 
# MAGIC     delta.autoOptimize.autoCompact = true,
# MAGIC     delta.targetFileSize = '128MB',
# MAGIC     delta.tuneFileSizesForRewrites = true
# MAGIC   );

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, regexp_replace, lit, col, current_timestamp

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
  (DeltaTable.forName(spark, "retail_client_gold").alias("target")
      .merge(data_deduplicated.alias("source"), "source.id = target.id")
      .whenMatchedDelete("source._change_type = 'delete'")
      .whenMatchedUpdateAll("source._change_type != 'delete'")
      .whenNotMatchedInsertAll("source._change_type != 'delete'")
      .execute())


(spark.readStream
       .option("readChangeFeed", "true")  # Updated to use correct option name
       .option("startingVersion", 1)
       .table("retail_client_silver")
       .withColumn("gold_data", lit("Delta CDF is Awesome"))
      .writeStream
        .foreachBatch(upsertToDelta)
        .option("checkpointLocation", raw_data_location+"/stream/checkpoint_clients_gold")
        .option("mergeSchema", "true")  # Enable schema evolution for gold layer
        .trigger(availableNow=True)  # Serverless trigger for cost-effective processing
      .start())

time.sleep(20)

# COMMAND ----------

# MAGIC %sql SELECT * FROM retail_client_gold

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Serverless CDC Processing with Incremental Data Processing
# MAGIC
# MAGIC With the data generator running, you can now demonstrate continuous serverless CDC processing. The pipeline is designed to process **only newly arrived data** using checkpoints and streaming offsets.
# MAGIC
# MAGIC **Key Incremental Processing Features:**
# MAGIC - ✅ **Auto Loader Checkpoints**: Only new files since last processing
# MAGIC - ✅ **Streaming Offsets**: Only new CDC records since last checkpoint  
# MAGIC - ✅ **Change Data Feed**: Only new changes since last processed version
# MAGIC - ✅ **Efficient Processing**: No reprocessing of historical data
# MAGIC - ✅ **Cost Optimization**: Pay only for new data processing

# COMMAND ----------

# DBTITLE 1,🚀 Step 4.1: Serverless Pipeline Trigger Function
def trigger_cdc_pipeline():
    """
    Trigger all CDC streams to process new data with serverless compute.
    This function can be called periodically (every minute, 5 minutes, etc.)
    """
    print(f"🔄 Triggering CDC pipeline at {datetime.now()}")
    
    # Enable automatic schema merging for MERGE operations
    # Schema evolution is handled automatically by mergeSchema=true in writeStream operations
    
    # Stop any existing streams first
    DBDemos.stop_all_streams()
    time.sleep(5)
    
    # Restart bronze layer (Auto Loader) - only process new files since last checkpoint
    print("   🔄 Processing new files for bronze layer...")
    bronzeDF = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "csv")
                    .option("cloudFiles.inferColumnTypes", "true")
                    .option("cloudFiles.schemaLocation",  raw_data_location+"/stream/schema_cdc_raw")
                    .option("cloudFiles.schemaHints", "id bigint, operation_date timestamp")
                    .option("cloudFiles.useNotifications", "false")
                    .option("cloudFiles.includeExistingFiles", "false")  # Only new files after checkpoint
                    .option("cloudFiles.maxFilesPerTrigger", "10")  # Process in batches for efficiency
                    .load(raw_data_location+'/user_csv'))

    (bronzeDF.withColumn("file_name", col("_metadata.file_path"))
            .withColumn("processing_time", current_timestamp())  # Track when processed
            .writeStream
            .option("checkpointLocation", raw_data_location+"/stream/checkpoint_cdc_raw")
            .option("mergeSchema", "true")  # Enable schema evolution for new columns
            .trigger(availableNow=True)  # Process only available new data
            .table("clients_cdc")
            .awaitTermination())
    
    # Restart silver layer (MERGE operations) - only process new CDC records
    print("   🔄 Processing new CDC records for silver layer...")
    (spark.readStream
           .table("clients_cdc")
         .writeStream
           .foreachBatch(merge_stream)
           .option("checkpointLocation", raw_data_location+"/stream/checkpoint_clients_cdc")
           .option("mergeSchema", "true")  # Enable schema evolution for silver layer
           .trigger(availableNow=True)  # Process only new CDC records since last checkpoint
         .start()
         .awaitTermination())
    
    # Restart gold layer (CDF processing) - only process new changes since last checkpoint
    print("   🔄 Processing new changes for gold layer using Change Data Feed...")
    (spark.readStream
           .option("readChangeFeed", "true")
           # No startingVersion specified - will automatically start from checkpoint
           .table("retail_client_silver")
           .withColumn("gold_data", lit("Delta CDF is Awesome"))
           .withColumn("cdf_processing_time", current_timestamp())  # Track CDF processing time
          .writeStream
            .foreachBatch(upsertToDelta)
            .option("checkpointLocation", raw_data_location+"/stream/checkpoint_clients_gold")
            .option("mergeSchema", "true")  # Enable schema evolution for gold layer
            .trigger(availableNow=True)  # Process only new changes since last checkpoint
          .start()
          .awaitTermination())
    
    print("✅ CDC pipeline completed processing available data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Deployment Options
# MAGIC
# MAGIC **Option 1: Scheduled Databricks Job**
# MAGIC ```python
# MAGIC # Schedule this notebook to run every 5 minutes using Databricks Jobs
# MAGIC # The data generator creates new files every 60 seconds
# MAGIC # Serverless compute will auto-scale and process all available data
# MAGIC trigger_cdc_pipeline()
# MAGIC ```
# MAGIC
# MAGIC **Option 2: Continuous Loop (for demo purposes)**
# MAGIC ```python
# MAGIC # Run continuous processing loop
# MAGIC while generator_running:
# MAGIC     trigger_cdc_pipeline()
# MAGIC     time.sleep(60)  # Process every minute
# MAGIC ```
# MAGIC
# MAGIC **Option 3: Event-Driven Processing**
# MAGIC - Use cloud storage notifications
# MAGIC - Trigger via REST API
# MAGIC - Integrate with orchestration tools (Airflow, etc.)

# COMMAND ----------

# DBTITLE 1,🚀 Step 4: Complete CDC Pipeline Demo
print("🎯 Running one iteration of serverless CDC processing...")
print("💡 In production, schedule this via Databricks Jobs every few minutes")

# Give the data generator time to create some files
print("⏳ Waiting 65 seconds for data generator to create new files...")
time.sleep(65)

# Process any new data
trigger_cdc_pipeline()

# Show results with table growth monitoring
print("\n📊 Monitoring table growth over time...")
print("💡 Watch how serverless compute handles growing data volumes efficiently")

# Function to get table sizes
def get_table_sizes():
    sizes = {}
    try:
        sizes['bronze'] = spark.sql("SELECT COUNT(*) as count FROM clients_cdc").collect()[0]['count']
    except:
        sizes['bronze'] = 0
    try:
        sizes['silver'] = spark.sql("SELECT COUNT(*) as count FROM retail_client_silver").collect()[0]['count']
    except:
        sizes['silver'] = 0
    try:
        sizes['gold'] = spark.sql("SELECT COUNT(*) as count FROM retail_client_gold").collect()[0]['count']
    except:
        sizes['gold'] = 0
    return sizes

# Monitor table growth over multiple iterations
print("🔍 Table Size Growth Monitoring:")
print("=" * 60)

for iteration in range(1, 4):  # Monitor 3 iterations
    print(f"\n📈 Iteration {iteration} - {datetime.now().strftime('%H:%M:%S')}")
    
    # Get current sizes
    sizes = get_table_sizes()
    print(f"🥉 Bronze (Raw CDC): {sizes['bronze']:,} records")
    print(f"🥈 Silver (Materialized): {sizes['silver']:,} records") 
    print(f"🥇 Gold (Enhanced): {sizes['gold']:,} records")
    
    # Calculate growth if not first iteration
    if iteration > 1:
        growth_bronze = sizes['bronze'] - previous_sizes['bronze']
        growth_silver = sizes['silver'] - previous_sizes['silver']
        growth_gold = sizes['gold'] - previous_sizes['gold']
        
        print(f"   📊 Growth: Bronze +{growth_bronze}, Silver +{growth_silver}, Gold +{growth_gold}")
    
    # Show recent records
    print("   🔍 Latest Records:")
    try:
        latest_bronze = spark.sql("""
            SELECT operation, COUNT(*) as count 
            FROM clients_cdc 
            GROUP BY operation 
            ORDER BY operation
        """).collect()
        operations_summary = {row['operation']: row['count'] for row in latest_bronze}
        print(f"   📁 Operations: {operations_summary}")
        
        # Show latest silver records
        latest_silver = spark.sql("""
            SELECT id, name, email 
            FROM retail_client_silver 
            ORDER BY id DESC 
            LIMIT 3
        """).collect()
        if latest_silver:
            print("   📝 Latest Silver Records:")
            for row in latest_silver:
                print(f"      ID: {row['id']}, Name: {row['name']}, Email: {row['email']}")
    except Exception as e:
        print(f"   ⚠️ Error showing details: {e}")
    
    previous_sizes = sizes
    
    # Wait for next iteration (except on last one)
    if iteration < 3:
        print(f"   ⏳ Waiting 65 seconds for more CDC data...")
        print("   💰 Serverless compute: No costs during wait time!")
        time.sleep(65)
        
        # Process new data
        print(f"   🔄 Processing new data (Iteration {iteration + 1})...")
        trigger_cdc_pipeline()

print("\n" + "=" * 60)
print("✅ Table growth monitoring completed!")
print("📈 Key Observations:")
print("   🔹 Tables grow incrementally with each CDC batch")
print("   🔹 Serverless compute scales automatically with data volume")
print("   🔹 Cost efficiency: Pay only during processing, not waiting")
print("   🔹 Real-time CDC processing with delta architecture")

# COMMAND ----------

# DBTITLE 1,📊 Step 5.1: Cleanup and Stop Data Generator
stop_cdc_generator()
DBDemos.stop_all_streams()

print("🎉 Demo completed! You've seen how serverless compute handles continuous CDC processing:")
print("✅ Cost-effective: Pay only for actual processing time")
print("✅ Auto-scaling: Automatically scales based on data volume") 
print("✅ Simplified ops: No cluster management required")
print("✅ Reliable: Built-in fault tolerance and automatic restarts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 6: Data Sharing and Datamesh Organization
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC - 🔄 **Change Tracking**: Track all INSERT/UPDATE/DELETE operations from any Delta table
# MAGIC - 📡 **Incremental Processing**: Subscribe to table modifications as incremental processes
# MAGIC - 🏗️ **Data Mesh Ready**: Each mesh can publish tables, others can subscribe to changes
# MAGIC - 🛡️ **GDPR Compliance**: Propagate changes (e.g., GDPR DELETE) across data meshes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 7: Data Ready for BI & ML Use Cases
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/product/Delta-Lake-CDC-CDF/cdc-flow-4.png" alt='BI and ML Ready' style='float: right' width='600'/>
# MAGIC
# MAGIC ### What's Available:
# MAGIC - 📊 **Business Intelligence**: Create SQL queries and dashboards
# MAGIC - 🤖 **Machine Learning**: Build ML models on clean, up-to-date data
# MAGIC - 🔄 **Real-time Analytics**: Access to latest data changes
# MAGIC - 📈 **Data Quality**: Clean, deduplicated, and validated data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 8: Next Steps
# MAGIC
# MAGIC ### Continue Your CDC Journey:
# MAGIC - 🔗 **[Multi-Table CDC Pipeline]($./02-CDC-CDF-full-multi-tables)**: Scale to multiple tables
# MAGIC - 🏗️ **[Delta Live Tables]($./dlt-cdc)**: Simplified CDC with `APPLY CHANGES`
# MAGIC - 📚 **[Delta Lake Demo]($./delta-lake)**: Deep dive into Delta Lake features
# MAGIC - 🚀 **[Auto Loader Demo]($./auto-loader)**: Advanced file ingestion patterns
# MAGIC
# MAGIC ### Production Deployment:
# MAGIC - 📅 **Schedule Jobs**: Use Databricks Jobs for automated processing
# MAGIC - 📊 **Monitor Performance**: Set up alerts and dashboards
# MAGIC - 🔒 **Security**: Implement proper access controls and data governance
# MAGIC - 💰 **Cost Optimization**: Monitor and optimize serverless compute usage
