# Databricks notebook source
dbdemos_iot_platformdbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Ingesting and transforming IOT sensors from Wind Turbinge using Delta Lake and Spark API
# MAGIC
# MAGIC <img style="float: right" width="300px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-2.png" />
# MAGIC
# MAGIC In this notebook, we'll show you an alternative to Delta Live Table: building an ingestion pipeline with the Spark API.
# MAGIC
# MAGIC As you'll see, this implementation is lower level than the Delta Live Table pipeline, and you'll have control over all the implementation details (handling checkpoints, data quality etc).
# MAGIC
# MAGIC Lower level also means more power. Using Spark API, you'll have unlimited capabilities to ingest data in Batch or Streaming.
# MAGIC
# MAGIC If you're unsure what to use, start with Delta Live Table!
# MAGIC
# MAGIC *Remember that Databricks workflow can be used to orchestrate a mix of Delta Live Table pipeline with standard Spark pipeline.*
# MAGIC
# MAGIC ### Dataset:
# MAGIC
# MAGIC As reminder, we have multiple data sources coming from different system:
# MAGIC
# MAGIC * <strong>Turbine metadata</strong>: Turbine ID, location (1 row per turbine)
# MAGIC * <strong>Turbine sensor stream</strong>: Realtime streaming flow from wind turbine sensor (vibration, energy produced, speed etc)
# MAGIC * <strong>Turbine status</strong>: Historical turbine status based to analyse which part is faulty (used as label in our ML model)
# MAGIC
# MAGIC
# MAGIC Leveraging Spark and Delta Lake makes such an implementation easy.
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.5-Delta-pipeline-spark-iot-turbine&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %pip install mlflow==2.17.2

# COMMAND ----------

# MAGIC %run ../../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Building a Spark Data pipeline with Delta Lake
# MAGIC
# MAGIC In this example, we'll implement a end 2 end pipeline consuming our IOT sources. We'll use the medaillon architecture but could build a star schema, data vault or any other modelisation.
# MAGIC
# MAGIC
# MAGIC
# MAGIC This can be challenging with traditional systems due to the following:
# MAGIC  * Data quality issue
# MAGIC  * Running concurrent operation
# MAGIC  * Running DELETE/UPDATE/MERGE over files
# MAGIC  * Governance & schema evolution
# MAGIC  * Performance ingesting millions of small files on cloud buckets
# MAGIC  * Processing & analysing unstructured data (image, video...)
# MAGIC  * Switching between batch or streaming depending of your requirement...
# MAGIC
# MAGIC ## Solving these challenges with Delta Lake
# MAGIC
# MAGIC <div style="float:left">
# MAGIC
# MAGIC **What's Delta Lake? It's a new OSS standard to bring SQL Transactional database capabilities on top of parquet files!**
# MAGIC
# MAGIC Used as a new Spark format, built on top of Spark API / SQL
# MAGIC
# MAGIC * **ACID transactions** (Multiple writers can simultaneously modify a data set)
# MAGIC * **Full DML support** (UPDATE/DELETE/MERGE)
# MAGIC * **BATCH and STREAMING** support
# MAGIC * **Data quality** (expectatiosn, Schema Enforcement, Inference and Evolution)
# MAGIC * **TIME TRAVEL** (Look back on how data looked like in the past)
# MAGIC * **Performance boost** with ZOrder, data skipping and Caching, solves small files issue 
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="height: 200px"/>
# MAGIC
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our predictive maintenance forecast.
# MAGIC
# MAGIC This information will then be used to build our DBSQL dashboard to analyse current turbine farm and impact on stock.
# MAGIC
# MAGIC Let'simplement the following flow: 
# MAGIC  
# MAGIC <div><img width="1100px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-full.png"/></div>
# MAGIC
# MAGIC *Note that we're including the ML model our [Data Scientist built](TODO) using Databricks AutoML to predict the churn.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Explore the dataset
# MAGIC
# MAGIC Let's review the files being received

# COMMAND ----------

# MAGIC %sql LIST '/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/incoming_data'

# COMMAND ----------

# DBTITLE 1,Review the raw sensor data received as JSON
# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/incoming_data`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="700px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-1.png"/>
# MAGIC </div>
# MAGIC   
# MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC
# MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
# MAGIC
# MAGIC Let's use it to create our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/retail/churn/...`. 

# COMMAND ----------

# DBTITLE 1,We'll store the raw data in a USER_BRONZE DELTA table, supporting schema evolution and incorrect data
# MAGIC %sql
# MAGIC -- Note: tables are automatically created during  .writeStream.table("sensor_bronze") operation, but we can also use plain SQL to create them:
# MAGIC CREATE TABLE IF NOT EXISTS spark_sensor_bronze (
# MAGIC   energy   DOUBLE,
# MAGIC   sensor_A DOUBLE,
# MAGIC   sensor_B DOUBLE,
# MAGIC   sensor_C DOUBLE,
# MAGIC   sensor_D DOUBLE,
# MAGIC   sensor_E DOUBLE,
# MAGIC   sensor_F DOUBLE,
# MAGIC   timestamp LONG,
# MAGIC   turbine_id STRING     
# MAGIC   ) using delta 
# MAGIC     CLUSTER BY (turbine_id) -- Requests by turbine ID will be faster, Databricks manage the file layout for you out of the box. 
# MAGIC     TBLPROPERTIES (
# MAGIC      delta.autooptimize.optimizewrite = TRUE,
# MAGIC      delta.autooptimize.autocompact   = TRUE ); 
# MAGIC -- With these 2 last options, Databricks engine will solve small files & optimize write out of the box!

# COMMAND ----------

volume_folder = f'/Volumes/{catalog}/{db}/{volume_name}'
def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                              .format("cloudFiles")
                              .option("cloudFiles.format", data_format)
                              .option("cloudFiles.inferColumnTypes", "true")
                              .option("cloudFiles.schemaLocation", f"{volume_folder}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                              .load(folder))

  return (bronze_products.writeStream
                    .option("checkpointLocation", f"{volume_folder}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
                    .option("mergeSchema", "true") #merge any new column dynamically
                    .trigger(availableNow= True) #Remove for real time streaming
                    .table("spark_"+table)) #Table will be created if we haven't specified the schema first
  
ingest_folder(f'{volume_folder}/historical_turbine_status', 'json', 'historical_turbine_status')
ingest_folder(f'{volume_folder}/turbine', 'json', 'turbine')
ingest_folder(f'{volume_folder}/incoming_data', 'parquet', 'sensor_bronze').awaitTermination()

# COMMAND ----------

# DBTITLE 1,Our user_bronze Delta table is now ready for efficient query
# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it'll be stored here
# MAGIC select * from spark_sensor_bronze;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it'll be stored here
# MAGIC select * from spark_turbine;

# COMMAND ----------

# DBTITLE 1,Quick data exploration leveraging pandas on spark (Koalas): sensor from our first turbine
#Let's explore a bit our datasets with pandas on spark.
first_turbine = spark.table('spark_sensor_bronze').limit(1).collect()[0]['turbine_id']
df = spark.table('spark_sensor_bronze').where(f"turbine_id == '{first_turbine}' ").orderBy('timestamp').pandas_api()
df.plot(x="timestamp", y=["sensor_F", "sensor_E"], kind="line")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Silver data: date cleaned
# MAGIC
# MAGIC <img width="700px" style="float:right" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-2.png"/>
# MAGIC
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

import pyspark.sql.functions as F
#Compute std and percentil of our timeserie per hour
sensors = [c for c in spark.read.table("spark_sensor_bronze").columns if "sensor" in c]
aggregations = [F.avg("energy").alias("avg_energy")]
for sensor in sensors:
  aggregations.append(F.stddev_pop(sensor).alias("std_"+sensor))
  aggregations.append(F.percentile_approx(sensor, [0.1, 0.3, 0.6, 0.8, 0.95]).alias("percentiles_"+sensor))
  
df = (spark.table("spark_sensor_bronze")
          .withColumn("hourly_timestamp", F.date_trunc("hour", F.from_unixtime("timestamp")))
          .groupBy('hourly_timestamp', 'turbine_id').agg(*aggregations))

df.write.mode('overwrite').saveAsTable("spark_sensor_hourly")
display(spark.table("spark_sensor_hourly"))
#Note: a more scalable solution would be to switch to streaming API and compute the aggregation with a ~3hours watermark and MERGE (upserting) the final output. For this demo clarity we we'll go with a full table update instead.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Build our training dataset
# MAGIC
# MAGIC <img width="700px" style="float:right" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-3.png"/>
# MAGIC
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

turbine = spark.table("spark_turbine")
health = spark.table("spark_historical_turbine_status")
(spark.table("spark_sensor_hourly")
  .join(turbine, ['turbine_id']).drop("row", "_rescued_data")
  .join(health, ['turbine_id'])
  .drop("_rescued_data")
  .write.mode('overwrite').saveAsTable("spark_turbine_training_dataset"))

display(spark.table("spark_turbine_training_dataset"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 4/ Call the ML model and get realtime turbine metrics
# MAGIC
# MAGIC <img width="700px" style="float:right" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-4.png"/>
# MAGIC
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

# DBTITLE 1,Load the ML model
#Note: ideally we should download and install the model libraries with the model requirements.txt and PIP. See 04.3-running-inference for an example
import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                              Stage/version  
#                                                                 Model name         |        
#                                                                     |              |        
predict_maintenance = mlflow.pyfunc.spark_udf(spark, "models:/main__build.dbdemos_iot_platform.dbdemos_turbine_maintenance@prod", "string") #, env_manager='virtualenv'
columns = predict_maintenance.metadata.get_input_schema().input_names()

# COMMAND ----------

w = Window.partitionBy("turbine_id").orderBy(col("hourly_timestamp").desc())
(spark.table("spark_sensor_hourly")
  .withColumn("row", F.row_number().over(w))
  .filter(col("row") == 1)
  .join(spark.table('spark_turbine'), ['turbine_id']).drop("row", "_rescued_data")
  .withColumn("prediction", predict_maintenance(*columns))
  .write.mode('overwrite').saveAsTable("spark_current_turbine_metrics"))

# COMMAND ----------

# MAGIC %sql select * from spark_current_turbine_metrics

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Simplify your operations with transactional DELETE/UPDATE/MERGE operations
# MAGIC
# MAGIC Traditional Data Lake struggle to run these simple DML operations. Using Databricks and Delta Lake, your data is stored on your blob storage with transactional capabilities. You can issue DML operation on Petabyte of data without having to worry about concurrent operations.

# COMMAND ----------

# DBTITLE 1,We just realised we have to delete bad entry for a specific turbine
spark.sql("DELETE FROM spark_sensor_bronze where turbine_id='"+first_turbine+"'")

# COMMAND ----------

# DBTITLE 1,Delta Lake keeps history of the table operation
# MAGIC %sql describe history spark_sensor_bronze;

# COMMAND ----------

# DBTITLE 1,We can leverage the history to go back in time, restore or clone a table and enable CDC...
# MAGIC %sql 
# MAGIC  --also works with AS OF TIMESTAMP "yyyy-MM-dd HH:mm:ss"
# MAGIC select * from spark_sensor_bronze version as of 1 ;
# MAGIC
# MAGIC -- You made the DELETE by mistake ? You can easily restore the table at a given version / date:
# MAGIC -- RESTORE TABLE spark_sensor_bronze TO VERSION AS OF 1
# MAGIC
# MAGIC -- Or clone it (SHALLOW provides zero copy clone):
# MAGIC -- CREATE TABLE spark_sensor_bronze_clone SHALLOW|DEEP CLONE sensor_bronze VERSION AS OF 1
# MAGIC
# MAGIC -- Turn on CDC to capture insert/update/delete operation:
# MAGIC -- ALTER TABLE spark_sensor_bronze SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# DBTITLE 1,Make sure all our tables are optimized
# MAGIC %sql
# MAGIC --Note: can be turned on by default or for all the database
# MAGIC ALTER TABLE spark_turbine                  SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );
# MAGIC ALTER TABLE spark_current_turbine_metrics  SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );
# MAGIC ALTER TABLE spark_sensor_bronze            SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );
# MAGIC ALTER TABLE spark_current_turbine_metrics  SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Our finale tables are now ready to be used to build SQL Dashboards and ML models for predictive maintenance!
# MAGIC <img style="float: right" width="400" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-1.png"/>
# MAGIC
# MAGIC Switch to Databricks SQL to see how this data can easily be requested with the [Turbine DBSQL Dashboard](/sql/dashboards/a6bb11d9-1024-47df-918d-f47edc92d5f4) to start reviewing our Wind Turbine stats or the [DBSQL Predictive maintenance Dashboard](/sql/dashboards/d966eb63-6d37-4762-b90f-d3a2b51b9ba8).
# MAGIC
# MAGIC Creating a single flow was simple.  However, handling many data pipeline at scale can become a real challenge:
# MAGIC * Hard to build and maintain table dependencies 
# MAGIC * Difficult to monitor & enforce advance data quality
# MAGIC * Impossible to trace data lineage
# MAGIC * Difficult pipeline operations (observability, error recovery)
# MAGIC
# MAGIC
# MAGIC #### To solve these challenges, Databricks introduced **Delta Live Table**
# MAGIC A simple way to build and manage data pipelines for fresh, high quality data!

# COMMAND ----------

# MAGIC %md
# MAGIC # Next: secure and share data with Unity Catalog
# MAGIC
# MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
# MAGIC
# MAGIC Jump to the [Governance with Unity Catalog notebook]($../../02-Data-governance/02-UC-data-governance-security-iot-turbine) or [Go back to the introduction]($../../00-IOT-wind-turbine-introduction-lakehouse)
