# Databricks notebook source
#To reset the data and restart the demo from scratch, switch the widget to True and run the "%run ./_resources/00-setup $reset_all_data=$reset_all_data" cell below.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC # What is Databricks Auto Loader?
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader/autoloader-edited-anim.gif" style="float:right; margin-left: 10px" />
# MAGIC 
# MAGIC [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) lets you scan a cloud storage folder (S3, ADLS, GS) and only ingest the new data that arrived since the previous run.
# MAGIC 
# MAGIC This is called **incremental ingestion**.
# MAGIC 
# MAGIC Auto Loader can be used in a near real-time stream or in a batch fashion, e.g., running every night to ingest daily data.
# MAGIC 
# MAGIC Auto Loader provides a strong gaurantee when used with a Delta sink (the data will only be ingested once).
# MAGIC 
# MAGIC ## How Auto Loader simplifies data ingestion
# MAGIC 
# MAGIC Ingesting data at scale from cloud storage can be really hard at scale. Auto Loader makes it easy, offering these benefits:
# MAGIC 
# MAGIC 
# MAGIC * **Incremental** & **cost-efficient** ingestion (removes unnecessary listing or state handling)
# MAGIC * **Simple** and **resilient** operation: no tuning or manual code required
# MAGIC * Scalable to **billions of files**
# MAGIC   * Using incremental listing (recommended, relies on filename order)
# MAGIC   * Leveraging notification + message queue (when incremental listing can't be used)
# MAGIC * **Schema inference** and **schema evolution** are handled out of the box for most formats (csv, json, avro, images...)
# MAGIC 
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fauto_loader%2Fnotebook&dt=FEATURE_AUTOLOADER">

# COMMAND ----------

# DBTITLE 1,Data initialization - run the cell to prepare the demo data.
# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Let's explore what is being delivered in our bucket: (json)
display(spark.read.text(raw_data_location+'/user_json'))

# COMMAND ----------

# MAGIC %md ### Auto Loader basics
# MAGIC Let's create a new Auto Loader stream that will incrementally ingest new incoming files.
# MAGIC 
# MAGIC In this example we will specify the full schema. We will also use `cloudFiles.maxFilesPerTrigger` to take 1 file a time to simulate a process adding files 1 by 1.

# COMMAND ----------

bronzeDF = (spark.readStream \
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.maxFilesPerTrigger", "1")  #demo only, remove in real stream
                .schema("address string, creation_date string, firstname string, lastname string, id bigint")
                .load(raw_data_location+'/user_json'))
display(bronzeDF)

# COMMAND ----------

# MAGIC %md ## Schema inference
# MAGIC Specifying the schema manually can be a challenge, especially with dynamic JSON. Notice that we are missing the "age" data because we overlooked specifying this column in the schema.
# MAGIC 
# MAGIC * Schema inference has always been expensive and slow at scale, but not with Auto Loader. Auto Loader efficiently samples data to infer the schema and stores it under `cloudFiles.schemaLocation` in your bucket. 
# MAGIC * Additionally, `cloudFiles.inferColumnTypes` will determine the proper data type from your JSON.
# MAGIC 
# MAGIC Let's redefine our stream with these features. Notice that we now have all of the JSON fields.
# MAGIC 
# MAGIC *Notes:*
# MAGIC * *With Delta Live Tables you don't even have to set this option, the engine manages the schema location for you.*
# MAGIC * *Sampling size can be changed with `spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes`*

# COMMAND ----------

# DBTITLE 1,Auto Loader can now infer the schema automatically (from any format) 
bronzeDF = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", raw_data_location+'/inferred_schema')
                .option("cloudFiles.inferColumnTypes", "true")
                .load(raw_data_location+'/user_json'))
display(bronzeDF)

# COMMAND ----------

# MAGIC %md ### Schema hints
# MAGIC You might need to enforce a part of your schema, e.g., to convert a timestamp. This can easily be done with Schema Hints.
# MAGIC 
# MAGIC In this case, we'll make sure that the `id` is read as `bigint` and not `int`:

# COMMAND ----------

bronzeDF = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"{raw_data_location}/inferred_schema")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaHints", "id bigint")
                .load(raw_data_location+'/user_json'))
display(bronzeDF)

# COMMAND ----------

# MAGIC %md ## Schema evolution

# COMMAND ----------

# DBTITLE 1,Schema evolution is now supported by restarting the stream
def get_stream():
  return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"{raw_data_location}/inferred_schema")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaHints", "id bigint")
                .load(raw_data_location+'/user_json'))
display(get_stream())

# COMMAND ----------

# MAGIC %md ### Incorrect schema
# MAGIC Auto Loader automatically recovers from incorrect schema and conflicting type. It'll save incorrect data in the `_rescued_data` column.

# COMMAND ----------

# DBTITLE 1,Adding an incorrect field ("id" as string instead of bigint)
incorrect_data = spark.read.json(sc.parallelize(['{"email":"quentin.ambard@databricks.com", "firstname":"Quentin", "id": "456455", "lastname":"Ambard"}']))
incorrect_data.write.format("json").mode("append").save(raw_data_location+"/user_json")

# COMMAND ----------

wait_for_rescued_data()
# Start the stream and filter on on the rescue column to see how the incorrect data is captured
display(get_stream().filter("_rescued_data is not null"))

# COMMAND ----------

# MAGIC %md ### Adding a new column
# MAGIC By default the stream will tigger a `UnknownFieldException` exception on new column. You then have to restart the stream to include the new column. 
# MAGIC 
# MAGIC Make sure your previous stream is still running and run the next cell.
# MAGIC 
# MAGIC *Notes*:
# MAGIC * *See `cloudFiles.schemaEvolutionMode` for different behaviors and more details.*
# MAGIC * *Don't forget to add `.writeStream.option("mergeSchema", "true")` to dynamically add when columns when writting to a delta table*

# COMMAND ----------

# DBTITLE 1,Adding a row with an extra column ("new_column":"test new column value")
# Stop all the existing streams
stop_all_streams()
# Add 'new_column'
row = '{"email":"quentin.ambard@databricks.com", "firstname":"Quentin", "id":456454, "lastname":"Ambard", "new_column":"test new column value"}'
new_row = spark.read.json(sc.parallelize([row]))
new_row.write.format("json").mode("append").save(raw_data_location+"/user_json")

# COMMAND ----------

# Exsiting stream wil fail with: org.apache.spark.sql.catalyst.util.UnknownFieldException: Encountered unknown field(s) during parsing: {"new_column":"test new column value"}
# UNCOMMENT_FOR_DEMO display(get_stream())

# COMMAND ----------

# We just have to restart it to capture the new data. Let's filter on the new column to make sure we have the proper row 
# (re-run the cell)
# UNCOMMENT_FOR_DEMO display(get_stream().filter('new_column is not null'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Ingesting a high volume of input files
# MAGIC Scanning folders with many files to detect new data is an expensive operation, leading to ingestion challenges and higher cloud storage costs.
# MAGIC 
# MAGIC To solve this issue and support an efficient listing, Databricks autoloader offers two modes:
# MAGIC 
# MAGIC - Incremental listing with `cloudFiles.useIncrementalListing` (recommended), based on the alphabetical order of the file's path to only scan new data: (`ingestion_path/YYYY-MM-DD`)
# MAGIC - Notification system, which sets up a managed cloud notification system sending new file name to a queue (when we can't rely on file name order). See `cloudFiles.useNotifications` for more details.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader-mode.png" width="700"/>
# MAGIC 
# MAGIC Use the incremental listing option whenever possible. Databricks Auto Loader will try to auto-detect and use the incremental approach when possible.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Support for images
# MAGIC Databricks Auto Loader provides native support for images and binary files.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader-images.png" width="800" />
# MAGIC 
# MAGIC Just set the format accordingly and the engine will do the rest: `.option("cloudFiles.format", "binaryFile")`
# MAGIC 
# MAGIC Use-cases:
# MAGIC 
# MAGIC - ETL images into a Delta table using Auto Loader
# MAGIC - Automatically ingest continuously arriving new images
# MAGIC - Easily retrain ML models on new images
# MAGIC - Perform distributed inference using a pandas UDF directly from Delta 

# COMMAND ----------

# MAGIC %md ## Deploying robust ingestion jobs in production
# MAGIC 
# MAGIC Let's see how to use Auto Loader to ingest JSON files, support schema evolution, and automatically restart when a new column is found.
# MAGIC 
# MAGIC If you need your job to be resilient with regard to an evolving schema, you have multiple options:
# MAGIC 
# MAGIC * Let the full job fail & configure Databricks Workflow to restart it automatically
# MAGIC * Leverage Delta Live Tables to simplify all the setup (DLT handles everything for you out of the box)
# MAGIC * Wrap your call to restart the stream when the new column appears.
# MAGIC 
# MAGIC Here is an example:

# COMMAND ----------

# DBTITLE 1,Define helper functions
def start_stream_restart_on_schema_evolution():
  while True:
    try:
      q = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", f"{raw_data_location}/inferred_schema")
                  .option("cloudFiles.inferColumnTypes", "true")
                  .load(raw_data_location+"/user_json")
                .writeStream
                  .format("delta")
                  .option("checkpointLocation", raw_data_location+"/checkpoint")
                  .option("mergeSchema", "true")
                  .table("autoloader_demo_output"))
      q.awaitTermination()
      return q
    except BaseException as e:
      #Adding a new column will trigger an UnknownFieldException. In this case we just restart the stream:
      if not ('UnknownFieldException' in str(e.stackTrace)):
        raise e
        
#start_stream_restart_on_schema_evolution()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC 
# MAGIC We've seen how Databricks Auto Loader can be used to easily ingest your files, solving all ingestion challenges!
# MAGIC 
# MAGIC You're ready to use it in your projects!

# COMMAND ----------

# DBTITLE 1,Stop all active stream
stop_all_streams()
