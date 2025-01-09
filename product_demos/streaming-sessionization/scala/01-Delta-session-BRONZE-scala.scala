// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC
// MAGIC # Streaming on Databricks with Spark and Delta Lake - Scala version
// MAGIC
// MAGIC **Scala version:** This notebook implement the same logic as [the python]($../01-Delta-session-BRONZE), but using Scala.
// MAGIC
// MAGIC Streaming on Databricks is greatly simplified using Delta Live Table (DLT). <br/>
// MAGIC DLT lets you write your entire data pipeline, supporting streaming transformation using SQL or python and removing all the technical challenges.
// MAGIC
// MAGIC We strongly recommend implementing your pipelines using DLT as this will allow for much robust pipelines, enforcing data quality and greatly accelerating project delivery.<br/>
// MAGIC *For a DLT example, please install `dbdemos.install('dlt-loans')` or the C360 Lakehouse demo: `dbdemos.install('lakehouse-retail-churn')`*
// MAGIC
// MAGIC Spark Streaming API offers lower-level primitive offering more advanced control, such as `foreachBatch` and custom streaming operation with `applyInPandasWithState`.
// MAGIC
// MAGIC Some advanced use-case can be implemented using these APIs, and this is what we'll focus on.
// MAGIC
// MAGIC ## Building a sessionization stream with Delta Lake and Spark Streaming
// MAGIC
// MAGIC ### What's sessionization?
// MAGIC <div style="float:right" ><img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/session_diagram.png" style="height: 200px; margin:0px 0px 0px 10px"/></div>
// MAGIC
// MAGIC Sessionization is the process of finding time-bounded user session from a flow of event, grouping all events happening around the same time (ex: number of clicks, pages most view etc)
// MAGIC
// MAGIC When there is a temporal gap greater than X minute, we decide to split the session in 2 distinct sessions
// MAGIC
// MAGIC ### Why is that important?
// MAGIC
// MAGIC Understanding sessions is critical for a lot of use cases:
// MAGIC
// MAGIC - Detect cart abandonment in your online shot, and automatically trigger marketing actions as follow-up to increase your sales
// MAGIC - Build better attribution model for your affiliation, based on the user actions during each session 
// MAGIC - Understand user journey in your website, and provide better experience to increase your user retention
// MAGIC - ...
// MAGIC
// MAGIC
// MAGIC ### Sessionization with Spark & Delta
// MAGIC
// MAGIC Sessionization can be done in many ways. SQL windowing is often used but quickly become too restricted for complex use-case. 
// MAGIC
// MAGIC Instead, we'll be using the following Delta Architecture:
// MAGIC
// MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/sessionization.png" width="1200px">
// MAGIC
// MAGIC Being able to process and aggregate your sessions in a Batch and Streaming fashion can be a real challenge, especially when updates are required in your historical data!
// MAGIC
// MAGIC Thankfully, Delta and Spark can simplify our job, using Spark Streaming function with a custom stateful operation (`flatMapGroupsWithState` operator), in a streaming and batch fashion.
// MAGIC
// MAGIC Let's build our Session job to detect cart abandonment !
// MAGIC
// MAGIC
// MAGIC *Note: again, this is an advanced demo - if you're starting with Databricks and are looking for a simple streaming pipeline we recommand going with DLT instead.*
// MAGIC
// MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
// MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fstreaming%2Fsessionization%2Fbronze&dt=FEATURE_STREAMING_SESSIONIZATION">

// COMMAND ----------

// MAGIC %run ../_resources/00-setup-scala $reset_all_data=false

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC
// MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Bronze table: store the stream as Delta Lake table
// MAGIC
// MAGIC <img style="float:right; height: 250px; margin: 0px 30px 0px 30px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/sessionization_bronze.png">
// MAGIC
// MAGIC
// MAGIC The first step is to consume data from our streaming engine (Kafka, Kinesis, Pulsar etc.) and save it in our Data Lake.
// MAGIC
// MAGIC We won't be doing any transformation, the goal is to be able to re-process all the data and change/improve the downstream logic when needed
// MAGIC
// MAGIC #### Solving small files and compaction issues
// MAGIC
// MAGIC Everytime we capture kafka events, they'll be stored in our table and this will create new files. After several days, we'll endup with millions of small files leading to performance issues.<br/>
// MAGIC Databricks solves that with autoOptimize & autoCompact, 2 properties to set at the table level.
// MAGIC
// MAGIC *Note that if the table isn't created with all the columns. The engine will automatically add the new column from kafka at write time, merging the schema gracefuly*

// COMMAND ----------

// DBTITLE 1,Create the table events_raw
// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS events_raw (key string, value string) TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

// COMMAND ----------

// DBTITLE 1,Read messages from Kafka and save them as events_raw
//NOTE: the demo runs with Kafka, and dbdemos doesn't publically expose its demo kafka servers. Use your own IPs to run the demo properly
//val kafkaBootstrapServersTLS = "b-1.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9092,b-2.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9092,b-3.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9092"
val kafkaBootstrapServersTLS = "<Replace by your own kafka servers>"

val stream = spark
  .readStream
  // === Configurations for Kafka streams ===
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServersTLS) 
  .option("kafka.security.protocol", "PLAINTEXT") //SSL
  .option("subscribe", "dbdemos-sessions") // kafka topic
  .option("startingOffsets", "latest") // Consume messages from the end
  .option("maxOffsetsPerTrigger", "10000") // Control ingestion rate - backpressure
  .option("ignoreChanges", "true")
  .load()
  .withColumn("key", $"key".cast("string"))
  .withColumn("value", $"value".cast("string"))
  .writeStream
  // === Write to the delta table ===
  .format("delta")
  .trigger(Trigger.ProcessingTime("20 seconds"))
  .option("checkpointLocation", s"$volumeFolder/checkpoints/bronze")
  .option("mergeSchema", "true")
  .outputMode("append")
  .table("events_raw")

waitForTable("events_raw")

// COMMAND ----------

// DBTITLE 1,Our table events_raw is ready and will contain all events
// MAGIC %sql SELECT * FROM events_raw;

// COMMAND ----------

// MAGIC %md
// MAGIC ### Our Raw events are now ready to be analyzed
// MAGIC
// MAGIC It's now easy to run queries in our events_raw table. Our data is saved as JSON, databricks makes it easy to query:

// COMMAND ----------

// DBTITLE 1,Action per platform
// MAGIC %sql
// MAGIC select count(*), value:platform as platform from events_raw group by platform;

// COMMAND ----------

// MAGIC %md
// MAGIC ## Searching for duplicate events
// MAGIC
// MAGIC As you can see, our producer sends incorrect messages.
// MAGIC
// MAGIC Not only we have null event_id from time to time, but we also have duplicate events (identical events being send twice with the same ID and exact same content)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) event_count, value :event_id event_id, first(value) from events_raw
// MAGIC   group by event_id
// MAGIC     having event_count > 1
// MAGIC   order by event_id;

// COMMAND ----------

// DBTITLE 1,Stop all the streams 
stopAllStreams(sleepTime=120)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Next steps: Cleanup data and remove duplicates
// MAGIC
// MAGIC It looks like we have duplicate event in our dataset. Let's see how we can perform some cleanup. 
// MAGIC
// MAGIC In addition, reading from JSON isn't super efficient, and what if our json changes over time ?
// MAGIC
// MAGIC While we can explore the dataset using spark json manipulation, this isn't ideal. For example is the json in our message changes after a few month, our request will fail.
// MAGIC
// MAGIC Futhermore, performances won't be great at scale: because all our data is stored as a unique, we can't leverage data skipping and a columnar format
// MAGIC
// MAGIC That's why we need another table:  **[A Silver Table!]($./02-Delta-session-SILVER-scala)**
