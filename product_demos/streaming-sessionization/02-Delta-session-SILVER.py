# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)  2/ SILVER table: store the content of our events in a structured table
# MAGIC
# MAGIC <img style="float:right; height: 230px; margin: 0px 30px 0px 30px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/sessionization_silver.png">
# MAGIC
# MAGIC We can create a new silver table containing all our data.
# MAGIC
# MAGIC This will allow to store all our data in a proper table, with the content of the json stored in a columnar format. 
# MAGIC
# MAGIC Should our message content change, we'll be able to adapt the transformation of this job to always allow SQL queries over this SILVER table.
# MAGIC
# MAGIC If we realized our logic was flawed from the begining, it'll also be easy to start a new cluster to re-process the entire table with a better transformation!
# MAGIC
# MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fstreaming%2Fsessionization%2Fsilver&dt=FEATURE_STREAMING_SESSIONIZATION">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Stream and clean the raw events
wait_for_table("events_raw") #Wait until the previous table is created to avoid error if all notebooks are started at once

#For the sake of the example we'll get the schema from a json row. In a real deployment we could query a schema registry.
row_example = """{"user_id": "5ee7ba5f-77b2-47e4-8061-dd89f19626f3", "platform": "other", "event_id": "03c3d410-f01f-4f51-8ee0-7fab9be96855", "event_date": 1669301257, "action": "view", "uri": "https://databricks.com/home.htm"}"""
json_schema = F.schema_of_json(row_example)

stream = (spark
            .readStream
              .table("events_raw")
             # === Our transformation, easy to adapt if our logic changes ===
            .withColumn('json', F.from_json(col("value"), json_schema))
            .select('json.*')
             # Drop null events
             .where("event_id is not null and user_id is not null and event_date is not null")
             .withColumn('event_datetime', F.to_timestamp(F.from_unixtime(col("event_date")))))
display(stream)

# COMMAND ----------

(stream
  .withWatermark('event_datetime', '1 hours')
  .dropDuplicates(['event_id'])
  .writeStream
    .trigger(processingTime="20 seconds")
    .option("checkpointLocation", cloud_storage_path+"/checkpoints/silver")
    .option("mergeSchema", "true")
    .table('events'))

wait_for_table("events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make sure we don't have any duplicate nor null event (they've been filtered out)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) event_count, event_id FROM events
# MAGIC   GROUP BY event_id
# MAGIC     HAVING event_count > 1 or event_id is null
# MAGIC   ORDER BY event_id;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's display a real-time view of our traffic using our stream, grouped by platform, for the last minute

# COMMAND ----------

spark.readStream.table("events").createOrReplaceTempView("events_stream")

# COMMAND ----------

# DBTITLE 1,Let's monitor our events from the last minutes with a window function
# MAGIC %sql
# MAGIC -- Visualization: bar plot with X=start Y=count (SUM, group by platform)
# MAGIC WITH event_monitoring AS
# MAGIC   (SELECT WINDOW(event_datetime, "10 seconds") w, count(*) c, platform FROM events_stream WHERE CAST(event_datetime as INT) > CAST(CURRENT_TIMESTAMP() as INT)-120 GROUP BY w, platform)
# MAGIC SELECT w.*, c, platform FROM event_monitoring 
# MAGIC ORDER BY START DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Let's find our TOP 10 more active pages, updated in real time with a streaming query:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualization: pie with X=URL Y=count (SUM)
# MAGIC select count(*) as count, uri from events_stream group by uri order by count desc limit 10;

# COMMAND ----------

# DBTITLE 1,Stop all the streams 
stop_all_streams(sleep_time=120)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We now have our silver table ready to be used!
# MAGIC
# MAGIC Let's compute our sessions based on this table with  **[a Gold Table](https://demo.cloud.databricks.com/#notebook/4438519)**
# MAGIC
# MAGIC
# MAGIC **[Go Back](https://demo.cloud.databricks.com/#notebook/4128443)**
