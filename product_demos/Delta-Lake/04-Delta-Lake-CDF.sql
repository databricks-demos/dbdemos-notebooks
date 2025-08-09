-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Delta Lake Change Data Feed
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:200px; float: right"/>
-- MAGIC
-- MAGIC Delta Lake is an open format and can be read using multiple engine or with standalone libraries (java, python, rust)...
-- MAGIC
-- MAGIC It's then easy to subscribe to modifications stream on one of your table to propagage the changes downstream in a medaillon architecture.
-- MAGIC
-- MAGIC See the [documentation](https://docs.databricks.com/delta/delta-change-data-feed.html) for more details.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Fcdf&dt=FEATURE_DELTA">
-- MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- DBTITLE 1,Init the demo data
-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## CDF for Data Mesh & Delta Sharing
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
-- MAGIC
-- MAGIC When sharing data within a Datamesh and/or to external organization with Delta Sharing, you not only need to share existing data, but also all modifications, so that your consumer can capture apply the same changes.
-- MAGIC
-- MAGIC CDF makes **Data Mesh** implementation easier. Once enabled by an organisation, data can be shared with other. It's then easy to subscribe to the modification stream and propagage GDPR DELETE downstream.
-- MAGIC
-- MAGIC To do so, we need to make sure the CDF are enabled at the table level. Once enabled, it'll capture all the table modifications using the `table_changes` function.
-- MAGIC
-- MAGIC For more details, visit the [CDF documentation](https://docs.databricks.com/delta/delta-change-data-feed.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Try Out CDF :
-- MAGIC - Enable CDF
-- MAGIC - Do Sample changes
-- MAGIC - Query Delta History to ensure changes went through
-- MAGIC - Query older version
-- MAGIC - Check for CDF tracking columns

-- COMMAND ----------

-- DBTITLE 1,Enable CDF at the table level
ALTER TABLE user_delta SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md #### Delta CDF table_changes output
-- MAGIC In addition to the row details, `table_changes` provides back 4 cdc types in the "_change_type" column:
-- MAGIC
-- MAGIC | CDC Type             | Description                                                               |
-- MAGIC |----------------------|---------------------------------------------------------------------------|
-- MAGIC | **update_preimage**  | Content of the row before an update                                       |
-- MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
-- MAGIC | **delete**           | Content of a row that has been deleted                                    |
-- MAGIC | **insert**           | Content of a new row that has been inserted                               |
-- MAGIC
-- MAGIC Let's query the changes of the Delta Version 12 which should be our MERGE operation (you can run a `DESCRIBE HISTORY user_data_bronze` to see the version numbers).
-- MAGIC
-- MAGIC As you can see 1 row has been UPDATED (we get the old and new value), 1 DELETED and one INSERTED.

-- COMMAND ----------

-- DBTITLE 1,Let's make sure we have some changes in our table
-- Make sure you run the first notebook to load all the data.
UPDATE user_delta SET firstname = 'John' WHERE ID < 10;
DELETE FROM user_delta WHERE ID > 1000;

-- COMMAND ----------

-- DBTITLE 1,Use below command to get history of all operations on this table
DESCRIBE HISTORY user_delta;

-- COMMAND ----------

-- DBTITLE 1,Use below command to get the table as of the version number
select * from table_changes("user_delta", 118);

-- COMMAND ----------

-- DBTITLE 1,Below Query will tell us what are the distinct changes that happened in the version
select distinct(_change_type) from table_changes("user_delta", 118)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using CDF to capture incremental change (stream)
-- MAGIC
-- MAGIC To capture the last changes from your table, you can leverage Spark Streaming API. 
-- MAGIC
-- MAGIC It's then easy to subscribe to modifications stream on one of your table to propagage GDPR DELETE downstream

-- COMMAND ----------

-- MAGIC %python
-- MAGIC stream = spark.readStream.format("delta") \
-- MAGIC               .option("readChangeFeed", "true") \
-- MAGIC               .option("startingVersion", 118) \
-- MAGIC               .table("user_delta")
-- MAGIC
-- MAGIC
-- MAGIC display(stream.select("_change_type", "_commit_version", "_commit_timestamp", "id", "firstname", "email"), checkpointLocation = get_chkp_folder(folder))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using CDF for CDC on source table, and MERGE incrementally on target
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Create Target Table (No CDF required)
CREATE table user_delta_silver as select * from user_delta

-- COMMAND ----------

select * from user_delta_silver

-- COMMAND ----------

-- DBTITLE 1,Sample change in source table user_delta
-- Make sure you run the first notebook to load all the data.
UPDATE user_delta SET firstname = 'John_cdc' WHERE ID < 10;

-- COMMAND ----------

-- DBTITLE 1,Making sure change went through in user_delta
select * from user_delta WHERE ID < 10;

-- COMMAND ----------

-- DBTITLE 1,Query delta history to get correct version for CDF
describe history user_delta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cdf_changes = spark.read.format("delta") \
-- MAGIC   .option("readChangeData", "true") \
-- MAGIC   .option("startingVersion", 121) \
-- MAGIC   .table("user_delta")
-- MAGIC
-- MAGIC cdf_changes.createOrReplaceTempView("source_cdf_changes")
-- MAGIC display(cdf_changes)

-- COMMAND ----------

-- DBTITLE 1,Use MERGE to Apply CDC to Target Table
MERGE INTO user_delta_silver AS target
USING (
  SELECT * FROM source_cdf_changes
  WHERE _change_type IN ('update_postimage', 'insert')
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- DBTITLE 1,Ensuring Merge applied CDC to target
select * from user_delta_silver WHERE ID < 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC time.sleep(30)
-- MAGIC DBDemos.stop_all_streams()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Easier CDF with Delta Live Table APPLY CHANGES
-- MAGIC
-- MAGIC Delta Lake CDF is a low level API. To implement simple CDC pipeline using pure SQL (including SCDT2 tables), you can leverage the Delta Live Table engine! See the [documentation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html) for more details.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it, we covered the main capabilities provided by Delta Lake.
-- MAGIC
-- MAGIC If you want to know more about the technical implementation, you can have a look to the [internal structure of Delta Lake]($./05-Advanced-Delta-Lake-Internal) (optional) or [go back to the Introduction]($./00-Delta-Lake-Introduction).
