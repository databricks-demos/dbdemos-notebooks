-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Delta Lake internals
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:200px; float: right"/>
-- MAGIC
-- MAGIC Let's deep dive into Delta Lake internals.
-- MAGIC
-- MAGIC ## Exploring delta structure
-- MAGIC
-- MAGIC Under the hood, Delta is composed of parquet files and a transactional log. Transactional log contains all the metadata operation. Databricks leverage this information to perform efficient data skipping at scale among other things.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Finternal&dt=FEATURE_DELTA">
-- MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- DBTITLE 1,Init the demo data
-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md ### Exploring delta structure
-- MAGIC
-- MAGIC Delta is composed of parquet files and a transactional log

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table('user_delta').write.mode('overwrite').save(f'/Volumes/{catalog}/{schema}/{volume_name}/user_delta_table')

-- COMMAND ----------


DESCRIBE DETAIL `delta`.`/Volumes/main__build/dbdemos_delta_lake/delta_lake_raw_data/user_delta_table`

-- COMMAND ----------

-- DBTITLE 1,Delta is composed of parquet files
-- MAGIC %python
-- MAGIC delta_folder = spark.sql(f"DESCRIBE DETAIL `delta`.`/Volumes/{catalog}/{schema}/{volume_name}/user_delta_table`").collect()[0]['location']
-- MAGIC print(delta_folder)
-- MAGIC display(dbutils.fs.ls(delta_folder))

-- COMMAND ----------

-- DBTITLE 1,And a transactional log
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(delta_folder+"/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC commit_log = dbutils.fs.head(delta_folder+"/_delta_log/00000000000000000000.json", 10000)
-- MAGIC print(json.dumps(json.loads(commit_log.split('\n')[0]), indent = 2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OPTIMIZE in action
-- MAGIC Running an `OPTIMIZE` + `VACUUM` will re-order all our files.
-- MAGIC
-- MAGIC As you can see, we have multiple small parquet files in our folder:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(delta_folder))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's OPTIMIZE our table to see how the engine will compact the table:

-- COMMAND ----------

OPTIMIZE `delta`.`/Volumes/main__build/dbdemos_delta_lake/delta_lake_raw_data/user_delta_table`;
-- as we vacuum with 0 hours, we need to remove the safety check:

-- Note: commented out as this option isn't available on serverless compute for now - see ES-1302674
-- set spark.databricks.delta.retentionDurationCheck.enabled = false;

-- VACUUM `delta`.`/Volumes/main__build/dbdemos_delta_lake/delta_lake_raw_data/user_delta_table` retain 0 hours;

-- COMMAND ----------

-- DBTITLE 1,Only one parquet file remains after the OPTIMIZE+VACUUM operation
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(delta_folder))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it! You know everything about Delta Lake!
-- MAGIC
-- MAGIC As next step, you learn more about Delta Live Table to simplify your ingestion pipeline: `dbdemos.install('delta-live-table')`
-- MAGIC
-- MAGIC Go back to [00-Delta-Lake-Introduction]($./00-Delta-Lake-Introduction).
