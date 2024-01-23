-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Testing our DLT pipeline
-- MAGIC 
-- MAGIC Tests can be added directly as expectation within DLT.
-- MAGIC 
-- MAGIC This is typically done using a companion notebook and creating a test version of the DLT pipeline.
-- MAGIC 
-- MAGIC The test DLT pipeline will consume a small test datasets that we'll use to perform cheks on the output: given a specific input, we test the transformation logic by ensuring the output is correct, adding wrong data as input to cover all cases.
-- MAGIC 
-- MAGIC By leveraging expectations, we can simply run a test DLT pipeline. If the pipeline fail, this means that our tests are failing and something is incorrect.
-- MAGIC 
-- MAGIC <img style="float: right" width="1000px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-advanecd/DLT-advanced-unit-test-3.png"/>
-- MAGIC 
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_unit_test%2Fnotebook_test&dt=DLT_UNIT_TEST">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Testing incorrect schema ingestion
-- MAGIC 
-- MAGIC The first thing we'd like to test is that our pipeline is robust and will discard incorrect rows.
-- MAGIC 
-- MAGIC As example, this line from our test dataset should be discarded and flagged as incorrect:
-- MAGIC ```
-- MAGIC {"id":"invalid ID", "email":"margaret84@example.com", ....}
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Let's make sure incorrect input rows (bad schema) are dropped
CREATE TEMPORARY LIVE TABLE TEST_user_bronze_dlt (
  CONSTRAINT incorrect_data_removed EXPECT (not_empty_rescued_data = 0) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: bronze table properly drops row with incorrect schema"
AS SELECT count(*) as not_empty_rescued_data from live.user_bronze_dlt  where _rescued_data is not null or email='margaret84@example.com'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Let's continue our tests on the silver table with multiple checks at once
-- MAGIC 
-- MAGIC We'll next ensure that our silver table transformation does the following:
-- MAGIC 
-- MAGIC * null ids are removed (our test dataset contains null)
-- MAGIC * we should have 4 rows as output (based on the input)
-- MAGIC * the emails are properly anonymized

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_user_silver_dlt_anonymize (
  CONSTRAINT keep_all_rows              EXPECT (num_rows = 4)      ON VIOLATION FAIL UPDATE, 
  CONSTRAINT email_should_be_anonymized EXPECT (clear_email = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT null_ids_removed           EXPECT (null_id_count = 0) ON VIOLATION FAIL UPDATE  
)
COMMENT "TEST: check silver table removes null ids and anonymize emails"
AS (
  WITH
   rows_test  AS (SELECT count(*) AS num_rows       FROM live.user_silver_dlt),
   email_test AS (SELECT count(*) AS clear_email    FROM live.user_silver_dlt  WHERE email LIKE '%@%'),
   id_test    AS (SELECT count(*) AS null_id_count  FROM live.user_silver_dlt  WHERE id IS NULL)
  SELECT * from email_test, id_test, rows_test)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Testing Primary key uniqueness
-- MAGIC 
-- MAGIC Finally, we'll enforce uniqueness on the gold table to avoid any duplicates

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_user_gold_dlt (
  CONSTRAINT pk_must_be_unique EXPECT (duplicate = 1) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: check that gold table only contains unique customer id"
AS SELECT count(*) as duplicate, id FROM live.user_gold_dlt GROUP BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it. All we have to do now is run the full pipeline.
-- MAGIC 
-- MAGIC If one of the condition defined in the TEST table fail, the test pipeline expectation will fail and we'll know something need to be fixed!
-- MAGIC 
-- MAGIC You can open the <a dbdemos-pipeline-id="dlt-test" href="#joblist/pipelines/cade4f82-4003-457c-9f7c-a8e5559873b6">Delta Live Table Pipeline for unit-test</a> to see the tests in action
