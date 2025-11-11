# Databricks notebook source
# MAGIC %md 
# MAGIC # Defining the Test source
# MAGIC
# MAGIC ## Adding an abstraction layer for testability 
# MAGIC
# MAGIC By defining the ingestion source in an external table, we can easily switch from the production source to a test one.
# MAGIC
# MAGIC This lets you easily replace an ingestion from a Kafka server in production by a small csv file in your test. 
# MAGIC
# MAGIC This notebook correspond to the TEST stream (the **blue** input source on the left)
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-advanecd/DLT-advanced-unit-test-1.png"/>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=DLT-ingest_test&demo_name=dlt-unit-test&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Source for customer dataset
# MAGIC
# MAGIC
# MAGIC This notebook will be used in test only. We'll generate a fixed test dataset and use this test data for our unit tests.
# MAGIC
# MAGIC Note that we'll have to run the test pipeline with a full refresh to reconsume all the data.

# COMMAND ----------

# DBTITLE 1,Ingest raw User stream data in incremental mode
from pyspark import pipelines as dp

spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")

@dp.materialized_view(comment="Raw user data - Test")
def raw_user_data():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaHints", "id int")
      .load(f"/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/users_json/*.json"))

# COMMAND ----------

# DBTITLE 1,Ingest user spending score
from pyspark import pipelines as dp

@dp.materialized_view(comment="Raw spend data - Test")
def raw_spend_data():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaHints", "id int, age int, annual_income float, spending_core float")
    .load(f"/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/spend_csv/*.csv"))
