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
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_unit_test%2Fnotebook_ingestion_test&dt=DLT_UNIT_TEST">

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
import dlt
spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")
@dlt.view(comment="Raw user data - Test")
def raw_user_data():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaHints", "id int")
      .load(f"/demos/retail/customers/test/users_json/*.json"))

# COMMAND ----------

# DBTITLE 1,Ingest user spending score
@dlt.view(comment="Raw spend data - Test")
def raw_spend_data():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaHints", "id int, age int, annual_income float, spending_core float")
    .load(f"/demos/retail/customers/test/spend_csv/*.csv"))
