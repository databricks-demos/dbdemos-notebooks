# Databricks notebook source
# MAGIC %md 
# MAGIC # Defining the Production source
# MAGIC
# MAGIC ## Adding an abstraction layer for testability 
# MAGIC
# MAGIC By defining the ingestion source in an external table, we can easily switch from the production source to a test one.
# MAGIC
# MAGIC This lets you easily replace an ingestion from a Kafka server in production by a small csv file in your test. 
# MAGIC
# MAGIC This notebook correspond to the PROD stream (the **green** input source on the left)
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-advanecd/DLT-advanced-unit-test-1.png"/>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=DLT-ingest_prod&demo_name=dlt-unit-test&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Source for customer dataset
# MAGIC
# MAGIC
# MAGIC In prod, we'll be using the autoloader to our prod landing folder. 
# MAGIC
# MAGIC To give more flexibility in our deployment, we'll go further and set the location as a LDP parameter

# COMMAND ----------

# DBTITLE 1,Ingest raw User stream data in incremental mode
import dlt
DEFAULT_LANDING_PATH = "/Volumes/main__build/dbdemos_ldp_unit_test/raw_data/prod"

@dlt.view(comment="Raw user data - Production")
def raw_user_data():
  landing_path = spark.conf.get("mypipeline.landing_path", DEFAULT_LANDING_PATH)
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaHints", "id int")
      .load(f"{landing_path}/users_json")
  )

# COMMAND ----------

# DBTITLE 1,Ingest user spending score
@dlt.view(comment="Raw spend data - Production")
def raw_spend_data():
  landing_path = spark.conf.get("mypipeline.landing_path", DEFAULT_LANDING_PATH)
  return(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaHints","id int, age int, annual_income float, spending_core float")
    .load(f"{landing_path}/spend_csv")
  )
