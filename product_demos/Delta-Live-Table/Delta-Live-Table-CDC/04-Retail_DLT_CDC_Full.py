# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Implementing a CDC pipeline using DLT for N tables
# MAGIC 
# MAGIC We saw previously how to setup a CDC pipeline for a single table. However, real-life database typically involve multiple tables, with 1 CDC folder per table.
# MAGIC 
# MAGIC Operating and ingesting all these tables at scale is quite challenging. You need to start multiple table ingestion at the same time, working with threads, handling errors, restart where you stopped, deal with merge manually.
# MAGIC 
# MAGIC Thankfully, DLT takes care of that for you. We can leverage python loops to naturally iterate over the folders (see the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-cookbook.html#programmatically-manage-and-create-multiple-live-tables) for more details)
# MAGIC 
# MAGIC DLT engine will handle the parallelization whenever possible, and autoscale based on your data volume.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt_pipeline_full.png" width="1000"/>
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_cdc%2Fnotebook_dlt_full&dt=DLT_CDC">
# MAGIC <!-- [metadata={"description":"Process CDC from external system and save them as a Delta Table. BRONZE/SILVER.<br/><i>Usage: demo CDC flow.</i>",
# MAGIC  "authors":["mojgan.mazouchi@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into", "cdc", "cdf"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,2 tables in our cdc_raw: customers and transactions
# MAGIC %fs ls /tmp/demo/cdc_raw

# COMMAND ----------

#Let's loop over all the folders and dynamically generate our DLT pipeline. 
import dlt
from pyspark.sql.functions import *
  
  
def create_pipeline(table_name):
  print(f"Building DLT CDC pipeline for {table_name}")
  
  ##Raw CDC Table
  #        .option("cloudFiles.maxFilesPerTrigger", "1")
  @dlt.create_table(name=table_name+"_cdc",
                    comment = "New "+table_name+" data incrementally ingested from cloud object storage landing zone")
  def raw_cdc():
    return (
      spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/demos/dlt/cdc_raw/"+table_name))
  
  ##Clean CDC input and track quality with expectations
  @dlt.create_view(name=table_name+"_cdc_clean",
                  comment="Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type")
  @dlt.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
  @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
  @dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'DELETE', 'UPDATE')")
  def raw_cdc_clean():
    return dlt.read_stream(table_name+"_cdc")
  
  
  ##Materialize the final table
  dlt.create_target_table(name=table_name, comment="Clean, materialized "+table_name)
  dlt.apply_changes(target = table_name, #The customer table being materilized
                    source = table_name+"_cdc_clean", #the incoming CDC
                    keys = ["id"], #what we'll be using to match the rows to upsert
                    sequence_by = col("operation_date"), #we deduplicate by operation date getting the most recent value
                    ignore_null_updates = False,
                    apply_as_deletes = expr("operation = 'DELETE'"), #DELETE condition
                    except_column_list = ["operation", "operation_date", "_rescued_data"]) #in addition we drop metadata columns
  
  
for folder in dbutils.fs.ls("/demos/dlt/cdc_raw"):
  table_name = folder.name[:-1]
  create_pipeline(table_name)

# COMMAND ----------

# DBTITLE 1,Add final layer joining 2 tables
@dlt.create_table(name="transactions_per_customers",
                  comment = "table join between users and transactions for further analysis")
def raw_cdc():
  return dlt.read("transactions").join(dlt.read("customers"), ["id"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion 
# MAGIC We can now scale our CDC pipeline to N tables using python factorization. This gives us infinite possibilities and abstraction level in our DLT pipelines.
# MAGIC 
# MAGIC DLT handles all the hard work for us so that we can focus on business transformation and drastically accelerate DE team:
# MAGIC - simplify file ingestion with the autoloader
# MAGIC - track data quality using exception
# MAGIC - simplify all operations including upsert with APPLY CHANGES
# MAGIC - process all our tables in parallel
# MAGIC - autoscale based on the amount of data
# MAGIC 
# MAGIC DLT gives more power to SQL-only users, letting them build advanced data pipeline without requiering strong Data Engineers skills.
