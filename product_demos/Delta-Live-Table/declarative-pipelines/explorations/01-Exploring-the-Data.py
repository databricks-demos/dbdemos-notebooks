# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring the raw data
# MAGIC
# MAGIC For this demo, we've generated some data and stored it in a Unity Catalog Volume. Volumes can store any type of file and can either be managed by Unity Catalog or connected to cloud storage. Lakeflow Declaritive Pipelines can automatically pick up new files and incrementally process data in a volume making your pipelines fast and efficient.
# MAGIC
# MAGIC Let's start by taking a look at the contents of the `raw_data` volume.
# MAGIC
# MAGIC **Note: this notebook is a simple Exploration Notebook, it's not part of our final Pipeline!**
# MAGIC
# MAGIC Having a notebook on the side to test SQL queries interactively can be very handy to accelerate exploration and build your pipelines faster!
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=01-Exploring-the-Data&demo_name=declarative-pipelines&event=VIEW">

# COMMAND ----------

import os

raw_data_volume = "/Volumes/main__build/dbdemos_pipeline_bike/raw_data/"

# Print out a list of directories in our raw_data volume and a few files from those directories
for table in os.listdir(raw_data_volume):
  print(table + "/")
  for file in os.listdir(raw_data_volume + table)[:3]:
    print("  " + file)
  print("  ...")


# COMMAND ----------

# MAGIC %md
# MAGIC It looks like we've got a few directories here with `csv` and `json` files in them. Let's start by taking a look at the maintenance logs files using the SQL `read_files` function.
# MAGIC
# MAGIC `read_files` supports several different file formats including `csv` and `json`. Take a look at the [Databricks documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) to see the available formats and options.
# MAGIC
# MAGIC Additionally, using the `STREAM` keyword `read_files` can be used in streaming tables to ingest files into Delta Lake. `read_files` leverages Auto Loader when used in a streaming table query.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/main__build/dbdemos_pipeline_bike/raw_data/maintenance_logs/*.csv", format => "csv") limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC These files contains the field `issue_description` which is a free text field people can use to enter in a description of the issue they ran into while using a bike. Free text fields often include character sequences that may break CSV parsers. Let's do some data exploration on this data to see if we are processing it correctly.
# MAGIC
# MAGIC Based on our knowledge of the system giving us this data, all the fields are required. Let's look at records where that's not the case.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Yup, it looks like there's some instances where the `issue_description` fields include a newline character. Let's tell `read_files` that records may span multiple lines and see if that fixes the issue. 

# COMMAND ----------

from pyspark.sql.functions import expr
# note - in python for now to avoid temp FAILED_READ_FILE.NO_HINT issue
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("multiLine", "true") \
    .load("/Volumes/main__build/dbdemos_pipeline_bike/raw_data/maintenance_logs/*.csv")

filtered_df = df.filter(expr("maintenance_id IS NULL OR bike_id IS NULL OR reported_time IS NULL OR resolved_time IS NULL"))

print(filtered_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's do some quick spot checks on the `ride_logs` and `weather` files. The files in the `weather` directory are `json` files, so we need to make sure to use the `json` format option in `read_files`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/main__build/dbdemos_pipeline_bike/raw_data/rides/*.csv", format => "csv") limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/main__build/dbdemos_pipeline_bike/raw_data/weather/*.json", format => "json") limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files("/Volumes/main__build/dbdemos_pipeline_bike/raw_data/customers_cdc/*.parquet", format => "parquet") limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Understanding Change Data Capture (CDC) with AUTO CDC
# MAGIC
# MAGIC AUTO CDC is a declarative API in Lakeflow Declarative Pipelines that simplifies change data capture processing. 
# MAGIC
# MAGIC Key benefits of AUTO CDC:
# MAGIC - **Automatic ordering**: Handles records that arrive out of chronological order
# MAGIC - **Built-in SCD support**: Easily implement Type 1 or Type 2 slowly changing dimensions
# MAGIC - **Declarative syntax**: Simple SQL-based configuration without complex merge logic
# MAGIC - **Operation handling**: Supports INSERT, UPDATE, DELETE, and TRUNCATE operations
# MAGIC
# MAGIC Let's explore the distribution of CDC operations to understand the types of changes happening to customer data:

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   operation,
# MAGIC   count(*) as count,
# MAGIC   round(count(*) * 100.0 / sum(count(*)) over(), 1) as percentage
# MAGIC from read_files("/Volumes/main__build/dbdemos_pipeline_bike/raw_data/customers_cdc/*.parquet", format => "parquet") 
# MAGIC group by operation
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Building our Declarative Pipeline
# MAGIC We now have a good idea of our raw data and the queries we'll have to do!
# MAGIC
# MAGIC It's time to start building our pipeline!
# MAGIC
# MAGIC If you want to know more about Streaming Tables and Materialized views, open the [00-pipeline-tutorial notebook]($../transformations/00-pipeline-tutorial).
# MAGIC
# MAGIC If you know what you're doing, feel free to jump to the [01-bronze.sql]($../transformations/01-bronze.sql), [02-silver.sql]($../transformations/02-silver.sql) or [03-gold.sql]($../transformations/03-gold.sql) file!
# MAGIC
# MAGIC ### Alternative: learn how to track your Declarative Pipeline data quality
# MAGIC
# MAGIC Lakeflow Declarative Pipelines makes it easy to track your data quality and set alerts when something is wrong! Open the [02-Pipeline-event-monitoring]($./02-Pipeline-event-monitoring) notebook for more details.
