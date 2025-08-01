# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # What is Databricks SQL's read_files function?
# MAGIC The `read_files` function lets you directly query and ingest files (like `CSV`, `JSON`, `Parquet`, etc.) from cloud storage or Unity Catalog using SQL—no table setup needed.
# MAGIC
# MAGIC It supports ad-hoc exploration and incremental ingestion, including with `STREAMING TABLES`, and automatically infers schema and handles directories or patterns.
# MAGIC
# MAGIC *Note: Lakeflow Declarative Pipelines use read_files to easily load file data into tables, powering both batch and streaming workflows.*
# MAGIC
# MAGIC For more details, open [the read_files documentation](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files).

# COMMAND ----------

# DBTITLE 1,Data initialization - run the cell to prepare the demo data.
# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

display(dbutils.fs.ls(volume_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Basic Usage: Automatic Format Dectection
# MAGIC
# MAGIC One of the key advantages of `read_files` is automatic format detection. Let's try to read many file formats in our demo data folder, and use read_files to detect the file format

# COMMAND ----------

# DBTITLE 1,Reading JSON files (auto-detected)
display(spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_json') LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,Reading CSV files (auto-detected)
display(spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_csv') LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,Reading parquet files (auto-detected)
display(spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_parquet') LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,Partitioned Parquet auto-detection
display(spark.sql(f"SELECT year, month, COUNT(*) as records FROM read_files('{volume_folder}/user_parquet_partitioned') GROUP BY year, month"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC `read_files` also supports powerful glob patterns for selective file reading. You can select the specific format you want to read.

# COMMAND ----------

# DBTITLE 1,Basic glob patterns
display(spark.sql(f"SELECT 'JSON Files' as source, * FROM read_files('{volume_folder}/*json*') LIMIT 3"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Schema Inference
# MAGIC
# MAGIC Different formats have different schema inference capabilities and performance.
# MAGIC
# MAGIC We can also use schema hints to override the schema inferrence.

# COMMAND ----------

# DBTITLE 1,JSON schema (inferred from data)
json_schema = spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_json') LIMIT 0").schema
print(json_schema.treeString())

# COMMAND ----------

# DBTITLE 1,CSV schema (inferred with headers)
csv_schema = spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_csv') LIMIT 0").schema  
print(csv_schema.treeString())

# COMMAND ----------

# DBTITLE 1,Data Type Precision Comparison
display(spark.sql(f"""
SELECT
  format,
  MAX(id_type) AS id_type,
  MAX(age_group_type) AS age_group_type,
  MAX(date_type) AS date_type
FROM (
SELECT 
  'JSON' as format,
  typeof(id) as id_type,
  typeof(age_group) as age_group_type,
  typeof(creation_date) as date_type
FROM read_files('{volume_folder}/user_json')
UNION ALL
SELECT 
  'CSV' as format,
  typeof(id) as id_type, 
  typeof(age_group) as age_group_type,
  typeof(creation_date) as date_type
FROM read_files('{volume_folder}/user_csv')
UNION ALL
SELECT 
  'Parquet' as format,
  typeof(id) as id_type,
  typeof(age_group) as age_group_type, 
  typeof(creation_date) as date_type
FROM read_files('{volume_folder}/user_parquet')
) type_comparision
GROUP BY format
"""))

# COMMAND ----------

# DBTITLE 1,Using schema hints to override JSON inference
display(spark.sql(f"""
SELECT 
  id,
  typeof(id) as id_type_after_hint,
  age_group,
  typeof(age_group) as age_group_type_after_hint
FROM read_files(
  '{volume_folder}/user_json',
  schemaHints => 'id bigint, age_group string'
) LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Format-Specific Features
# MAGIC
# MAGIC There are some particular options that are specific to each format with `read_files`

# COMMAND ----------

# DBTITLE 1,CSV without headers
display(spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_csv_no_headers', format => 'csv', header => 'false') LIMIT 5"))

# COMMAND ----------

# DBTITLE 1,CSV without headers (manual schema)
display(spark.sql(f"""
SELECT * FROM read_files(
  '{volume_folder}/user_csv_no_headers',
  format => 'csv',
  schema => 'id bigint, creation_date string, firstname string, lastname string, email string, address string, gender double, age_group double'
) LIMIT 5
"""))

# COMMAND ----------

# DBTITLE 1,CSV with pipe delimiter
display(spark.sql(f"""
SELECT * FROM read_files(
  '{volume_folder}/user_csv_pipe_delimited',
  format => 'csv',
  sep => '|'  
) LIMIT 5
"""))

# COMMAND ----------

# DBTITLE 1,JSON with column type inference
display(spark.sql(f"""
SELECT 
  firstname,
  lastname,
  id,
  typeof(id) as id_inferred_type,
  age_group,
  typeof(age_group) as age_group_inferred_type
FROM read_files(
  '{volume_folder}/user_json',
  inferColumnTypes => true
) LIMIT 5  
"""))

# COMMAND ----------

# DBTITLE 1,Parquet optimization features
# Demonstrate column pruning (Parquet's key advantage)
print("⚡ Parquet Column Pruning Demo:")
import time

# Read all columns
start_time = time.time()
all_cols_count = spark.sql(f"SELECT * FROM read_files('{volume_folder}/user_parquet')").count()
all_cols_time = time.time() - start_time

# Read only specific columns  
start_time = time.time()
select_cols_count = spark.sql(f"SELECT id, firstname FROM read_files('{volume_folder}/user_parquet')").count()
select_cols_time = time.time() - start_time

print(f"📊 All columns: {all_cols_count:,} records in {all_cols_time:.2f}s")
print(f"📊 2 columns: {select_cols_count:,} records in {select_cols_time:.2f}s") 
print(f"⚡ Column pruning speedup: {all_cols_time/select_cols_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming Usage
# MAGIC
# MAGIC `read_files` can be used in streaming tables to ingest files into Delta Lake. `read_files` leverages Auto Loader when used in a streaming table query.
# MAGIC
# MAGIC To do so, simply add the `STREAM` keyword to your SQL queries:

# COMMAND ----------

# DBTITLE 1,Basic streaming example
# Create a streaming view
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW streaming_json_users AS
SELECT 
  *,
  current_timestamp() as processing_time
FROM STREAM read_files(
  '{volume_folder}/user_json',
  maxFilesPerTrigger => 5,
  schemaLocation => '{volume_folder}/read_files_streaming_schema'
)
""")

display(spark.sql("SELECT COUNT(*) as total_records FROM streaming_json_users"))

# COMMAND ----------

# DBTITLE 1,Multi-format streaming (separate streams with schema locations)
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW streaming_csv_users AS  
SELECT 
  *,
  'CSV' as source_format,
  current_timestamp() as processing_time
FROM STREAM read_files(
  '{volume_folder}/user_csv',
  schemaLocation => '{volume_folder}/csv_streaming_schema'
)
""")

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW streaming_parquet_users AS
SELECT 
  *,
  'PARQUET' as source_format, 
  current_timestamp() as processing_time
FROM STREAM read_files(
  '{volume_folder}/user_parquet',
  schemaLocation => '{volume_folder}/parquet_streaming_schema'
)
""")

display(spark.sql("""
SELECT 'CSV' as format, COUNT(*) as records FROM streaming_csv_users
UNION ALL  
SELECT 'PARQUET' as format, COUNT(*) as records FROM streaming_parquet_users
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. read_files vs Auto Loader
# MAGIC
# MAGIC We have covered some basic features of `read_files`. However, there might be some questions about when to use `read_files` and when to use Auto Loader.
# MAGIC
# MAGIC We have some comparison and decision matrix that could help you decide when to leverage the power of `read_files` and Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | Capability | read_files | Auto Loader |
# MAGIC |-----------|------------|-------------|
# MAGIC | Language | SQL | Python  |
# MAGIC | Ad-hoc queries | ✅ Perfect | Incremental Streaming focus  |
# MAGIC | Batch processing | ✅ Excellent | Incremental Streaming focus |
# MAGIC | Multi-format API | ✅ Unified API | Need to declare format |
# MAGIC | Streaming performance | Optimized for you | Mode advanced options for more control |
# MAGIC | Schema evolution | ⚠️ Manual | ✅ Automatic |
# MAGIC | Setup complexity | ✅ Zero setup | Pythonic config |
# MAGIC | File notifications | ❌ No | ✅ Cloud notifications |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC We have seen what the capabilities of Databricks SQL's `read_files` are, and now you can apply it in your projects.
# MAGIC
# MAGIC Open the [02-Auto-loader-schema-evolution-Ingestion]($./02-Auto-loader-schema-evolution-Ingestion) Notebook to explore the Auto Loader options!

# COMMAND ----------

DBDemos.stop_all_streams()
