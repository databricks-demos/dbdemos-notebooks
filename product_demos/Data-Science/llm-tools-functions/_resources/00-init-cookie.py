# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %run ./00-init

# COMMAND ----------

import concurrent.futures

volume_folder = f"/Volumes/{catalog}/{db}/{volume_name}/cookies"

def download_and_save_table(table):
    if not spark.catalog.tableExists("cookies_"+table):
        DBDemos.download_file_from_git(volume_folder + "/" + table, "databricks-demos", "dbdemos-dataset", "llm/cookies-demo/" + table)
        spark.read.format('parquet').load(volume_folder + "/" + table).write.mode('overwrite').saveAsTable("cookies_"+table)

try:
    tables = ["customer_reviews", "customers", "franchises", "gold_reviews_chunked", "suppliers", "transactions"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_and_save_table, tables)
except Exception as e:
    print(f"Couldn't download the data properly for the demo, do you have access to internet?")
    raise e
