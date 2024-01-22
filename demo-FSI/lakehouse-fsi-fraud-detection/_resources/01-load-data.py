# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
import os
import requests
import timeit
import time
folder = "/dbdemos/fsi/fraud-detection"
from concurrent.futures import ThreadPoolExecutor

#Return true if the folder is empty or does not exists
def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

import collections
def download_file(url, destination):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename
  
def download_file_from_git(dest, owner, repo, path):
    if not os.path.exists(dest):
      os.makedirs(dest)
    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents{path}').json()
    files = [f['download_url'] for f in files if 'NOTICE' not in f['name']]
    def download_to_dest(url):
         download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
        collections.deque(executor.map(download_to_dest, files))
      
if reset_all_data or is_folder_empty(folder+"/customers_parquet") or is_folder_empty(folder+"/transactions_parquet") or is_folder_empty(folder+"/fraud_report") or is_folder_empty(folder+"/country_code"):
  if reset_all_data:
    dbutils.fs.rm("/dbdemos/fsi/fraud-detection", True)
    
  #customers
  download_file_from_git('/dbfs'+folder+'/customers_parquet', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/customers")
  #transactions
  download_file_from_git('/dbfs'+folder+'/transactions_parquet', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/transactions")
  #countries
  download_file_from_git('/dbfs'+folder+'/country_code', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/country_code")
  #countries
  download_file_from_git('/dbfs'+folder+'/fraud_report_parquet', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/fraud_report")
    
  def write_to(folder, output_format, output_folder):
    spark.read.format('parquet').load(folder).repartition(16).write.format(output_format).option('header', 'true').mode('overwrite').save(output_folder)
    
  with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(write_to(folder+'/transactions_parquet', 'json', folder+'/transactions'))
    executor.submit(write_to(folder+'/customers_parquet', 'csv', folder+'/customers'))
    executor.submit(write_to(folder+'/fraud_report_parquet', 'csv', folder+'/fraud_report'))

else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
