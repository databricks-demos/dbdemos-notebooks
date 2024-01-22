# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-setup&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

dbutils.widgets.text("reset_all_data", "false", "Reset Data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

import re
min_required_version = "13.3"
version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
version_search = re.search('^([0-9]*\.[0-9]*)', version_tag)
assert version_search, f"The Databricks version can't be extracted from {version_tag}, shouldn't happen, please correct the regex"
current_version = float(version_search.group(1))
assert float(current_version) >= float(min_required_version), f'The Databricks version of the cluster must be >= {min_required_version}. Current version detected: {current_version}'
assert "ml" in version_tag.lower(), f"The Databricks ML runtime must be used. Current version detected doesn't contain 'ml': {version_tag} "

# COMMAND ----------

volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

if reset_all_data:
  assert len(volume_folder) > 5
  print(f'clearing up db {dbName}')
  spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")
  if volume_folder.endswith("volume_claims"):
    try:
      dbutils.fs.rm(volume_folder, True)
    except Exception as e:
      print(f'Could not clean folder {volume_folder}, volume might not exist? {e}')

def use_and_create_db(catalog, dbName, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  spark.sql(f"USE CATALOG `{catalog}`")
  spark.sql(f"""create database if not exists `{dbName}` """)

assert catalog not in ['hive_metastore', 'spark_catalog']
#If the catalog is defined, we force it to the given value and throw exception if not.
if len(catalog) > 0:
  current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
  if current_catalog != catalog:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs:
      spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
      if catalog == 'dbdemos':
        spark.sql(f"ALTER CATALOG `{catalog}` OWNER TO `account users`")
  use_and_create_db(catalog, dbName)

if catalog == 'dbdemos':
  try:
    spark.sql(f"GRANT CREATE, USAGE on DATABASE `{catalog}`.`{dbName}` TO `account users`")
    spark.sql(f"ALTER SCHEMA `{catalog}`.`{dbName}` OWNER TO `account users`")
  except Exception as e:
    print("Couldn't grant access to the schema to all users:"+str(e))    

print(f"using catalog.database `{catalog}`.`{dbName}`")
spark.sql(f"""USE `{catalog}`.`{dbName}`""")    

#Return true if the folder is empty or does not exists
def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

# COMMAND ----------

import os
import requests
import timeit
import time
spark.sql(f'CREATE VOLUME IF NOT EXISTS {volume_name};')

import collections
 
def download_file_from_git(dest, owner, repo, path):
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

    if not os.path.exists(dest):
      os.makedirs(dest)
    from concurrent.futures import ThreadPoolExecutor
    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents{path}').json()
    files = [f['download_url'] for f in files if 'NOTICE' not in f['name']]
    def download_to_dest(url):
         download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
        collections.deque(executor.map(download_to_dest, files))
        
  
if reset_all_data or is_folder_empty(volume_folder+"/Accidents") or \
                     is_folder_empty(volume_folder+"/Claims") or \
                     is_folder_empty(volume_folder+"/Policies") or \
                     is_folder_empty(volume_folder+"/Images") or \
                     is_folder_empty(volume_folder+"/Telematics"):

  #Accidents
  download_file_from_git(volume_folder+'/Accidents/metadata', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Accidents/metadata")
  download_file_from_git(volume_folder+'/Accidents/images', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Accidents/images")
  #Claims
  download_file_from_git(volume_folder+'/Claims', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Claims/Claims")
  #Policies
  download_file_from_git(volume_folder+'/Policies', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Policies")
  #Telematics
  download_file_from_git(volume_folder+'/Telematics', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Telematics")
  #training images
  download_file_from_git(volume_folder+'/Images', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Images")
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

import numpy as np
import mlflow
import pandas as pd
import torch
import pyspark.sql.functions as F

# COMMAND ----------

#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  #You can programatically get a PAT token with the following
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  xp_root_path = f"/Shared/dbdemos/experiments/{demo_name}"
  try:
    r = w.workspace.mkdirs(path=xp_root_path)
  except Exception as e:
    print(f"ERROR: couldn't create a folder for the experiment under {xp_root_path} - please create the folder manually or  skip this init (used for job only: {e})")
    raise e
  xp = f"{xp_root_path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)
  set_experiment_permission(xp)
  return mlflow.get_experiment_by_name(xp)

def set_experiment_permission(experiment_path):
  from databricks.sdk import WorkspaceClient
  from databricks.sdk.service import iam
  w = WorkspaceClient()
  try:
      status = w.workspace.get_status(experiment_path)
  except Exception as e:
      print(f"error setting up shared experiment {experiment_path} permission: {e}")
  w.permissions.set("experiments", request_object_id=status.object_id,  access_control_list=[
                          iam.AccessControlRequest(group_name="users", permission_level=iam.PermissionLevel.CAN_MANAGE)])
  print(f"Experiment on {experiment_path} was set public")
