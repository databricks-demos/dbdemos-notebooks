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

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

print('run done')
DBDemos.setup_schema(catalog, db, False, volume_name)
volume_folder = f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import os
import requests
import timeit
import time
import collections
 
if DBDemos.is_any_folder_empty([volume_folder+"/Accidents", volume_folder+"/Claims", volume_folder+"/Policies", volume_folder+"/Images", volume_folder+"/Telematics"]):
  print(f'Downloading raw data under {volume_folder}...')
  #Accidents
  DBDemos.download_file_from_git(volume_folder+'/Accidents/metadata', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Accidents/metadata")
  DBDemos.download_file_from_git(volume_folder+'/Accidents/images', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Accidents/images")
  #Claims
  DBDemos.download_file_from_git(volume_folder+'/Claims', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Claims/Claims")
  #Policies
  DBDemos.download_file_from_git(volume_folder+'/Policies', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Policies")
  #Telematics
  DBDemos.download_file_from_git(volume_folder+'/Telematics', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Telematics")
  #training images
  DBDemos.download_file_from_git(volume_folder+'/Images', "databricks-demos", "dbdemos-dataset", "/fsi/smart-claims/Images")
else:
  print("data already existing.")

# COMMAND ----------

import numpy as np
import pandas as pd
import pyspark.sql.functions as F

# COMMAND ----------

#Force torch to local filestore to properly support serverless workspaces
try:
    import os
    import tempfile
    import torch

    file_store_path = os.path.join(tempfile.gettempdir(), os.environ["VIRTUAL_ENV"].split("/")[-1])
    store = torch.distributed.FileStore(file_store_path, world_size=1)
    torch.distributed.init_process_group(backend="gloo", rank=0, world_size=1, store=store)
except:
    pass
