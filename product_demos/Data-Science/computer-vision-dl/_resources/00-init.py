# Databricks notebook source
# MAGIC %md
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F_resources%2F00-init&demo_name=llm-rag-chatbot&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fllm-rag-chatbot%2F_resources%2F00-init&version=1">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library updates
# MAGIC SDK 0.1.6 has an issue with notebook-native authentication.  See https://github.com/databricks/databricks-sdk-py/issues/221
# MAGIC Update to the latest version of databricks-sdk

# COMMAND ----------

# DBTITLE 1,SDK 0.1.6 (DBR 13.3 and 14.3) update
# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo initialization

# COMMAND ----------

# DBTITLE 1,Reset all data widget
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# DBTITLE 1,Configure the catalog, schema, and volume names
# MAGIC %run ../config

# COMMAND ----------

# DBTITLE 1,Common dbdemo setup
# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

# DBTITLE 1,Setup the demo catalog, schema, and volume context
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

# DBTITLE 1,Import modules that will be used in the notebooks
import os
import torch
import mlflow
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.functions import to_date, col, regexp_extract, rand, to_timestamp, initcap, sha1
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data initialization (optional)

# COMMAND ----------

# DBTITLE 1,Install data into volume
if reset_all_data or DBDemos.is_folder_empty(volume_folder+"/labels") or DBDemos.is_folder_empty(volume_folder+"/images"):
  # data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Loading raw data under {volume_folder} , please wait a few minutes as we extract all images...")
  # Run the 01-load-data notebook in the _resources folder to load the data from S3 to the volume
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("computer-vision-dl"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"volume_folder": volume_folder})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example code: lightning dataloader from hugging face dataset example
# MAGIC
# MAGIC If your dataset is small, you could also load it from your spark dataframe with the huggingface dataset library:

# COMMAND ----------

def define_lightning_dataset_moduel():

  import pytorch_lightning as pl
  from datasets import Dataset
  class DeltaDataModuleHF(pl.LightningDataModule):
      from torch.utils.data import random_split, DataLoader
      def __init__(self, df, batch_size: int = 64):
          super().__init__()
          # For big dataset, you can use IterableDataset.from_spark()
          self.dataset = Dataset.from_spark(df.select('content', 'label'))
          self.splits = self.dataset.train_test_split(test_size=0.1)
          self.batch_size = batch_size
          self.transform = tf.Compose([
                  tf.Lambda(lambda b: Image.open(io.BytesIO(b)).convert("RGB")),
                  tf.Resize(256),
                  tf.CenterCrop(224),
                  tf.ToTensor(),
                  tf.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
              ])
          
          self.train_ds = self.splits['train'].map(lambda e: {'content': self.transform(e['content']), 'label': e['label']})
          self.train_ds.set_format(type='torch')
          self.val_ds = self.splits['test'].map(lambda e: {'content': self.transform(e['content']), 'label': e['label']})
          self.val_ds.set_format(type='torch')


      def setup(self, stage: str):
          print(f"preparing dataset, stage: {stage}")

      def train_dataloader(self):
          return torch.utils.data.DataLoader(self.train_ds, batch_size=self.batch_size, num_workers=8)
        
      def val_dataloader(self):
          return torch.utils.data.DataLoader(self.val_ds, batch_size=self.batch_size, num_workers=8)

      def test_dataloader(self):
          return torch.utils.data.DataLoader(self.val_ds, batch_size=self.batch_size, num_workers=8)
