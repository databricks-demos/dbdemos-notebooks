# Databricks notebook source
dbutils.widgets.text("volume_folder", "")
volume_folder = dbutils.widgets.get("volume_folder")

# COMMAND ----------

# MAGIC %pip install boto3

# COMMAND ----------

# MAGIC %md
# MAGIC # Download & extract the VisA dataset in pure Python
# MAGIC `%sh` and the aws-cli are not available on serverless compute, so we download the
# MAGIC public (unsigned) S3 object with boto3 and extract it with Python's `tarfile`.

# COMMAND ----------

import os, tarfile, boto3
from botocore import UNSIGNED
from botocore.config import Config

local_root = "/local_disk0/tmp/visa"
os.makedirs(local_root, exist_ok=True)
tar_path = os.path.join(local_root, "VisA_20220922.tar")

if not os.path.exists(tar_path):
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3.download_file("amazon-visual-anomaly", "VisA_20220922.tar", tar_path)

extract_dir = os.path.join(local_root, "data")
os.makedirs(extract_dir, exist_ok=True)
with tarfile.open(tar_path) as t:
    t.extractall(extract_dir, filter="data")  # only need pcb1 below

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy data to the volume

# COMMAND ----------

dbutils.fs.rm(volume_folder + "/images", recurse=True)
dbutils.fs.rm(volume_folder + "/labels", recurse=True)
dbutils.fs.mkdirs(volume_folder + "/images")
dbutils.fs.cp(f"file:{extract_dir}/pcb1/Data/Images/", volume_folder + "/images", recurse=True)
dbutils.fs.mkdirs(volume_folder + "/labels")
dbutils.fs.cp(f"file:{extract_dir}/pcb1/image_anno.csv", volume_folder + "/labels")
