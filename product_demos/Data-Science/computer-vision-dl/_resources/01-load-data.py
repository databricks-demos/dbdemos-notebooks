# Databricks notebook source
dbutils.widgets.text("volume_folder", "")
volume_folder = dbutils.widgets.get("volume_folder")

# COMMAND ----------

# MAGIC %pip install awscli

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/data
# MAGIC aws s3 cp --no-progress --no-sign-request s3://amazon-visual-anomaly/VisA_20220922.tar /tmp

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/data
# MAGIC tar xf /tmp/VisA_20220922.tar --no-same-owner -C /tmp/data/ 

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy data to the volume

# COMMAND ----------

dbutils.fs.rm(volume_folder + "/images", recurse=True)
dbutils.fs.rm(volume_folder + "/labels", recurse=True)
dbutils.fs.mkdirs(volume_folder + "/images")
dbutils.fs.cp("file:/tmp/data/pcb1/Data/Images/", volume_folder + "/images", recurse=True)
dbutils.fs.mkdirs(volume_folder + "/labels")
dbutils.fs.cp("file:/tmp/data/pcb1/image_anno.csv/", volume_folder + "/labels")
