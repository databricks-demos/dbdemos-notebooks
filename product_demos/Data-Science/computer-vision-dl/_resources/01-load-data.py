# Databricks notebook source
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
# MAGIC # Mode data to DBFS

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/dbdemos/manufacturing/pcb 
# MAGIC mkdir -p /dbfs/dbdemos/manufacturing/pcb/labels
# MAGIC cp -r /tmp/data/pcb1/Data/Images/ /dbfs/dbdemos/manufacturing/pcb/
# MAGIC cp /tmp/data/pcb1/image_anno.csv /dbfs/dbdemos/manufacturing/pcb/labels/
