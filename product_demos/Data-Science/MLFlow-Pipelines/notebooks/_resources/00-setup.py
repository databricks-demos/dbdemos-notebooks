# Databricks notebook source
# MAGIC %sh 
# MAGIC cd /tmp
# MAGIC wget https://github.com/stedy/Machine-Learning-with-R-datasets/raw/master/insurance.csv
# MAGIC mkdir -p /dbfs/demos/fsi/insurance
# MAGIC mv insurance.csv /dbfs/demos/fsi/insurance

# COMMAND ----------

# MAGIC %run ../../../../../_resources/00-global-setup $db_prefix=fsi $min_dbr_version=11.0

# COMMAND ----------

spark.read.csv('/demos/fsi/insurance', header=True, schema="age	int, sex string, bmi float, children int, smoker string, region string, charges float").write.mode('overwrite').saveAsTable("insurance_charge")

# COMMAND ----------

from pyspark.sql.functions import col
