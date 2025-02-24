# Databricks notebook source
# MAGIC %pip install mlflow==2.20.2 transformers==4.49.0 torch==1.13.1 torchvision==0.20.1 accelerate==1.4.0 importlib-metadata==6.8.0  zipp==3.16.2 Pillow==9.2.0 filelock==3.6.0

# COMMAND ----------

# MAGIC %md
# MAGIC # Scoring the Accident Images with Delta Live Table
# MAGIC
# MAGIC This notebook demonstrates how to do the scoring using Delta Live Table

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, col

catalog = "main__build"
schema = dbName = db = "dbdemos_fsi_smart_claims"
volume_name = "volume_claims"

# COMMAND ----------

@dlt.view(comment="Accident images uploaded from mobile app")
def raw_accident_image():
    return(
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "binaryFile")
      .load(f"/Volumes/{catalog}/{db}/{volume_name}/Accidents/images"))
      .withColumn("image_name", F.regexp_extract(col("path"), r".*/(.*?).jpg", 1)))
      
@dlt.table(comment="Accident images metadata")
def raw_accident_metadata():
    return(
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"/Volumes/{catalog}/{db}/{volume_name}/Accidents/metadata"))

# COMMAND ----------

import mlflow
model_name = "dbdemos_claims_damage_level"
#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')

#Loads the model from UC
predict_damage_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@prod")
columns = predict_damage_udf.metadata.get_input_schema().input_names()

# COMMAND ----------

@dlt.table(comment="Accident details with images metadata, increased with damage evaluation using AI")
def accident_images():
    raw_images = (dlt.readStream("raw_accident_image")
                     .withColumn("damage_prediction", predict_damage_udf(*columns)))

    #Only process 1k claims for the demo to run faster
    metadata = dlt.read("raw_accident_metadata").orderBy(F.rand()).limit(1000)
    return raw_images.join(metadata, on="image_name")
