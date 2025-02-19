# Databricks notebook source
# MAGIC %md
# MAGIC #Scoring the Accident Images 
# MAGIC
# MAGIC In this notebook, we'll show how to score the incoming images of damaged vehicles as they flow into the system to come up with a damage severity score using the model that we previously training and promoted to the production.
# MAGIC
# MAGIC The end results are stored in the `accident_images` delta table
# MAGIC
# MAGIC *Note: we could also have these transformations available in a Delta Live Table. Open [02.2-EXTRA-Batch-Scoring-DLT]($./EXTRA-DLT-inference/02.2-EXTRA-Batch-Scoring-DLT) to see how it's done.*
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=02.2-Batch-Scoring&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %pip install mlflow==2.20.2

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import mlflow
# Use the Unity Catalog model registry
mlflow.set_registry_uri("databricks-uc")
# download model requirement from remote registry
requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.dbdemos_claims_damage_level@prod").download_artifacts(artifact_path="requirements.txt") 

# COMMAND ----------

# MAGIC %pip install -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Incrementally ingest the raw incoming images
# MAGIC
# MAGIC New images can typically land in a cloud storage (S3/ADLS/GCS), mounted within Unity Catalog using Volumes. 
# MAGIC
# MAGIC Let's start by ingesting them and saving them as a Delta Lake table 

# COMMAND ----------

# DBTITLE 1,Incrementally load the raw incoming JPG images
volume_path = f"/Volumes/{catalog}/{db}/{volume_name}"

(spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.schemaLocation", f"{volume_path}/checkpoint/images_shema")
            .load(f"{volume_path}/Accidents/images")
            .withColumn("image_name", F.regexp_extract(F.col("path"), r".*/(.*?.jpg)", 1))
      .writeStream
            .option("checkpointLocation", f"{volume_path}/checkpoint/images")
            .trigger(availableNow=True)
            .table("raw_accident_image")).awaitTermination()

display(spark.table("raw_accident_image").limit(10))

# COMMAND ----------

# DBTITLE 1,Incrementally load the image metadata
(spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", f"{volume_path}/checkpoint/images_m_shema")
            .load(f"{volume_path}/Accidents/metadata")
      .writeStream
            .option("checkpointLocation", f"{volume_path}/checkpoint/images_m")
            .trigger(availableNow=True)
            .table("raw_accident_metadata")).awaitTermination()
            
display(spark.table("raw_accident_metadata").limit(10))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Score the damaged vehicle image to determine damage severity
# MAGIC
# MAGIC Our claim images are now added to our tables and easily accessible. Let's load our model from Unity Catalog to score the damage. 

# COMMAND ----------

import mlflow
model_name = "dbdemos_claims_damage_level"

mlflow.set_registry_uri('databricks-uc')

#Loading the model from UC
predict_damage_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@prod")
spark.udf.register("predict_damage", predict_damage_udf)
columns = predict_damage_udf.metadata.get_input_schema().input_names()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test inferences

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT image_name, predict_damage(content) as damage_prediction, content FROM raw_accident_image LIMIT 10

# COMMAND ----------

raw_images = (spark.read.table("raw_accident_image")
                   .withColumn("damage_prediction", predict_damage_udf(*columns)))

#Only process 1k claims for the demo to run faster
metadata = spark.table("raw_accident_metadata").orderBy(F.rand()).limit(1000)

raw_images.join(metadata, on="image_name").write.mode('overwrite').saveAsTable("accident_images")

# COMMAND ----------

display(spark.table("accident_images").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Real time inference
# MAGIC
# MAGIC While this use-case is working with batch inferences (consuming incremental new data in a stream), we could also deploy our model behind a [Serverless model endpoint](#mlflow/endpoints). 
# MAGIC
# MAGIC Images can be sent as base64 data over the endpoint. For more details on how to do that, you can run `dbdemos.install('computer-vision-pcb')`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Add telematics and accident data to the claims & policy data
# MAGIC
# MAGIC Telematics data is joined with Claims and Policy data to monitor the behavior of the driver before the accident. End results are stored into "claim_policy_accident" delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE claim_policy_accident AS 
# MAGIC   SELECT
# MAGIC     t.*,
# MAGIC     a.* EXCEPT (chassis_no, claim_no)
# MAGIC   FROM
# MAGIC     claim_policy_telematics t
# MAGIC     JOIN accident_images a USING(claim_no)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, we demonstrated how to <b> retrieve the model </b> from Unity Catalog and run inferences to <b> score </b> on new image data and persist the results back into delta tables. 
# MAGIC
# MAGIC Telematics data, accident image data, claims & policy data </b> are all joined together to provide a 360 view of the accident scene to the claims investigation officer to <b>reconstruct the scene</b> and make a decision on what to do next. Eg. release funds, authorize the car for repairs, approve rental loaner car or send for further investigation.
# MAGIC
# MAGIC Open notebook [02.3-Dynamic-Rule-Engine]($./02.3-Dynamic-Rule-Engine) to see how dynamic rules can be implemented to start processing our claims faster based on this information.
