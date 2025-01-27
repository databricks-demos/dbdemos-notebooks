# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow==2.20.0 cloudpickle==2.2.1 
# MAGIC # hardcode the ml 15.4 LTS libraries versions here - should move to env_manager='conda' for prod use-case instead
# MAGIC %pip install category-encoders==2.6.3 cffi==1.15.1 databricks-automl-runtime==0.2.21 defusedxml==0.7.1 holidays==0.45 lightgbm==4.2.0 lz4==4.3.2 matplotlib==3.7.2 numpy==1.23.5 pandas==1.5.3 psutil==5.9.0 pyarrow==14.0.1 scikit-learn==1.3.0 scipy==1.11.1
# MAGIC %pip install azure-core azure-storage-file-datalake #for the display() in Azure only

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to load the wind turbine prediction model as a spark udf and save it as a SQL function
# MAGIC  
# MAGIC Make sure you add this notebook in your DLT job to have access to the `get_turbine_status` function. (Currently mixing python in a SQL DLT notebook won't run the python)
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.2-DLT-Wind-Turbine-SQL-UDF&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                                                                        Stage/version  
#                                                                                           Model name                          |        
#                                                                                               |                               |        
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/main_build.dbdemos_iot_platform.dbdemos_turbine_maintenance@prod", "string")
spark.udf.register("predict_maintenance", predict_maintenance_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC
# MAGIC This notebook must be included in your DLT "libraries" parameter:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     "id": "95f28631-1884-425e-af69-05c3f397dd90",
# MAGIC     "name": "xxxx",
# MAGIC     "storage": "/demos/dlt/lakehouse_iot_wind_turbine/xxxxx",
# MAGIC     "configuration": {
# MAGIC         "pipelines.useV2DetailsPage": "true"
# MAGIC     },
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.1-DLT-Wind-Turbine-SQL"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.2-DLT-Wind-Turbine-SQL-UDF"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "target": "retail_lakehouse_churn_xxxx",
# MAGIC     "continuous": false,
# MAGIC     "development": false
# MAGIC }
# MAGIC ```
