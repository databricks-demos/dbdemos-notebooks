# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow==2.14.3 importlib-metadata==6.8.0 cloudpickle==2.2.1 zipp==3.16.2
# MAGIC %pip install category-encoders==2.5.1.post0 cffi==1.15.0 databricks-automl-runtime==0.2.15 defusedxml==0.7.1 holidays==0.18 lightgbm==3.3.4 psutil==5.8.0 scikit-learn==1.1.1 typing-extensions==4.1.1 
# MAGIC %pip install azure-core azure-storage-file-datalake #for the display() in Azure only
# MAGIC %pip install --ignore-installed Jinja2==3.1.2 markupsafe==2.1.1

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
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/main__build.dbdemos_iot_turbine.dbdemos_turbine_maintenance@prod", "string")
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
