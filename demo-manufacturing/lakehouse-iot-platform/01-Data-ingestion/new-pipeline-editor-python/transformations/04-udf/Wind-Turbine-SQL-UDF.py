# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow==3.1.0
# MAGIC %pip install azure-core azure-storage-file-datalake #for the display() in Azure only
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to load the wind turbine prediction model as a spark udf and save it as a SQL function
# MAGIC  
# MAGIC Make sure you add this notebook in your (Lakeflow) Declarative Pipelines job to have access to the `get_turbine_status` function. (Currently mixing python in a SQL (Lakeflow) Declarative Pipelines notebook won't run the python)
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.2-DLT-Wind-Turbine-SQL-UDF&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')     
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/main_build.dbdemos_iot_platform.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC
# MAGIC This notebook must be included in your (Lakeflow) Declarative Pipeline "libraries" parameter:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     "id": "b5dbbef6-aab1-45d8-9fa3-04d0da636a05",
# MAGIC     "pipeline_type": "WORKSPACE",
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5,
# MAGIC                 "mode": "ENHANCED"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "event_log": {
# MAGIC         "catalog": "main_build",
# MAGIC         "schema": "dbdemos_iot_platform",
# MAGIC         "name": "event_log"
# MAGIC     },
# MAGIC     "continuous": false,
# MAGIC     "channel": "CURRENT",
# MAGIC     "photon": false,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/quentin.ambard@databricks.com/dbdemos-notebooks/demo-manufacturing/lakehouse-iot-platform/01-Data-ingestion/01.1-DLT-Wind-Turbine-SQL"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/quentin.ambard@databricks.com/dbdemos-notebooks/demo-manufacturing/lakehouse-iot-platform/01-Data-ingestion/01.2-DLT-Wind-Turbine-SQL-UDF"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "dbdemos-build-manuf-iot-turbine",
# MAGIC     "edition": "ADVANCED",
# MAGIC     "catalog": "main_build",
# MAGIC     "schema": "dbdemos_iot_platform",
# MAGIC     "data_sampling": false
# MAGIC }
# MAGIC ```
