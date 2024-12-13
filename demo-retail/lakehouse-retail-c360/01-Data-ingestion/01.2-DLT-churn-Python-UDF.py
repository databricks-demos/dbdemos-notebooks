# Databricks notebook source
# DBTITLE 1,Let's install mlflow & the ML libs to be able to load our model (from requirement.txt file):
# MAGIC %pip install mlflow==2.19.0 cloudpickle==2.2.1 databricks-automl-runtime==0.2.21 category-encoders==2.6.3 holidays==0.45 lightgbm==4.3.0
# MAGIC #If you issues, make sure this matches your automl dependency version. For prod usage, use env_manager='conda'
# MAGIC %pip install azure-core azure-storage-file-datalake #for the display() in Azure only
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to load the `predict_churn` model as a spark udf and save it as a SQL function. While this code was present in the SQL notebook, it won't be run by the DLT engine (since the notebook is SQL we only read sql cess)
# MAGIC  
# MAGIC For the UDF to be available, you must this notebook in your DLT. (Currently mixing python in a SQL DLT notebook won't run the python)
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.2-DLT-churn-Python-UDF&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

import mlflow 
mlflow.set_registry_uri('databricks-uc')
#                                                                                                     Stage/version  
#                                                                                   Model name               |        
#                                                                                       |                    |        
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/main__build.dbdemos_retail_c360.dbdemos_customer_churn@prod", "int")
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC
# MAGIC This notebook must be included in your DLT "libraries" parameter:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     "id": "95f28631-1884-425e-af69-05c3f397dd90",
# MAGIC     "name": "xxxx",
# MAGIC     "storage": "/demos/dlt/lakehouse_churn/xxxxx",
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
# MAGIC                 "path": "/Repos/xxxx/01.2-DLT-churn-Python-UDF"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.1-DLT-churn-SQL"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "target": "retail_lakehouse_churn_xxxx",
# MAGIC     "continuous": false,
# MAGIC     "development": false
# MAGIC }
# MAGIC ```
