# Databricks notebook source
# MAGIC %md
# MAGIC # Predictive Maintenance Inference - Batch or serverless real-time
# MAGIC
# MAGIC
# MAGIC With AutoML, our best model was automatically saved in our MLFlow registry.
# MAGIC
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained. That's what we did in our Delta Live Table pipeline!
# MAGIC
# MAGIC Alternatively, this can be schedule in a separate job. Here is an example to show you how MLFlow can be directly used to retriver the model and run inferences.
# MAGIC
# MAGIC *Make sure you run the previous notebook to be able to access the data.*
# MAGIC
# MAGIC ## Environment Recreation
# MAGIC The cell below downloads the model artifacts associated with your model in the remote registry, which include `conda.yaml` and `requirements.txt` files. In this notebook, `pip` is used to reinstall dependencies by default.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Flakehouse_iot_turbine%2Fml_inference&dt=LAKEHOUSE_MANUFACTURING_TURBINE">

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os
import mlflow
mlflow.set_registry_uri('databricks-uc')
local_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.dbdemos_turbine_maintenance@prod").download_artifacts("") # download model from remote registry

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.17.0
# MAGIC %pip install -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry: Open MLFlow model registry and click the "User model for inference" button!

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

mlflow.set_registry_uri('databricks-uc')
#                                                                                                   Stage/version
#                                                                                 Model name              |
#                                                                                       |                 |
predict_maintenance = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{db}.dbdemos_turbine_maintenance@prod", "string")
#We can use the function in SQL
spark.udf.register("predict_maintenance", predict_maintenance)

# COMMAND ----------

# DBTITLE 1,Run inferences
columns = predict_maintenance.metadata.get_input_schema().input_names()
spark.table('turbine_hourly_features').withColumn("dbdemos_turbine_maintenance", predict_maintenance(*columns)).display()

# COMMAND ----------

# DBTITLE 1,Or in SQL directly
# MAGIC %sql
# MAGIC SELECT turbine_id, predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, percentiles_sensor_A, percentiles_sensor_B, percentiles_sensor_C, percentiles_sensor_D, percentiles_sensor_E, percentiles_sensor_F, location, model, state) as prediction FROM turbine_hourly_features

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.dbdemos_turbine_maintenance@prod")
df = spark.table('turbine_hourly_features').select(*columns).limit(10).toPandas()
df['churn_prediction'] = model.predict(df)
display(df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Realtime model serving with Databricks serverless serving
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="800" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-model-serving.gif" />
# MAGIC
# MAGIC Databricks also provides serverless serving.
# MAGIC
# MAGIC Click on model Serving, enable realtime serverless and your endpoint will be created, providing serving over REST api within a Click.
# MAGIC
# MAGIC Databricks Serverless offer autoscaling, including downscaling to zero when you don't have any traffic to offer best-in-class TCO while keeping low-latencies model serving.

# COMMAND ----------

dataset = spark.table(f'turbine_hourly_features').select(*columns).limit(3).toPandas()
dataset

# COMMAND ----------

# DBTITLE 1,Call the REST API deployed using standard python
import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}


def score_model(dataset):
  url = f'https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/model-endpoint/dbdemos_turbine_maintenance/Production/invocations'
  headers = {'Authorization': f'Bearer {dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

#Start your model endpoint in the model menu and uncomment this line to test realtime serving!
#score_model(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next step: Leverage inferences and automate action to lower cost
# MAGIC
# MAGIC ## Automate action to react on potential turbine failure
# MAGIC
# MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
# MAGIC
# MAGIC - Send targeting email campaign to the customer the most likely to churn
# MAGIC - Phone campaign to discuss with our customers and understand what's going
# MAGIC - Understand what's wrong with our line of product and fixing it
# MAGIC
# MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
# MAGIC
# MAGIC ## Track churn impact over the next month and campaign impact
# MAGIC
# MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
# MAGIC
# MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $129,914 / month!
# MAGIC
# MAGIC <img width="800px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
# MAGIC
# MAGIC <a href='/sql/dashboards/90b2c9c7-e5c7-46d8-b7fe-5b98ed84d5d1'>Open the Predictive Maintenance DBSQL dashboard</a> | [Go back to the introduction]($../00-IOT-wind-turbine-introduction-lakehouse)
