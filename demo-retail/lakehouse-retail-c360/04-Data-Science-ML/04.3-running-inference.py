# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Inference - Batch or serverless real-time
# MAGIC
# MAGIC
# MAGIC With AutoML, our best model was automatically saved in our MLFlow registry.
# MAGIC
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained. That's what we did in our Delta Live Table pipeline!
# MAGIC
# MAGIC Alternatively, this can be schedule in a separate job. Here is an example to show you how MLFlow can be directly used to retriver the model and run inferences.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.3-running-inference&demo_name=lakehouse-retail-c360&event=VIEW">

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

model_name = "dbdemos_customer_churn"

#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')   


#                                                                                                Alias
#                                                                                  Model name       |
#                                                                                        |          |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@prod")
#We can use the function in SQL
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# DBTITLE 1,Run inferences
columns = predict_churn_udf.metadata.get_input_schema().input_names()
import pyspark.sql.functions as F
spark.table('ml_churn_features').withColumn("churn_prediction", predict_churn_udf(*columns)).display()

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.{model_name}@prod")
df = spark.table('ml_churn_features').select(*columns).limit(10).toPandas()
df['churn_prediction'] = model.predict(df)
df.head(3)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Realtime model serving with Databricks serverless serving
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="700" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-c360-model-serving.png?raw=true" />
# MAGIC
# MAGIC Databricks also provides serverless serving.
# MAGIC
# MAGIC Click on model Serving, enable realtime serverless and your endpoint will be created, providing serving over REST api within a Click.
# MAGIC
# MAGIC Databricks Serverless offer autoscaling, including downscaling to zero when you don't have any traffic to offer best-in-class TCO while keeping low-latencies model serving.
# MAGIC
# MAGIC To deploy your serverless model, open the [Model Serving menu](https://e2-demo-tools.cloud.databricks.com/?o=1660015457675682#mlflow/endpoints), and select the model you registered within Unity Catalog.

# COMMAND ----------

dataset = spark.table('ml_churn_features').select(*columns).limit(3).toPandas()
#Make it a string to send to the inference endpoint
dataset['last_transaction'] = dataset['last_transaction'].astype(str)
dataset

# COMMAND ----------

# DBTITLE 1,Call the REST API deployed using standard python
import os
import requests
import numpy as np
import pandas as pd
import json

model_endpoint_name = "dbdemos_customer_churn_endpoint"

def score_model(dataset):
  url = f'https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/serving-endpoints/{model_endpoint_name}/invocations'
  headers = {'Authorization': f'Bearer {dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')}
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

#Deploy your model and uncomment to run your inferences live!
#score_model(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next step: Leverage inferences and automate actions to increase revenue
# MAGIC
# MAGIC ## Automate action to reduce churn based on predictions
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
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
# MAGIC
# MAGIC <a href='/sql/dashboards/f25702b4-56d8-40a2-a69d-d2f0531a996f'>Open the Churn prediction DBSQL dashboard</a>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reducing churn leveraging Databricks GenAI and LLMs capabilities 
# MAGIC
# MAGIC GenAI provides unique capabilities to improve your customer relationship, providing better services but also better analyzing your churn risk.
# MAGIC
# MAGIC Databricks provides built-in GenAI capabilities for you to accelerate such GenAI apps deployment. 
# MAGIC
# MAGIC Discover how with the [04.4-GenAI-for-churn]($./04.4-GenAI-for-churn) Notebook.
# MAGIC
# MAGIC [Go back to the introduction]($../00-churn-introduction-lakehouse)
