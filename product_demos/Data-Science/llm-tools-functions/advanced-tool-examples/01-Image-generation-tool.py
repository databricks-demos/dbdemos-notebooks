# Databricks notebook source
# MAGIC %pip install --quiet -U databricks-sdk==0.23.0 langchain-community==0.2.10 langchain-openai==0.1.19 mlflow==2.14.3 faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init $reset_all=false

# COMMAND ----------

# MAGIC %md 
# MAGIC # Leveraging shutterstock Image Generation model as a tool

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION generate_image(prompt STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'This function generate an images and returns its content as base64. It also saves the image in the demo volume.'
# MAGIC AS
# MAGIC $$
# MAGIC   try:
# MAGIC     prompt = 'A lake made of data and bytes, going around a house: the Data Lakehouse'
# MAGIC     databricks_endpoint = f"{WorkspaceClient().config.host}/serving-endpoints/databricks-shutterstock-imageai/invocations"
# MAGIC
# MAGIC     databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# MAGIC
# MAGIC     headers = {"Authorization": f"Bearer {databricks_token}"}
# MAGIC     jdata = {"prompt": prompt}
# MAGIC
# MAGIC     response = requests.post(databricks_endpoint, json=jdata, headers=headers).json()
# MAGIC     image_data = response['data'][0]['b64_json']
# MAGIC     image_id   = response['id']
# MAGIC
# MAGIC     # Assuming base64_str is the string value without 'data:image/jpeg;base64,'
# MAGIC     file_object=io.BytesIO(base64.decodebytes(bytes(image_data, "utf-8")))
# MAGIC     img = Image.open(file_object)
# MAGIC     # Convert to JPEG and upload to volumes
# MAGIC     # this actually works in a notebook, but not yet in a DBSQL UDF, which cannot connect to a volume
# MAGIC     img.save(f'/Volumes/mimiq_retail_prod/results/images/{image_id}.jpeg', format='JPEG') 
# MAGIC   except:
# MAGIC     return {"temperature_in_celsius": 25.0, "rain_in_mm": 0.0}
# MAGIC $$;
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT get_weather(52.52, 13.41) as weather;

# COMMAND ----------

#Use a service principal token to access the serving endpoint. Never package PAT token within your function. This will be improved soon to simplify user experience.
databricks_token = dbutils.secrets.get('dbdemos', 'llm-agent-tools')
#databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests, io, base64
from PIL import Image

prompt = 'A lake made of data and bytes, going around a house: the Data Lakehouse, detailed, 8k'

databricks_endpoint = f"{WorkspaceClient().config.host}/serving-endpoints/databricks-shutterstock-imageai/invocations"
databricks_endpoint = f"https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/databricks-shutterstock-imageai/invocations"


headers = {"Authorization": f"Bearer {databricks_token}"}
jdata = {"prompt": prompt}

response = requests.post(databricks_endpoint, json=jdata, headers=headers).json()
print(response)
image_data = response['data'][0]['b64_json']
image_id   = response['id']

# Assuming base64_str is the string value without 'data:image/jpeg;base64,'
file_object=io.BytesIO(base64.decodebytes(bytes(image_data, "utf-8")))
img = Image.open(file_object)
# Convert to JPEG and upload to volumes
# this actually works in a notebook, but not yet in a DBSQL UDF, which cannot connect to a volume
img.save(f'/Volumes/mimiq_retail_prod/results/images/{image_id}.jpeg', format='JPEG') 

# COMMAND ----------


