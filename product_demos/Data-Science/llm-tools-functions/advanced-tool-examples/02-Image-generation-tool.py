# Databricks notebook source
# MAGIC %pip install --quiet -U databricks-sdk==0.41.0 mlflow==2.20.0 faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init $reset_all=false

# COMMAND ----------

# MAGIC %md 
# MAGIC # Leveraging shutterstock Image Generation model as a tool

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure you save your PAT token as a secret:
# MAGIC
# MAGIC - Open a terminal from your cluster
# MAGIC - Create the scope: `databricks secrets create-scope dbdemos`
# MAGIC - Add your token in the scope: `databricks secrets put-secret dbdemos llm-agent-tools`

# COMMAND ----------

#Use a service principal token to access the serving endpoint. Never package PAT token within your function. This will be improved soon to simplify user experience.
databricks_token = dbutils.secrets.get('dbdemos', 'llm-agent-tools')
#databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

host = WorkspaceClient().config.host
print(host)
spark.sql(f'''CREATE OR REPLACE FUNCTION generate_image_with_secret(my_prompt STRING, databricks_token STRING)
RETURNS binary
LANGUAGE PYTHON
COMMENT 'This function generate an images and returns its content as binary. It also saves the image in the demo volume.'
AS
$$
    import requests, io, base64
    from PIL import Image
    response = requests.post(f"{host}/serving-endpoints/databricks-shutterstock-imageai/invocations", 
                            json={{"prompt": my_prompt}}, headers={{"Authorization": "Bearer "+databricks_token}}).json()
    image_data = response['data'][0]['b64_json']
    image = io.BytesIO(base64.decodebytes(bytes(image_data, "utf-8")))
    return image.getvalue()
$$;''')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS generate_image;
# MAGIC CREATE OR REPLACE FUNCTION generate_image(my_prompt STRING)
# MAGIC RETURNS binary
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This function generate an images and returns its content as binary.'
# MAGIC RETURN
# MAGIC   SELECT generate_image_with_secret(my_prompt, secret('dbdemos', 'llm-agent-tools')) as image;

# COMMAND ----------

#-- let's test our function:
image = spark.sql(f"SELECT generate_image_with_secret('test', '{databricks_token}') as image")
display(image.select(
    F.col("image").alias("image", metadata={"spark.contentAnnotation": '{"mimeType": "image/jpeg"}'})
))

# COMMAND ----------

test = spark.sql(f"SELECT len(generate_image_with_secret('test', '{databricks_token}')) as image")
display(test)


# COMMAND ----------

test = spark.sql(f"SELECT len(generate_image('test')) as image")
display(test)

# COMMAND ----------

test = spark.sql(f"SELECT generate_image('test') as image").collect()[0]['image']
display(Image.open(io.BytesIO(test)))

# COMMAND ----------

test = spark.sql(f"SELECT generate_image_with_secret('test', '{databricks_token}') as image").collect()[0]['image']
display(Image.open(io.BytesIO(test)))

# COMMAND ----------

#-- let's test our function:
image = spark.sql(f"SELECT generate_image('test') as image")
image = spark.sql('select `image`.`/Volumes/xx/xx/generated_images`)
display(image.select(
    F.col("image").alias("image", metadata={"spark.contentAnnotation": '{"mimeType": "image/jpeg"}'})
))

# COMMAND ----------

# MAGIC %md
# MAGIC # Debug stuff to be removed
# MAGIC

# COMMAND ----------

host = WorkspaceClient().config.host
print(host)
spark.sql(f'''CREATE OR REPLACE FUNCTION generate_image(my_prompt STRING, databricks_token STRING)
RETURNS STRUCT<image_data STRING, image_id STRING>
LANGUAGE PYTHON
COMMENT 'This function generate an images and returns its content as base64. It also saves the image in the demo volume.'
AS
$$
    import requests, io, base64
    from PIL import Image
    response = requests.post(f"{host}/serving-endpoints/databricks-shutterstock-imageai/invocations", 
                            json={{"prompt": my_prompt}}, headers={{"Authorization": f"Bearer {databricks_token}"}}).json()
    image_data = response['data'][0]['b64_json']
    return {{"image_data": image_data, "image_id": response['id']}}
$$;''')

#-- let's test our function:
image = spark.sql(f"SELECT generate_image('test', '{databricks_token}')"))

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests, io, base64
from PIL import Image

host = WorkspaceClient().config.host
prompt = 'A nice house sitting on a lake, at sunset, surrounded with trees: the Lakehouse. The house has open windows, sunset, detailed, realistic, 8k'

response = requests.post(f"{host}/serving-endpoints/databricks-shutterstock-imageai/invocations", 
                         json={"prompt": prompt}, headers={"Authorization": f"Bearer {databricks_token}"}).json()
image_data = response['data'][0]['b64_json']
image_id   = response['id']

# Assuming base64_str is the string value without 'data:image/jpeg;base64,'
file_object=io.BytesIO(base64.decodebytes(bytes(image_data, "utf-8")))
# Convert to JPEG and upload to volumes
# this actually works in a notebook, but not yet in a DBSQL UDF, which cannot connect to a volume
Image.open(file_object).save(f'/Volumes/{catalog}/{db}/{volume_name}/images/lakehouse-{image_id}.jpeg', format='JPEG') 


# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's see the Lakehouse images generated by our AI:

# COMMAND ----------

for image in dbutils.fs.ls(f'/Volumes/{catalog}/{db}/{volume_name}/images'):
  with open(image.path[len('dbfs:'):], 'rb') as f:
    display(Image.open(io.BytesIO(f.read())))

# COMMAND ----------


