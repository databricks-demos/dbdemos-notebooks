# Databricks notebook source
# MAGIC %run "../00-Setup/Initialize"

# COMMAND ----------

# DBTITLE 1,config schema
# MAGIC %sql
# MAGIC select staging_path

# COMMAND ----------

res = _sqldf.first()

stg_path = res["staging_path"]

# COMMAND ----------

# DBTITLE 1,def function for staging process
def download_file(down_url, destination):
  import requests

  with requests.get(down_url, stream=True) as r:
    r.raise_for_status()
    with open(destination, 'wb') as f:
      for chunk in r.iter_content(chunk_size=8192): 
        # If you have chunk encoded response uncomment if
        # and set chunk_size parameter to None.
        #if chunk: 
        f.write(chunk)

# COMMAND ----------

def get_file(file_name, entity_name):
    import requests

    owner = "shyamraodb"
    repo = "star-schema-elt"
    path = "seed"

    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents/{path}').json()

    # download url for <file_name>
    down_url = [f['download_url'] for f in files if file_name in f['name']][0]
    destination = stg_path + "/" + entity_name + "/" + file_name

    download_file(down_url, destination)