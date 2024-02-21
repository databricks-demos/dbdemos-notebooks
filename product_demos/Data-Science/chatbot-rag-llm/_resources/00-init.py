# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install mlflow==2.10.1 lxml==4.9.3 transformers==4.30.2 langchain==0.1.5 databricks-vectorsearch==0.22
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------


dbutils.widgets.text("reset_all_data", "false", "Reset Data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, length, pandas_udf
import os
import mlflow
from typing import Iterator
from mlflow import MlflowClient

# COMMAND ----------

import re
min_required_version = "11.3"
version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
version_search = re.search('^([0-9]*\.[0-9]*)', version_tag)
assert version_search, f"The Databricks version can't be extracted from {version_tag}, shouldn't happen, please correct the regex"
current_version = float(version_search.group(1))
assert float(current_version) >= float(min_required_version), f'The Databricks version of the cluster must be >= {min_required_version}. Current version detected: {current_version}'

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  #You can programatically get a PAT token with the following
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  xp_root_path = f"/Shared/dbdemos/experiments/{demo_name}"
  try:
    r = w.workspace.mkdirs(path=xp_root_path)
  except Exception as e:
    print(f"ERROR: couldn't create a folder for the experiment under {xp_root_path} - please create the folder manually or  skip this init (used for job only: {e})")
    raise e
  xp = f"{xp_root_path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)

# COMMAND ----------

if reset_all_data:
  print(f'clearing up db {dbName}')
  spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")

# COMMAND ----------

def use_and_create_db(catalog, dbName, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  spark.sql(f"USE CATALOG `{catalog}`")
  spark.sql(f"""create database if not exists `{dbName}` """)

assert catalog not in ['hive_metastore', 'spark_catalog']
#If the catalog is defined, we force it to the given value and throw exception if not.
if len(catalog) > 0:
  current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
  if current_catalog != catalog:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs:
      spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
      if catalog == 'dbdemos':
        spark.sql(f"ALTER CATALOG {catalog} OWNER TO `account users`")
  use_and_create_db(catalog, dbName)

if catalog == 'dbdemos':
  try:
    spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{dbName} TO `account users`")
    spark.sql(f"ALTER SCHEMA {catalog}.{dbName} OWNER TO `account users`")
  except Exception as e:
    print("Couldn't grant access to the schema to all users:"+str(e))    

print(f"using catalog.database `{catalog}`.`{dbName}`")
spark.sql(f"""USE `{catalog}`.`{dbName}`""")    

# COMMAND ----------

# DBTITLE 1,Optional: Allowing Model Serving IPs
#If your workspace has ip access list, you need to allow your model serving endpoint to hit your AI gateway. Based on your region, IPs might change. Please reach out your Databrics Account team for more details.

# def allow_serverless_ip():
#   base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
#   headers = {"Authorization": f"Bearer {<Your PAT Token>}", "Content-Type": "application/json"}
#   return requests.post(f"{base_url}/api/2.0/ip-access-lists", json={"label": "serverless-model-serving", "list_type": "ALLOW", "ip_addresses": ["<IP RANGE>"], "enabled": "true"}, headers = headers).json()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Helpers to get catalog and index status:

# COMMAND ----------

# Helper function
def get_latest_model_version(model_name):
    mlflow_client = MlflowClient()
    latest_version = 1
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

# COMMAND ----------

# DBTITLE 1,endpoint
import time
def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

# DBTITLE 1,index
def index_exists(vsc, endpoint_name, index_full_name):
    try:
        dict_vsindex = vsc.get_index(endpoint_name, index_full_name).describe()
        return dict_vsindex.get('status').get('ready', False)
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False
    
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import StringType
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
#Add retries with backoff to avoid 429 while fetching the doc
retries = Retry(
    total=3,
    backoff_factor=3,
    status_forcelist=[429],
)

def download_databricks_documentation_articles(max_documents=None):
    # Fetch the XML content from sitemap
    response = requests.get(DATABRICKS_SITEMAP_URL)
    root = ET.fromstring(response.content)

    # Find all 'loc' elements (URLs) in the XML
    urls = [loc.text for loc in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
    if max_documents:
        urls = urls[:max_documents]

    # Create DataFrame from URLs
    df_urls = spark.createDataFrame(urls, StringType()).toDF("url").repartition(10)

    # Pandas UDF to fetch HTML content for a batch of URLs
    @pandas_udf("string")
    def fetch_html_udf(urls: pd.Series) -> pd.Series:
        adapter = HTTPAdapter(max_retries=retries)
        http = requests.Session()
        http.mount("http://", adapter)
        http.mount("https://", adapter)
        def fetch_html(url):
            try:
                response = http.get(url)
                if response.status_code == 200:
                    return response.content
            except requests.RequestException:
                return None
            return None

        with ThreadPoolExecutor(max_workers=200) as executor:
            results = list(executor.map(fetch_html, urls))
        return pd.Series(results)

    # Pandas UDF to process HTML content and extract text
    @pandas_udf("string")
    def download_web_page_udf(html_contents: pd.Series) -> pd.Series:
        def extract_text(html_content):
            if html_content:
                soup = BeautifulSoup(html_content, "html.parser")
                article_div = soup.find("div", itemprop="articleBody")
                if article_div:
                    return str(article_div).strip()
            return None

        return html_contents.apply(extract_text)

    # Apply UDFs to DataFrame
    df_with_html = df_urls.withColumn("html_content", fetch_html_udf("url"))
    final_df = df_with_html.withColumn("text", download_web_page_udf("html_content"))

    # Select and filter non-null results
    final_df = final_df.select("url", "text").filter("text IS NOT NULL").cache()
    if final_df.isEmpty():
      raise Exception("Dataframe is empty, couldn't download Databricks documentation, please check sitemap status.")

    return final_df

# COMMAND ----------

def display_gradio_app(space_name = "databricks-demos-chatbot"):
    displayHTML(f'''<div style="margin: auto; width: 1000px"><iframe src="https://{space_name}.hf.space" frameborder="0" width="1000" height="950" style="margin: auto"></iframe></div>''')

# COMMAND ----------

# DBTITLE 1,Cleanup utility to remove demo assets
def cleanup_demo(catalog, db, serving_endpoint_name, vs_index_fullname):
  vsc = VectorSearchClient()
  try:
    vsc.delete_index(endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME, index_name=vs_index_fullname)
  except Exception as e:
    print(f"can't delete index {VECTOR_SEARCH_ENDPOINT_NAME} {vs_index_fullname} - might not be existing: {e}")
  try:
    WorkspaceClient().serving_endpoints.delete(serving_endpoint_name)
  except Exception as e:
    print(f"can't delete serving endpoint {serving_endpoint_name} - might not be existing: {e}")
  spark.sql(f'DROP SCHEMA `{catalog}`.`{db}` CASCADE')

# COMMAND ----------

# DBTITLE 1,Demo helper to debug permission issue
def test_demo_permissions(host, secret_scope, secret_key, vs_endpoint_name, index_name, embedding_endpoint_name = None, managed_embeddings = True):
  error = False
  CSS_REPORT = """
  <style>
  .dbdemos_install{
                      font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;
  color: #3b3b3b;
  box-shadow: 0 .15rem 1.15rem 0 rgba(58,59,69,.15)!important;
  padding: 10px 20px 20px 20px;
  margin: 10px;
  font-size: 14px !important;
  }
  .dbdemos_block{
      display: block !important;
      width: 900px;
  }
  .code {
      padding: 5px;
      border: 1px solid #e4e4e4;
      font-family: monospace;
      background-color: #f5f5f5;
      margin: 5px 0px 0px 0px;
      display: inline;
  }
  </style>"""

  def display_error(title, error, color=""):
    displayHTML(f"""{CSS_REPORT}
      <div class="dbdemos_install">
                          <h1 style="color: #eb0707">Configuration error: {title}</h1> 
                            {error}
                        </div>""")
  
  def get_email():
    try:
      return spark.sql('select current_user() as user').collect()[0]['user']
    except:
      return 'Uknown'

  def get_token_error(msg, e):
    return f"""
    {msg}<br/><br/>
    Your model will be served using Databrick Serverless endpoint and needs a Pat Token to authenticate.<br/>
    <strong> This must be saved as a secret to be accessible when the model is deployed.</strong><br/><br/>
    Here is how you can add the Pat Token as a secret available within your notebook and for the model:
    <ul>
    <li>
      first, setup the Databricks CLI on your laptop or using this cluster terminal:
      <div class="code dbdemos_block">pip install databricks-cli</div>
    </li>
    <li> 
      Configure the CLI. You'll need your workspace URL and a PAT token from your profile page
      <div class="code dbdemos_block">databricks configure</div>
    </li>  
    <li>
      Create the dbdemos scope:
      <div class="code dbdemos_block">databricks secrets create-scope dbdemos</div>
    <li>
      Save your service principal secret. It will be used by the Model Endpoint to autenticate. <br/>
      If this is a demo/test, you can use one of your PAT token.
      <div class="code dbdemos_block">databricks secrets put-secret dbdemos rag_sp_token</div>
    </li>
    <li>
      Optional - if someone else created the scope, make sure they give you read access to the secret:
      <div class="code dbdemos_block">databricks secrets put-acl dbdemos '{get_email()}' READ</div>

    </li>  
    </ul>  
    <br/>
    Detailed error trying to access the secret:
      <div class="code dbdemos_block">{e}</div>"""

  try:
    secret = dbutils.secrets.get(secret_scope, secret_key)
    secret_principal = "__UNKNOWN__"
    try:
      from databricks.sdk import WorkspaceClient
      w = WorkspaceClient(token=dbutils.secrets.get(secret_scope, secret_key), host=host)
      secret_principal = w.current_user.me().emails[0].value
    except Exception as e_sp:
      error = True
      display_error(f"Couldn't get the SP identity using the Pat Token saved in your secret", 
                    get_token_error(f"<strong>This likely means that the Pat Token saved in your secret {secret_scope}/{secret_key} is incorrect or expired. Consider replacing it.</strong>", e_sp))
      return
  except Exception as e:
    error = True
    display_error(f"We couldn't access the Pat Token saved in the secret {secret_scope}/{secret_key}", 
                  get_token_error("<strong>This likely means your secret isn't set or not accessible for your user</strong>.", e))
    return
  
  try:
    from databricks.vector_search.client import VectorSearchClient
    vsc = VectorSearchClient(workspace_url=host, personal_access_token=secret, disable_notice=True)
    vs_index = vsc.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME, index_name=index_name)
    if embedding_endpoint_name:
      if managed_embeddings:
        from langchain_community.embeddings import DatabricksEmbeddings
        results = vs_index.similarity_search(query_text='What is Apache Spark?', columns=["content"], num_results=1)
      else:
        from langchain_community.embeddings import DatabricksEmbeddings
        embedding_model = DatabricksEmbeddings(endpoint=embedding_endpoint_name)
        embeddings = embedding_model.embed_query('What is Apache Spark?')
        results = vs_index.similarity_search(query_vector=embeddings, columns=["content"], num_results=1)

  except Exception as e:
    error = True
    vs_error = f"""
    Why are we getting this error?<br/>
    The model is using the Pat Token saved with the secret {secret_scope}/{secret_key} to access your vector search index '{index_name}' (host:{host}).<br/><br/>
    To do so, the principal owning the Pat Token must have USAGE permission on your schema and READ permission on the index.<br/>
    The principal is the one who generated the token you saved as secret: `{secret_principal}`. <br/>
    <i>Note: Production-grade deployement should to use a Service Principal ID instead.</i><br/>
    <br/>
    Here is how you can fix it:<br/><br/>
    <strong>Make sure your Service Principal has USE privileve on the schema</strong>:
    <div class="code dbdemos_block">
    spark.sql('GRANT USAGE ON CATALOG `{catalog}` TO `{secret_principal}>`');<br/>
    spark.sql('GRANT USAGE ON DATABASE `{catalog}`.`{db}` TO `{secret_principal}`');<br/>
    </div>
    <br/>
    <strong>Grant SELECT access to your SP to your index:</strong>
    <div class="code dbdemos_block">
    from databricks.sdk import WorkspaceClient<br/>
    import databricks.sdk.service.catalog as c<br/>
    WorkspaceClient().grants.update(c.SecurableType.TABLE, "{index_name}",<br/>
                                            changes=[c.PermissionsChange(add=[c.Privilege["SELECT"]], principal="{secret_principal}")])
    </div>
    <br/>
    <strong>If this is still not working, make sure the value saved in your {secret_scope}/{secret_key} secret is your SP pat token </strong>.<br/>
    <i>Note: if you're using a shared demo workspace, please do not change the secret value if was set to a valid SP value by your admins.</i>

    <br/>
    <br/>
    Detailed error trying to access the endpoint:
    <div class="code dbdemos_block">{str(e)}</div>
    </div>
    """
    if "403" in str(e):
      display_error(f"Permission error on Vector Search index {index_name} using the endpoint {vs_endpoint_name} and secret {secret_scope}/{secret_key}", vs_error)
    else:
      display_error(f"Unkown error accessing the Vector Search index {index_name} using the endpoint {vs_endpoint_name} and secret {secret_scope}/{secret_key}", vs_error)
  def get_wid():
    try:
      return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('orgId')
    except:
      return None
  if get_wid() in ["5206439413157315", "984752964297111", "1444828305810485", "2556758628403379"]:
    print(f"----------------------------\nYou are in a Shared FE workspace. Please don't override the secret value (it's set to the SP `{secret_principal}`).\n---------------------------")

  if not error:
    print('Secret and permissions seems to be properly setup, you can continue the demo!')

# COMMAND ----------

def pprint(obj):
  import pprint
  pprint.pprint(obj, compact=True, indent=1, width=100)

# COMMAND ----------

#Temp workaround to test if a table exists in shared cluster mode in DBR 14.2 (see SASP-2467)
def table_exists(table_name):
    try:
        spark.table(table_name).isEmpty()
    except:
        return False
    return True
