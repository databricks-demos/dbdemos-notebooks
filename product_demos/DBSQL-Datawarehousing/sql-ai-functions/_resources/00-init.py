# Databricks notebook source
# MAGIC %md
# MAGIC # init notebook setting up the backend
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fsql_ai_functions%2Finit&dt=DBSQL">

# COMMAND ----------

dbutils.widgets.text("catalog", "dbdemos", "Catalog")
dbutils.widgets.text("db", "openai_demo", "Database")

import pyspark.sql.functions as F
import re
from pyspark.sql.functions import col, udf, length, pandas_udf

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=false $catalog=$catalog $db=$db

# COMMAND ----------

import requests
import time
import re

#Helper to send REST queries. This will try to use an existing warehouse or create a new one.
class SQLStatementAPI:
    def __init__(self, warehouse_name = "dbdemos-shared-endpoint", catalog = "dbdemos", schema = "openai_demo"):
        self.base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
        username = re.sub("[^A-Za-z0-9]", '_', username)
        warehouse = self.get_or_create_endpoint(username, warehouse_name)
        #Try to create it
        if warehouse is None:
          raise Exception(f"Couldn't find or create a warehouse named {warehouse_name}. If you don't have warehouse creation permission, please change the name to an existing one or ask an admin to create the warehouse with this name.")
        self.warehouse_id = warehouse['warehouse_id']
        self.catalog = catalog
        self.schema = schema
        self.wait_timeout = "50s"

    def get_or_create_endpoint(self, username, endpoint_name):
        ds = self.get_demo_datasource(endpoint_name)
        if ds is not None:
            return ds
        def get_definition(serverless, name):
            return {
                "name": name,
                "cluster_size": "Small",
                "min_num_clusters": 1,
                "max_num_clusters": 1,
                "tags": {
                    "project": "dbdemos"
                },
                "warehouse_type": "PRO",
                "spot_instance_policy": "COST_OPTIMIZED",
                "enable_photon": "true",
                "enable_serverless_compute": serverless,
                "channel": { "name": "CHANNEL_NAME_CURRENT" }
            }
        def try_create_endpoint(serverless):
            w = self._post("api/2.0/sql/warehouses", get_definition(serverless, endpoint_name))
            if "message" in w and "already exists" in w['message']:
                w = self._post("api/2.0/sql/warehouses", get_definition(serverless, endpoint_name+"-"+username))
            if "id" in w:
                return w
            print(f"WARN: Couldn't create endpoint with serverless = {endpoint_name} and endpoint name: {endpoint_name} and {endpoint_name}-{username}. Creation response: {w}")
            return None

        if try_create_endpoint(True) is None:
            #Try to fallback with classic endpoint?
            try_create_endpoint(False)
        ds = self.get_demo_datasource(endpoint_name)
        if ds is not None:
            return ds
        print(f"ERROR: Couldn't create endpoint.")
        return None      
      
    def get_demo_datasource(self, datasource_name):
        data_sources = self._get("api/2.0/preview/sql/data_sources")
        for source in data_sources:
            if source['name'] == datasource_name:
                return source
        """
        #Try to fallback to an existing shared endpoint.
        for source in data_sources:
            if datasource_name in source['name'].lower():
                return source
        for source in data_sources:
            if "dbdemos-shared-endpoint" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared-sql-endpoint" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared" in source['name'].lower():
                return source"""
        return None
      
    def execute_sql(self, sql):
      x = self._post("api/2.0/sql/statements", {"statement": sql, "warehouse_id": self.warehouse_id, "catalog": self.catalog, "schema": self.schema, "wait_timeout": self.wait_timeout})
      return self.result_as_df(x, sql)
    
    def wait_for_statement(self, results, timeout = 600):
      sleep_time = 3
      i = 0
      while i < timeout:
        if results['status']['state'] not in ['PENDING', 'RUNNING']:
          return results
        time.sleep(sleep_time)
        i += sleep_time
        results = self._get(f"api/2.0/sql/statements/{results['statement_id']}")
      self._post(f"api/2.0/sql/statements/{results['statement_id']}/cancel")
      return self._get(f"api/2.0/sql/statements/{results['statement_id']}")
        
      
    def result_as_df(self, results, sql):
      results = self.wait_for_statement(results)
      if results['status']['state'] != 'SUCCEEDED':
        print(f"Query error: {results}")
        return pd.DataFrame([[results['status']['state'],{results['status']['error']['message']}, results]], columns = ['state', 'message', 'results'])
      if results["manifest"]['schema']['column_count'] == 0:
        return pd.DataFrame([[results['status']['state'], sql]], columns = ['state', 'sql'])
      cols = [c['name'] for c in results["manifest"]['schema']['columns']]
      results = results["result"]["data_array"] if "data_array" in results["result"] else []
      return pd.DataFrame(results, columns = cols)

    def _get(self, uri, data = {}, allow_error = False):
        r = requests.get(f"{self.base_url}/{uri}", params=data, headers=self.headers)
        return self._process(r, allow_error)

    def _post(self, uri, data = {}, allow_error = False):
        return self._process(requests.post(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _put(self, uri, data = {}, allow_error = False):
        return self._process(requests.put(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _delete(self, uri, data = {}, allow_error = False):
        return self._process(requests.delete(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _process(self, r, allow_error = False):
      if r.status_code == 500 or r.status_code == 403 or not allow_error:
        r.raise_for_status()
      return r.json()
    
#sql_api = SQLStatementAPI(warehouse_name = "dbdemos-shared-endpoint-test", catalog = "dbdemos", schema = "openai_demo")
#sql_api.execute_sql("select 'test'")

# COMMAND ----------

def display_answer(answer):
    displayHTML("""
  <style>
  .messages {
    margin-top: 30px;
    display: flex;
    flex-direction: column;
    font-family: helvetica;
    font-size: 15px;
    display: flex ;
    flex-direction: column;
    align-items: center;
  }

  .message {
    border-radius: 20px;
    padding: 8px 15px;
    margin-top: 5px;
    margin-bottom: 5px;
    display: inline-block;
  }

  .mine {
    align-items: flex-end;
  }

  .mine .message {
    color: white;
    background: linear-gradient(to bottom, #00acea 0%, #0b7ebf 100%);
    background-attachment: fixed;
    position: relative;
    margin-left: 25%;
    margin-right: 50px;
  }
  .mine .message.last:before {
    content: "";
    position: absolute;
    z-index: 0;
    bottom: 0;
    right: -8px;
    height: 20px;
    width: 20px;
    background: linear-gradient(to bottom, #00D0EA 0%, #0085D1 100%);
    background-attachment: fixed;
    border-bottom-left-radius: 15px;
  }

  .mine .message.last:after {
    content: "";
    position: absolute;
    z-index: 1;
    bottom: 0;
    right: -10px;
    width: 10px;
    height: 20px;
    background: white;
    border-bottom-left-radius: 10px;
  }


  </style>
    <div class="mine messages">
      <div class="message last">
        """+answer.replace("\n", "<br>")+"""
       </div>
    </div>
  </div>

  """)
