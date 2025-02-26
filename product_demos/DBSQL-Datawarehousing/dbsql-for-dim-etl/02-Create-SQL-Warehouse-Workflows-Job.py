# Databricks notebook source
# MAGIC %md
# MAGIC #View the Introduction notebook
# MAGIC
# MAGIC If not already viewed, browse through [01-Patient-Dimension-ETL-Introduction]($./01-Patient-Dimension-ETL-Introduction)

# COMMAND ----------

# MAGIC %md
# MAGIC #What will be create
# MAGIC 1. Serverless SQL Warehouse
# MAGIC  If **not previously created** (with name staring "dbsqldemo-patient-dimension-etl-")
# MAGIC 2. New Workflows Job to run the ETL demo
# MAGIC
# MAGIC ##Required Privileges
# MAGIC
# MAGIC To run the demo, the following privileges are required:
# MAGIC 1. Create Catalog, Schema, Table, Volume<br>
# MAGIC   Note that the privilege to Create Catalog and Schema is not required if using precreated objects. See [Configure notebook]($./00-Setup/Configure)
# MAGIC 2. Create SQL Warehouse, Workflows Job
# MAGIC
# MAGIC Additionally, the data files are downloaded via the internet.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize

# COMMAND ----------

import requests
from requests import Response
import time

# COMMAND ----------

def get_dbutils_tags_safe():
    import json
    return json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']

def get_current_url():
    try:
        return "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    except:
        try:
            return "https://"+get_dbutils_tags_safe()['browserHostName']
        except:
            return "local"


# COMMAND ----------

def get_current_pat_token():
    try:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    except Exception as e:
        raise Exception("Couldn't get a PAT Token: "+str(e))
    if len(token) == 0:
        raise Exception("Empty PAT Token.")
    return token

# COMMAND ----------

def get_json_result(url: str, r: Response, print_auth_error = True):
    if r.status_code == 403:
        if print_auth_error:
            print(f"Unauthorized call. Check your PAT token {r.text} - {r.url} - {url}")
    try:
        return r.json()
    except Exception as e:
        print(f"API CALL ERROR - can't read json. status: {r.status_code} {r.text} - URL: {url} - {e}")
        raise e


# COMMAND ----------

def post(url: str, headers: str, json: dict = {}, retry = 0):
    with requests.post(url, headers = headers, json=json, timeout=60) as r:
        #TODO: should add that in the requests lib adapter for all requests.
        if r.status_code == 429 and retry < 2:
            import time
            import random
            wait_time = 15 * (retry+1) + random.randint(2*retry, 10*retry)
            print(f'WARN: hitting api request limit 429 error: {url}. Sleeping {wait_time}sec and retrying...')
            time.sleep(wait_time)
            print('Retrying call.')
            return post(url, headers, json, retry+1)
        else:
            return get_json_result(url, r)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create SQL Warehouse
# MAGIC
# MAGIC If one doesn't exist for this demo

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

# Initialize the WorkspaceClient
w = WorkspaceClient()

all_wh = w.warehouses.list()
demo_wh_exist = [wh for wh in all_wh if wh.name.startswith("dbsqldemo-patient-dimension-etl-")]

if len(demo_wh_exist) > 0:
    warehouse_id = demo_wh_exist[0].id

    print(f"Using existing warehouse: {demo_wh_exist[0].name} with id: {warehouse_id}")
else:
    # create new
    wh_name = f'dbsqldemo-patient-dimension-etl-{time.time_ns()}'

    # Create SQL warehouse
    created = w.warehouses.create(
        name=wh_name,
        cluster_size="Small",
        max_num_clusters=1,
        auto_stop_mins=1,
        tags=sql.EndpointTags(
            custom_tags=[sql.EndpointTagPair(key="purpose", value="dbsqldemo")]
        )
    ).result()

    warehouse_id = created.id

    print(f"Created warehouse: {wh_name} with id: {warehouse_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Job
# MAGIC
# MAGIC Rough edge: warehouse_id parameter to NotebookTask not working at this time.  Hence using API. 

# COMMAND ----------

nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
nb_dir = "/Workspace" + "/".join(nb_path.split("/")[:-1]) + "/"

task_setup_catalog_schema = {
    "task_key": "SETUP_CATALOG",
    "notebook_task": {
      "notebook_path": nb_dir + "00-Setup/Setup",
      "warehouse_id": warehouse_id
    }
}

task_create_code_table = {
    "task_key": "CREATE_TABLE_CODE",
    "depends_on": [
      {
        "task_key": "SETUP_CATALOG"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "01-Create/Code Table",
      "warehouse_id": warehouse_id
    }
}

task_create_config_table = {
    "task_key": "CREATE_TABLE_CONFIG",
    "depends_on": [
      {
        "task_key": "SETUP_CATALOG"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "01-Create/ETL Log Table",
      "warehouse_id": warehouse_id
    }
}

task_create_patient_tables = {
    "task_key": "CREATE_TABLE_PATIENT",
    "depends_on": [
      {
        "task_key": "SETUP_CATALOG"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "01-Create/Patient Tables",
      "warehouse_id": warehouse_id
    }
}

task_stage_source_file_initial = {
    "task_key": "demo_StgSrcFileInit",
    "depends_on": [
      {
        "task_key": "SETUP_CATALOG"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "_util/stage source file - initial load",
    }
}

task_patient_initial_load = {
    "task_key": "INITIAL_LOAD_PATIENT",
    "description": "Initial load of Patient Tables",
    "depends_on": [
      {
        "task_key": "CREATE_TABLE_PATIENT"
      },
      {
        "task_key": "CREATE_TABLE_CODE"
      },
      {
        "task_key": "CREATE_TABLE_CONFIG"
      },
      {
        "task_key": "demo_StgSrcFileInit"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "02-Populate/Patient Dimension ETL",
      "base_parameters": {
        "p_process_id": "{{job.id}}-{{job.run_id}}"
      },
      "warehouse_id": warehouse_id
    },
}

task_browse_result_initial_load = {
    "task_key": "demo_BrowseResultInit",
    "description": "Browse results of the initial load",
    "depends_on": [
      {
        "task_key": "INITIAL_LOAD_PATIENT"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "_util/browse current load",
      "warehouse_id": warehouse_id
    }
}

task_stage_source_file_incr_1 = {
    "task_key": "demo_StgSrcFileIncr",
    "depends_on": [
      {
        "task_key": "INITIAL_LOAD_PATIENT"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "_util/stage source file - incremental load #1",
    }
}

task_patient_load_incr1 = {
    "task_key": "INCREMENTAL_LOAD_PATIENT",
    "depends_on": [
      {
        "task_key": "demo_StgSrcFileIncr"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "02-Populate/Patient Dimension ETL",
      "base_parameters": {
        "p_process_id": "{{job.id}}-{{job.run_id}}"
      },
      "warehouse_id": warehouse_id
    }
}

task_browse_result_incr1 = {
    "task_key": "demo_BrowseResultIncr",
    "depends_on": [
      {
        "task_key": "INCREMENTAL_LOAD_PATIENT"
      }
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": nb_dir + "_util/browse current load",
      "warehouse_id": warehouse_id
    }
}

tasks = [task_setup_catalog_schema, task_create_code_table, task_create_config_table, task_create_patient_tables, task_stage_source_file_initial, task_patient_initial_load, task_browse_result_initial_load, task_stage_source_file_incr_1, task_patient_load_incr1, task_browse_result_incr1]

json = {
    "name": f"dbsqldemo-patient-dimension-etl-{time.time_ns()}",
    "tasks": tasks,
    "format": "MULTI_TASK"
}


# COMMAND ----------

pat_token = get_current_pat_token()
headers = {"Authorization": "Bearer " + pat_token, 'Content-type': 'application/json', 'User-Agent': 'dbsqldemos'}
workspace_url = get_current_url()

job = post(workspace_url+"/api/2.2/jobs/create", headers, json)

print(job)

# COMMAND ----------

# MAGIC %md
# MAGIC #Access the ETL Job
# MAGIC
# MAGIC As mentioned in the Introduction, the JOB:
# MAGIC 1. Creates the config and patient tables (including the dimension table).
# MAGIC 2. Runs the ETL 3 times, to showcase the various features: a) Initial Load b) Incremental Load #1 c) Incremental Load #3.
# MAGIC 3. Includes the "browse_..." tasks after each load. Click on the task to view the results of the corresponding load.
# MAGIC
# MAGIC **Run the Job and view the results in each of the "browse ..." tasks**
# MAGIC

# COMMAND ----------

print(f"Demo workflow available: {get_current_url()}/#job/{job['job_id']}/tasks")