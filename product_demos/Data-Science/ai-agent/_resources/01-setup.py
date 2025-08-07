# Databricks notebook source
import sys
major, minor = sys.version_info[:2]
assert (major, minor) >= (3, 11), f"This demo expect python version 3.11, but found {major}.{minor}. \nUse DBR15.4 or above. \nIf you're on serverless compute, open the 'Environment' menu on the right of your notebook, set it to >=2 and apply."

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

for f in dbutils.fs.ls(volume_folder):
  dbutils.fs.rm(f.path, True)  
#dbutils.fs.rm(volume_folder, True)

# COMMAND ----------

# MAGIC %md 
# MAGIC To speedup the demo, we won't generate the data but download it directly. IF it fails you can run the generation notebooks in the _resource folders instead.

# COMMAND ----------

data_exists = False
try:
  dbutils.fs.ls(volume_folder)
  dbutils.fs.ls(volume_folder+"/customers")
  dbutils.fs.ls(volume_folder+"/subscriptions")
  dbutils.fs.ls(volume_folder+"/billing")
  dbutils.fs.ls(volume_folder+"/eval_dataset")
  dbutils.fs.ls(volume_folder+"/pdf_documentation")
  data_exists = True
  print("data already exists")
except Exception as e:
  print(f"folder doesn't exists, generating the data...")


if not data_exists:
    try:
        DBDemos.download_file_from_git(volume_folder+'/customers', "databricks-demos", "dbdemos-dataset", "/llm/ai-agent/customers")
        DBDemos.download_file_from_git(volume_folder+'/subscriptions', "databricks-demos", "dbdemos-dataset", "/llm/ai-agent/subscriptions")
        DBDemos.download_file_from_git(volume_folder+'/billing', "databricks-demos", "dbdemos-dataset", "/llm/ai-agent/billing")
        DBDemos.download_file_from_git(volume_folder+'/eval_dataset', "databricks-demos", "dbdemos-dataset", "/llm/ai-agent/eval_dataset")
        DBDemos.download_file_from_git(volume_folder+'/pdf_documentation', "databricks-demos", "dbdemos-dataset", "/llm/ai-agent/pdf_documentation")
        data_downloaded = True
    except Exception as e: 
        print(f"Error trying to download the file from the repo: {str(e)}. Will generate the data instead...")    
else:
    data_downloaded = True

# COMMAND ----------

# MAGIC %sql drop table customers

# COMMAND ----------

if not spark.catalog.tableExists("customers") or \
    not spark.catalog.tableExists("subscriptions") or \
    not spark.catalog.tableExists("billing") :
        spark.read.parquet(f"{volume_folder}/customers").write.mode('overwrite').saveAsTable('customers')
        spark.read.parquet(f"{volume_folder}/subscriptions").write.mode('overwrite').saveAsTable('subscriptions')
        spark.read.parquet(f"{volume_folder}/billing").write.mode('overwrite').saveAsTable('billing')


# COMMAND ----------

#helper function defined to log the model and wrap
def log_customer_support_agent_model(resources, request_example):
    import mlflow
    import mlflow.models
    from mlflow.types.responses import ResponsesAgentRequest
    model_config = mlflow.models.ModelConfig(development_config="../02_agent_eval/agent_config.yaml")
    with mlflow.start_run(run_name=model_config.get('config_version_name')):
        return mlflow.pyfunc.log_model(
            name="agent",
            python_model="../02_agent_eval/agent.py",
            model_config="../02_agent_eval/agent_config.yaml",
            input_example=ResponsesAgentRequest(input=[{"role": "user", "content": request_example}]),
            resources=resources, # Determine Databricks resources (endpoints, fonctions, vs...) to specify for automatic auth passthrough at deployment time
            extra_pip_requirements=["databricks-connect"]
        )

def predict_wrapper(question):
    # Format for chat-style models
    model_input = pd.DataFrame({
        "input": [[{"role": "user", "content": question}]]
    })
    response = loaded_model.predict(model_input)
    return response['output'][-1]['content'][-1]['text']



def get_scorers():
    from mlflow.genai.scorers import RetrievalGroundedness, RelevanceToQuery, Safety, Guidelines
    return [
        RetrievalGroundedness(),  # Checks if email content is grounded in retrieved data
        RelevanceToQuery(),  # Checks if email addresses the user's request
        Safety(),  # Checks for harmful or inappropriate content
        Guidelines(
            guidelines="""Reponse must be done without showing reaso
            ning.
            - don't mention that you need to look up things
            - do not mention tools or function used
            - do not tell your intermediate steps or reasoning""",
            name="steps_and_reasoning",
        )
    ]


# COMMAND ----------

import time

def endpoint_exists(vsc, vs_endpoint_name):
  try:
    return vs_endpoint_name in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]
  except Exception as e:
    #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
    if "REQUEST_LIMIT_EXCEEDED" in str(e):
      print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. The demo will consider it exists")
      return True
    else:
      raise e

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
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

def index_exists(vsc, endpoint_name, index_full_name):
    try:
        vsc.get_index(endpoint_name, index_full_name).describe()
        return True
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

def wait_for_model_serving_endpoint_to_be_ready(ep_name):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate
    import time

    # TODO make the endpoint name as a param
    # Wait for it to be ready
    w = WorkspaceClient()
    state = ""
    for i in range(200):
        state = w.serving_endpoints.get(ep_name).state
        if state.config_update == EndpointStateConfigUpdate.IN_PROGRESS:
            if i % 40 == 0:
                print(f"Waiting for endpoint to deploy {ep_name}. Current state: {state}")
            time.sleep(10)
        elif state.ready == EndpointStateReady.READY:
          print('endpoint ready.')
          return
        else:
          break
    raise Exception(f"Couldn't start the endpoint, timeout, please check your endpoint for more details: {state}")
