# Databricks notebook source
# MAGIC %md
# MAGIC ## API helpers for ModelOps actions
# MAGIC
# MAGIC This notebook contains function to simplify MLOps operation during the demo, such as creating the MLOPs job if it doesn't exists, or webhook helpers

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# w = WorkspaceClient() # databricks-sdk >= 0.9.0 or MLR > 14.0

# COMMAND ----------

# DBTITLE 1,If databricks-sdk version < 0.9.0/MLR<=14.0
# Get current token and url (not required if sdk version >=0.8.0 / MLR>14.0)
databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

w = WorkspaceClient(host=databricks_url, token=databricks_token)

# COMMAND ----------

import re
def get_current_username():
  """
  Helper function to get current username
  """
  current_user = w.current_user.me().user_name.split("@")[0]

  return re.sub(r'\W+', '_', current_user)

# COMMAND ----------

import mlflow
from mlflow.utils.rest_utils import http_request
import json
from mlflow import MlflowClient


client = MlflowClient()
host_creds = client._tracking_client.store.get_host_creds()

# COMMAND ----------

import urllib
from mlflow.utils.rest_utils import http_request

# Helper to get the MLOps Databricks job or create it if it doesn't exists
def find_job(name, offset = 0, limit = 25):
  # TO-DO [later]: use databricks-sdk jops api instead
  r = http_request(host_creds=host_creds, endpoint="/api/2.1/jobs/list", method="GET", params={"limit": limit, "offset": offset, "name": urllib.parse.quote_plus(name)}).json()
  if 'jobs' in r:
      for job in r['jobs']:
          if job["settings"]["name"] == name:
              return job
      if r['has_more']:
          return find_job(name, offset+limit, limit)
  return None

def trigger_job(job_id_in, job_params: dict = {}) :
  
  r = w.jobs.run_now(job_id=job_id_in, notebook_params=job_params)
  
  if r.run_id:
    print(f"Succesfully triggered job #{job_id_in} with run #{r.run_id}")
    return r.run_id

  else:
    print(f"Could not trigger job #{job_id_in}, check if job exists")
    return None
  
def get_churn_staging_job_id():
  job_name = "dbdemos_churn_model_staging_validation_"+get_current_username()
  job = find_job(job_name)

  if job is not None:
    print(f"Job {job_name} exists grabbing job ID#{job['job_id']}")
    return job['job_id']

  else:
    #the job doesn't exist, we dynamically create it.
    #Note: requires DBR 10 ML to use automl model
    print(f"Job doesn't exists, creating job with name {job_name}...")
    notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = '/'.join(notebook_path.split('/')[:-1])

    # Create job using databricks sdk
    from databricks.sdk.service.jobs import Task, NotebookTask
    
    # Get current cluster id to run job on [for demo/example purpose]
    for tag in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")):
      if tag['key'] == "ClusterId":
        this_cluster_id = tag['value']
        break
      else:
        this_cluster_id = None
        # TO-DO [OPTIONAL]: modify inputs to create job cluster using latest ml_runtime and automatic node_type

    # Create job and configure to run on current demo/interactive cluster
    job = w.jobs.create(
      name=job_name,
      tasks=[Task(
        task_key='test-challenger-model',
        notebook_task=NotebookTask(f"{base_path}/04_job_challenger_validation"),
        existing_cluster_id=this_cluster_id)],
    )            
    
    return job.job_id

# COMMAND ----------

# DBTITLE 1,Helper function to emulate a model transition request
# Request transition
def request_transition(model_name, version, stage="Challenger", validation_job_id=None):
  
  def send_transition_request_slack_notification(message: str):
    """
    Inner helper function to send slack notification mimic-ing mlflow models transition-requests webhooks
    """
    slack_webhook = dbutils.secrets.get("fieldeng", f"{current_user_no_at}_slack_webhook")

    body = {'text': message}
    response = requests.post(
      slack_webhook, data=json.dumps(body),
      headers={'Content-Type': 'application/json'})
  
  # Send slack notification [OPTIONAL to mimic transition requests webhook]
  send_transition_request_slack_notification(f"{current_user} requested that registered UC model {model_name} version {version} transition to {stage}")

  # Trigger model validation job
  if validation_job_id is None:
    validation_job_id = get_churn_staging_job_id()

  transition_request_params = {
    'model_name': model_name,
    'version': version,
    'to_stage': stage}

  return trigger_job(validation_job_id, transition_request_params)

# COMMAND ----------

# Set UC Model Registry as default
mlflow.set_registry_uri("databricks-uc")

def cleanup_registered_model(registry_model_name):
  """
  Utilty function to delete a registered model in MLflow model registry.
  To delete a model in the model registry, all model versions must first be archived.
  This function 
  (i) first archives all versions of a model in the registry
  (ii) then deletes the model 
  
  :param registry_model_name: (str) Name of model in MLflow Model Registry
  """

  filter_string = f"name='{registry_model_name}'"
  model_versions = client.search_model_versions(filter_string)

  if len(model_versions) > 0:
    print(f"Deleting model named {registry_model_name}...")
    client.delete_registered_model(registry_model_name)
    
  else:
    print(f"No registered model named {registry_model_name} to delete")

# COMMAND ----------

def get_latest_model_version(model_name):
  client = MlflowClient()
  model_version_infos = client.search_model_versions("name = '%s'" % model_name)
  return max([model_version_info.version for model_version_info in model_version_infos])

# COMMAND ----------

# DBTITLE 1,Slack notification helper
# Slack Notifications _(OPTIONAL)_

# Webhooks can be used to send emails, Slack messages, and more. In this case we #use Slack along `dbutils.secrets` to not expose any tokens, but the URL #looks more or less like this: `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`

# You can read more about Slack webhooks [here](https://api.slack.com/messaging/webhooks#create_a_webhook).

import urllib
import json
import requests, json

def send_notification(message: str, slack_webhook: str = ""):

  try:
    if not slack_webhook.strip():
      slack_webhook = dbutils.secrets.get("fieldeng", f"{current_user_no_at}_slack_webhook")

    body = {'text': message}
    response = requests.post(
      slack_webhook, data=json.dumps(body),
      headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
      raise ValueError(
          'Request to slack returned an error %s, the response is:\n%s'
          % (response.status_code, response.text)
      )
      
  except:
    print("No slack notification sent.")
    pass
  displayHTML(f"""<div style="border-radius: 10px; background-color: #adeaff; padding: 10px; width: 400px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 3px">
        <div style="padding-bottom: 5px"><img style="width:20px; margin-bottom: -3px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/bell.png"/> <strong>Churn Model update</strong></div>
        {message}
        </div>""")
