# Databricks notebook source
# MAGIC %md
# MAGIC ## API helpers for MLOps operation
# MAGIC 
# MAGIC This notebook contains function to simplify MLOps operation during the demo, such as creating the MLOPs job if it doesn't exists, or webhook helpers

# COMMAND ----------

import mlflow
from mlflow.utils.rest_utils import http_request
import json

# COMMAND ----------

#Helper to get the MLOps Databricks job or create it if it doesn't exists
def find_job(name, offset = 0, limit = 25):
    r = http_request(host_creds=host_creds, endpoint="/api/2.1/jobs/list", method="GET", params={"limit": limit, "offset": offset, "name": urllib.parse.quote_plus(name)}).json()
    if 'jobs' in r:
        for job in r['jobs']:
            if job["settings"]["name"] == name:
                return job
        if r['has_more']:
            return find_job(name, offset+limit, limit)
    return None

def get_churn_staging_job_id():
  job = find_job("demos_churn_model_staging_validation")
  if job is not None:
    return job['job_id']
  else:
    #the job doesn't exist, we dynamically create it.
    #Note: requires DBR 10 ML to use automl model
    notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = '/'.join(notebook_path.split('/')[:-1])
    cloud_name = get_cloud_name()
    if cloud_name == "aws":
      node_type = "i3.xlarge"
    elif cloud_name == "azure":
      node_type = "Standard_DS3_v2"
    elif cloud_name == "gcp":
      node_type = "n1-standard-4"
    else:
      raise Exception(f"Cloud '{cloud_name}' isn't supported!")
    job_settings = {
                  "email_notifications": {},
                  "name": "demos_churn_model_staging_validation",
                  "max_concurrent_runs": 1,
                  "tasks": [
                      {
                          "new_cluster": {
                              "spark_version": "12.2.x-cpu-ml-scala2.12",
                              "spark_conf": {
                                  "spark.databricks.cluster.profile": "singleNode",
                                  "spark.master": "local[*, 4]"
                              },
                              "num_workers": 0,
                              "node_type_id": node_type,
                              "driver_node_type_id": node_type,
                              "custom_tags": {
                                  "ResourceClass": "SingleNode"
                              },
                              "spark_env_vars": {
                                  "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                              },
                              "enable_elastic_disk": True
                          },
                          "notebook_task": {
                              "notebook_path": f"{base_path}/05_job_staging_validation"
                          },
                          "email_notifications": {},
                          "task_key": "test-model"
                      }
                  ]
          }
    print("Job doesn't exists, creating it...")
    r = http_request(host_creds=host_creds, endpoint="/api/2.1/jobs/create", method="POST", json=job_settings).json()
    return r['job_id']

# COMMAND ----------

# DBTITLE 1,Helpers to manage Model registry webhooks
# Manage webhooks
try:
  from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec, HttpUrlSpec
  def create_job_webhook(model_name, job_id):
    return RegistryWebhooksClient().create_webhook(
      model_name = model_name,
      events = ["TRANSITION_REQUEST_CREATED"],
      job_spec = JobSpec(job_id=job_id, access_token=token),
      description = "Trigger the ops_validation job when a model is requested to move to staging.",
      status = "ACTIVE")

  def create_notification_webhook(model_name, slack_url):
    from databricks_registry_webhooks import RegistryWebhooksClient, JobSpec, HttpUrlSpec
    return RegistryWebhooksClient().create_webhook(
      model_name = model_name,
      events = ["TRANSITION_REQUEST_CREATED"],
      description = "Notify the MLOps team that a model is requested to move to staging.",
      status = "ACTIVE",
      http_url_spec = HttpUrlSpec(url=slack_url))

  # List
  def list_webhooks(model_name):
    from databricks_registry_webhooks import RegistryWebhooksClient
    return RegistryWebhooksClient().list_webhooks(model_name = model_name)

  # Delete
  def delete_webhooks(webhook_id):
    from databricks_registry_webhooks import RegistryWebhooksClient
    return RegistryWebhooksClient().delete_webhook(id=webhook_id)

except:
  def raise_exception():
    print("You need to install databricks-registry-webhooks library to easily perform this operation (you could also use the rest API directly).")
    print("Please run: %pip install databricks-registry-webhooks ")
    raise RuntimeError("function not available without databricks-registry-webhooks.")

  def create_job_webhook(model_name, job_id):
    raise_exception()
  def create_notification_webhook(model_name, slack_url):
    raise_exception()
  def list_webhooks(model_name):
    raise_exception()
  def delete_webhooks(webhook_id):
    raise_exception()
    
def reset_webhooks(model_name):
  whs = list_webhooks(model_name)
  for wh in whs:
    delete_webhooks(wh.id)

# COMMAND ----------

# DBTITLE 1,Slack notification helper
# Slack Notifications
#Webhooks can be used to send emails, Slack messages, and more.  In this case we #use Slack.  We also use `dbutils.secrets` to not expose any tokens, but the URL #looks more or less like this:
#`https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`
#You can read more about Slack webhooks [here](https://api.slack.com/messaging/webhooks#create_a_webhook).
import urllib 
import json 
import requests, json

def send_notification(message):
  try:
    slack_webhook = dbutils.secrets.get("rk_webhooks", "slack")
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
    print("slack isn't properly setup in this workspace.")
    pass
  displayHTML(f"""<div style="border-radius: 10px; background-color: #adeaff; padding: 10px; width: 400px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 3px">
        <div style="padding-bottom: 5px"><img style="width:20px; margin-bottom: -3px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/bell.png"/> <strong>Churn Model update</strong></div>
        {message}
        </div>""")    

# COMMAND ----------

# DBTITLE 1,Transition model stage & write model comment helper
client = mlflow.tracking.client.MlflowClient()

host_creds = client._tracking_client.store.get_host_creds()
host = host_creds.host
token = host_creds.token

def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()


# Request transition to staging
def request_transition(model_name, version, stage):
  
  staging_request = {'name': model_name,
                     'version': version,
                     'stage': stage,
                     'archive_existing_versions': 'true'}
  response = mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(staging_request))
  return(response)
  
  
# Comment on model
def model_comment(model_name, version, comment):
  
  comment_body = {'name': model_name,
                  'version': version, 
                  'comment': comment}
  response = mlflow_call_endpoint('comments/create', 'POST', json.dumps(comment_body))
  return(response)

# Accept or reject transition request
def accept_transition(model_name, version, stage, comment):
  approve_request_body = {'name': model_details.name,
                          'version': model_details.version,
                          'stage': stage,
                          'archive_existing_versions': 'true',
                          'comment': comment}
  
  mlflow_call_endpoint('transition-requests/approve', 'POST', json.dumps(approve_request_body))

def reject_transition(model_name, version, stage, comment):
  
  reject_request_body = {'name': model_details.name, 
                         'version': model_details.version, 
                         'stage': stage, 
                         'comment': comment}
  
  mlflow_call_endpoint('transition-requests/reject', 'POST', json.dumps(reject_request_body))

# COMMAND ----------

# After receiving payload from webhooks, use MLflow client to retrieve model details and lineage
def fetch_webhook_data(): 
  try:
    registry_event = json.loads(dbutils.widgets.get('event_message'))
    model_name = registry_event['model_name']
    model_version = registry_event['version']
    if 'to_stage' in registry_event and registry_event['to_stage'] != 'Staging':
      dbutils.notebook.exit()
  except:
    #If it's not in a job but interactive demo, we get the last version from the registry
    model_name = 'dbdemos_mlops_churn'
    model_version = client.get_latest_versions(model_name, ['None'])[0].version
  return(model_name, model_version)

# COMMAND ----------


