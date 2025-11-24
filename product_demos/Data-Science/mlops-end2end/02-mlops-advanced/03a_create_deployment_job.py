# Databricks notebook source
# MAGIC %md
# MAGIC # Create MLflow3.0 Model Deployment Job
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-3a-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_feature_prep&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Feature engineering",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["feature store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define tasks and create programmatically
# MAGIC Running this notebook will create a Databricks Job templated as a MLflow 3.0 Deployment Job. This job will have three tasks: Evaluation, Approval_Check, and Deployment. The Evaluation task will evaluate the model on a dataset, the Approval_Check task will check if the model has been approved for deployment using UC Tags and the Approval button in the UC Model UI, and the Deployment task will deploy the feature to the Online Store and model to a serving endpoint.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/aws/en/assets/images/deployment-job-create-ui-c43b64da503e3f0babcb9ff81a78610d.png" width="1200">
# MAGIC
# MAGIC Template adapted from documentation ([AWS](https://docs.databricks.com/aws/mlflow/deployment-job#example-template-notebooks) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#example-template-notebooks) | [GCP](https://docs.databricks.com/gcp/mlflow/deployment-job#example-template-notebooks)) into your Databricks Workspace.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create a UC Model or use an existing one. For example, see the MLflow 3 examples ([AWS](https://docs.databricks.com/aws/mlflow/mlflow-3-install#example-notebooks) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/mlflow-3-install#example-notebooks) | [GCP](https://docs.databricks.com/gcp/mlflow/mlflow-3-install#example-notebooks)).
# MAGIC 2. Create the independent tasks as python notebooks in your workspace or repo.
# MAGIC 3. Create the main deployment DAG/job
# MAGIC 4. _OPTIONAL_ After running the notebook, the created job will not be connected to any UC Model. **You will still need to connect the job to a UC Model** in the UC Model UI as indicated in the documentation or using MLflow as shown in the final cell ([AWS](https://docs.databricks.com/aws/mlflow/deployment-job#connect) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#connect) | [GCP](https://docs.databricks.com/gcp/mlflow/deployment-job#connect)), in case the model does not create already.

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC mlflow>=3.3.0
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --quiet mlflow --upgrade
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point to Notebook tasks

# COMMAND ----------

import os


current_directory = os.getcwd()

# COMMAND ----------

# REQUIRED
model_name = f"{catalog}.{db}.advanced_mlops_churn" # The name of the already created UC Model
model_version = "1" # The version of the already created UC Model
job_name = "DBDemos-Model-Deployment-Job" # The desired name of the deployment job

# REQUIRED: Create notebooks for each task
evaluation_notebook_path = f"{current_directory}/04a_challenger_validation"
approval_notebook_path = f"{current_directory}/04b_challenger_approval" # OPTIONNAL only if Human in the Loop is required
deployment_notebook_path = f"{current_directory}/06_serve_features_and_model"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakeflow Job
# MAGIC **NOTE:**
# MAGIC - Adding a `smoke_test` flag to `True` for not deploying/serving the model for now (for demo/test purposes).

# COMMAND ----------

# Create job with necessary configuration to connect to model as deployment job
from databricks.sdk.service import jobs


job_settings = jobs.JobSettings(
    name=job_name,
    tasks=[
        jobs.Task(
            task_key="Evaluation",
            notebook_task=jobs.NotebookTask(notebook_path=evaluation_notebook_path),
            max_retries=0,
        ),
        jobs.Task(
            task_key="Approval_Check",
            notebook_task=jobs.NotebookTask(
                notebook_path=approval_notebook_path,
                base_parameters={"approval_tag_name": "{{task.name}}"}
            ),
            depends_on=[jobs.TaskDependency(task_key="Evaluation")],
            max_retries=0,
        ),
        jobs.Task(
            task_key="Deployment",
            notebook_task=jobs.NotebookTask(
                notebook_path=deployment_notebook_path,
                base_parameters={"smoke_test": False},
            ),
            depends_on=[jobs.TaskDependency(task_key="Approval_Check")],
            max_retries=0,
        ),
    ],
    parameters=[
        jobs.JobParameter(name="model_name", default=model_name),
        jobs.JobParameter(name="model_version", default=model_version),
    ],
    queue=jobs.QueueSettings(enabled=True),
    max_concurrent_runs=1,
)

# COMMAND ----------

from databricks.sdk import WorkspaceClient


w = WorkspaceClient()

# Search for the job by name (in case it exists)
existing_jobs = w.jobs.list(name=job_name)
job_id = None
for created_job in existing_jobs:
  if created_job.settings.name == job_name and created_job.creator_user_name == current_user:
      job_id = created_job.job_id
      break

if job_id:
  # Update existing job
  print("Updating existing...")
  w.jobs.update(job_id=job_id, new_settings=job_settings)

else:
  # Create new job
  print("Creating new...")
  created_job = w.jobs.create(**job_settings.__dict__)
  job_id = created_job.job_id

print(f"Job ID: {job_id}")

# COMMAND ----------

# DBTITLE 1,ONE-TIME Operation
print("Use the job name " + job_name + " to connect the deployment job to the UC model " + model_name + " as indicated in the UC Model UI.")
print("\nFor your reference, the job ID is: " + str(job_id))
print("\nDocumentation: \nAWS: https://docs.databricks.com/aws/mlflow/deployment-job#connect \nAzure: https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#connect \nGCP: https://docs.databricks.com/gcp/mlflow/deployment-job#connect")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Link job to UC Model
# MAGIC Programmatically link the deployment job to the UC model.
# MAGIC If the model doesn't exist, it will be created with no versions.

# COMMAND ----------

import mlflow
from mlflow.tracking.client import MlflowClient


client = MlflowClient(registry_uri="databricks-uc")

try:
  model_info = client.get_registered_model(model_name)
  if model_info:
    # Model exists - Link job
    if model_info.deployment_job_id == job_id:
      print("Model exists with existing job - Pass")
      pass

    else:
      print("Model exists - Updating job")
      client.update_registered_model(model_name, deployment_job_id="") # Unlink current job
      client.update_registered_model(model_name, deployment_job_id=job_id) # Link new one

except mlflow.exceptions.RestException as e:
  if "PERMISSION_DENIED" in str(e):
        print(f"Permission denied on model `{model_name}` - Deployment Job NOT UPDATED.")
        # Optionally, handle or re-raise
        pass

  else:
    # Create Empty Model placeholder and Link job
    print("Model does not exist - Creating model and linking job")
    client.create_registered_model(model_name, deployment_job_id=job_id)
