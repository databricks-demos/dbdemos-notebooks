# Databricks notebook source
# MAGIC %md
# MAGIC # Model Deployment Job creation (programmatically)
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
# MAGIC mlflow==3.1.4
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --quiet mlflow --upgrade
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true $reset_all_data=false

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
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


w = WorkspaceClient()
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
                base_parameters={"smoke_test": True},
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

created_job = w.jobs.create(**job_settings.__dict__)
print("Use the job name " + job_name + " to connect the deployment job to the UC model " + model_name + " as indicated in the UC Model UI.")
print("\nFor your reference, the job ID is: " + str(created_job.job_id))
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
  if client.get_registered_model(model_name):
    # Model exists - Link job
    client.update_registered_model(model_name, deployment_job_id=created_job.job_id)

except mlflow.exceptions.RestException:
  # Create Empty Model placeholder and Link job
  client.create_registered_model(model_name, deployment_job_id=created_job.job_id)
