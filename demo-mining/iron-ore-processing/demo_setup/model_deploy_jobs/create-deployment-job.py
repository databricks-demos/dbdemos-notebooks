# Databricks notebook source
# MAGIC %md
# MAGIC # INSTRUCTIONS
# MAGIC Running this notebook will create a Databricks Job templated as a MLflow 3.0 Deployment Job. This job will have three tasks: Evaluation, Approval_Check, and Deployment. The Evaluation task will evaluate the model on a dataset, the Approval_Check task will check if the model has been approved for deployment using UC Tags and the Approval button in the UC Model UI, and the Deployment task will deploy the model to a serving endpoint.
# MAGIC
# MAGIC 1. Copy the example deployment jobs template notebooks ([AWS](https://docs.databricks.com/aws/mlflow/deployment-job#example-template-notebooks) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#example-template-notebooks) | [GCP](https://docs.databricks.com/gcp/mlflow/deployment-job#example-template-notebooks)) into your Databricks Workspace.
# MAGIC 2. Create a UC Model or use an existing one. For example, see the MLflow 3 examples ([AWS](https://docs.databricks.com/aws/mlflow/mlflow-3-install#example-notebooks) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/mlflow-3-install#example-notebooks) | [GCP](https://docs.databricks.com/gcp/mlflow/mlflow-3-install#example-notebooks)).
# MAGIC 3. Update the TODOs/values in the next cell before running the notebook.
# MAGIC 4. After running the notebook, the created job will not be connected to any UC Model. You will still need to **connect the job to a UC Model** in the UC Model UI as indicated in the documentation ([AWS](https://docs.databricks.com/aws/mlflow/deployment-job#connect) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#connect) | [GCP](https://docs.databricks.com/gcp/mlflow/deployment-job#connect)).

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# TODO: Update these values as necessary
model_name = f"{catalog_name}.{schema_name}.fe_model" # The name of the already created UC Model
job_name = "mining_demo_si_model_deployment_job" # The desired name of the deployment job

# TODO: Create notebooks for each task and populate the notebook path here, replacing the INVALID PATHS LISTED BELOW.
# These paths should correspond to where you put the notebooks templated from the example deployment jobs template notebook
# in your Databricks workspace.
evaluation_notebook_path = "/Workspace/Shared/iron_ore_precessing_demo/demo_setup/model_deploy_jobs/evaluation"
approval_notebook_path = "/Workspace/Shared/iron_ore_precessing_demo/demo_setup/model_deploy_jobs/approval"
deployment_notebook_path = "/Workspace/Shared/iron_ore_precessing_demo/demo_setup/model_deploy_jobs/deployment"

# COMMAND ----------

# Create job with necessary configuration to connect to model as deployment job
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
job_settings = jobs.JobSettings(
    name=job_name,
    tasks=[
        # jobs.Task(
        #     task_key="Evaluation",
        #     notebook_task=jobs.NotebookTask(notebook_path=evaluation_notebook_path),
        #     max_retries=0,
        # ),
        jobs.Task(
            task_key="Approval_Check",
            notebook_task=jobs.NotebookTask(
                notebook_path=approval_notebook_path,
                base_parameters={"approval_tag_name": "{{task.name}}"}
            ),
            #depends_on=[jobs.TaskDependency(task_key="Evaluation")],
            max_retries=0,
        ),
        jobs.Task(
            task_key="Deployment",
            notebook_task=jobs.NotebookTask(notebook_path=deployment_notebook_path),
            depends_on=[jobs.TaskDependency(task_key="Approval_Check")],
            max_retries=0,
        ),
    ],
    parameters=[
        jobs.JobParameter(name="model_name", default=model_name),
        jobs.JobParameter(name="model_version", default=""),
    ],
    queue=jobs.QueueSettings(enabled=True),
    max_concurrent_runs=1,
)

created_job = w.jobs.create(**job_settings.__dict__)
print("Use the job name " + job_name + " to connect the deployment job to the UC model " + model_name + " as indicated in the UC Model UI.")
print("\nFor your reference, the job ID is: " + str(created_job.job_id))
print("\nDocumentation: \nAWS: https://docs.databricks.com/aws/mlflow/deployment-job#connect \nAzure: https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#connect \nGCP: https://docs.databricks.com/gcp/mlflow/deployment-job#connect")