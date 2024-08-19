# Databricks notebook source
# MAGIC %pip install "databricks-sdk>=0.28.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Create Model Development Pipeline
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time

w = WorkspaceClient()

notebook_root_path = f"/Repos/{w.current_user.me().user_name}/dbdemos-notebooks/product_demos/Data-Science/mlops-end2end/02-mlops-advanced"

# cluster_id = w.clusters.ensure_cluster_is_running(
#    os.environ["DATABRICKS_CLUSTER_ID"]) and os.environ["DATABRICKS_CLUSTER_ID"]

cluster_id = "0609-084551-b0k61am"

created_job = w.jobs.create(
    name="[test] Advanced MLOps - Model Developement",
    tasks=[
        jobs.Task(
            description="Model training",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{notebook_root_path}/02_automl_champion"
            ),
            task_key="Model_training",
            timeout_seconds=0,
        ),
        jobs.Task(
            description="Register model",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{notebook_root_path}/03_from_notebook_to_models_in_uc"
            ),
            task_key="Register_model",
            depends_on=[jobs.TaskDependency(task_key="Model_training")],
            timeout_seconds=0,
        ),
        jobs.Task(
            description="Challenger Validation",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{notebook_root_path}/04_challenger_validation"
            ),
            task_key="Challenger_validation",
            depends_on=[jobs.TaskDependency(task_key="Register_model")],
            timeout_seconds=0,
        )
    ],
)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time

w = WorkspaceClient()

notebook_root_path = f"/Repos/{w.current_user.me().user_name}/dbdemos-notebooks/product_demos/Data-Science/mlops-end2end/02-mlops-advanced"

# cluster_id = w.clusters.ensure_cluster_is_running(
#    os.environ["DATABRICKS_CLUSTER_ID"]) and os.environ["DATABRICKS_CLUSTER_ID"]

cluster_id = "0609-084551-b0k61am"

created_job = w.jobs.create(
    name="[test] Advanced MLOps - Drift Detection",
    tasks=[
        jobs.Task(
            description="Drift Detection",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{notebook_root_path}/08_drift_detection"
            ),
            task_key="Model_training",
            timeout_seconds=0,
        ),
        jobs.Task(
            description="Register model",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{notebook_root_path}/03_from_notebook_to_models_in_uc"
            ),
            task_key="Register_model",
            depends_on=[jobs.TaskDependency(task_key="Model_training")],
            timeout_seconds=0,
        ),
        jobs.Task(
            description="Challenger Validation",
            existing_cluster_id=cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{notebook_root_path}/04_challenger_validation"
            ),
            task_key="Challenger_validation",
            depends_on=[jobs.TaskDependency(task_key="Register_model")],
            timeout_seconds=0,
        )
    ],
)

# COMMAND ----------

{
  "name": "Advanced MLOps - Batch inference & drift detection",
  "email_notifications": {
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "trigger": {
    "pause_status": "UNPAUSED",
    "table_update": {
      "table_names": [
        "aminen_catalog.advanced_mlops.mlops_churn_advanced_inference"
      ]
    }
  },
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "Drift_detection",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Repos/amine.nouira@databricks.com/dbdemos-notebooks/product_demos/Data-Science/mlops-end2end/02-mlops-advanced/08_drift_detection",
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "0609-084551-b0k61am",
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      },
      "webhook_notifications": {}
    },
    {
      "task_key": "Check_Violations",
      "depends_on": [
        {
          "task_key": "Drift_detection"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "GREATER_THAN",
        "left": "{{tasks.Drift_detection.values.all_violations_count}}",
        "right": "0"
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      },
      "webhook_notifications": {}
    },
    {
      "task_key": "Retrain_model",
      "depends_on": [
        {
          "task_key": "Check_Violations",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 1002697068336153
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      },
      "webhook_notifications": {}
    }
  ],
  "queue": {
    "enabled": true
  },
  "run_as": {
    "user_name": "amine.nouira@databricks.com"
  }
}
