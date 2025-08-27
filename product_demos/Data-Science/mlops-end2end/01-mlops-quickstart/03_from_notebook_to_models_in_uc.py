# Databricks notebook source
# MAGIC %md
# MAGIC # Managing the model lifecycle in Unity Catalog
# MAGIC
# MAGIC One of the primary challenges among data scientists and ML engineers is the absence of a central repository for models, their versions, and the means to manage them throughout their lifecycle.
# MAGIC
# MAGIC [Models in Unity Catalog](https://docs.databricks.com/en/mlflow/models-in-uc.html) addresses this challenge and enables members of the data team to:
# MAGIC <br>
# MAGIC * **Discover** registered models, current aliases in model development, experiment runs, and associated code with a registered model
# MAGIC * **Promote** models to different phases of their lifecycle with the use of model aliases
# MAGIC * **Tag** models to capture metadata specific to your MLOps process
# MAGIC * **Deploy** different versions of a registered model, offering MLOps engineers the ability to deploy and conduct testing of different model versions
# MAGIC * **Test** models in an automated fashion
# MAGIC * **Document** models throughout their lifecycle
# MAGIC * **Secure** access and permission for model registrations, execution, or modifications
# MAGIC
# MAGIC We will look at how we test and promote a new __Challenger__ model as a candidate to replace an existing __Champion__ model.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-3-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or disable the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03_from_notebook_to_models_in_uc&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use Models in Unity Catalog
# MAGIC Typically, data scientists who use MLflow will conduct many experiments, each with a number of runs that track and log metrics and parameters. During the course of this development cycle, they will select the best run within an experiment and register its model to Unity Catalog.  Think of this as **committing** the model to the Unity Catalog, much as you would commit code to a version control system.
# MAGIC
# MAGIC Unity Catalog proposes free-text model alias i.e. `Baseline`, `Challenger`, `Champion` along with tagging.
# MAGIC
# MAGIC Users with appropriate permissions can create models, modify aliases and tags, use models etc.

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC mlflow==3.3.0
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow --upgrade
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programmatically find the best run and push the model to Unity Catalog for validation
# MAGIC
# MAGIC We have completed the training runs to find a candidate __Challenger__ model. We'll programmatically select the best model from our last ML experiment and register it to Unity Catalog. We can easily do that using MLFlow `search_runs` API:

# COMMAND ----------

model_name = f"{catalog}.{db}.mlops_churn"

# COMMAND ----------

print(f"Finding best run from {xp_name} and pushing new model version to {model_name}")
mlflow.set_experiment(f"{xp_path}/{xp_name}")

# COMMAND ----------

import mlflow


xp_name = "dbdemos_mlops_churn_demo_quickstart"
print(f"Finding best run from {xp_name}_* and pushing new model version to {model_name}")
xp_path = f"/Users/{current_user}"

mlflow.set_experiment(f"{xp_path}/{xp_name}")


experiment_id = mlflow.search_experiments(filter_string=f"name LIKE '{xp_path}/{xp_name}%'", order_by=["last_update_time DESC"])[0].experiment_id
print(experiment_id)

# COMMAND ----------

# Let's get our best ml run
best_model = mlflow.search_runs(
  experiment_ids=experiment_id,
  order_by=["metrics.val_f1_score DESC"],
  max_results=1,
  filter_string="status = 'FINISHED' and run_name='light_gbm_baseline'" #filter on mlops_best_run to always use the notebook 02 to have a more predictable demo
)
# Optional: Load MLflow Experiment as a spark df and see all runs
# df = spark.read.format("mlflow-experiment").load(experiment_id)
best_model

# COMMAND ----------

# MAGIC %md Once we have our best model, we can now register it to the Unity Catalog Model Registry using it's run ID

# COMMAND ----------

print(f"Registering model to {model_name}")  # {model_name} is defined in the setup script

# Get the run id from the best model
run_id = best_model.iloc[0]['run_id']

# Register the best model from experiments run to MLflow model registry
model_details = mlflow.register_model(f"runs:/{run_id}/sklearn_model", model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC At this point, the model does not yet have any aliases or descriptions that indicate its lifecycle and meta-data/info.  Let's update this information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Give the registered model a description
# MAGIC
# MAGIC We'll do this overall for the registered model.

# COMMAND ----------

from mlflow import MlflowClient


client = MlflowClient()

# The main model description is typically done once.
client.update_registered_model(
  name=model_details.name,
  description="This model predicts whether a customer will churn using the features in the mlops_churn_training table. It is used to power the Telco Churn Dashboard in DB SQL.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC And add some more details on the new version we just registered

# COMMAND ----------

# Provide more details on this specific model version
best_score = best_model['metrics.val_f1_score'].values[0]
run_name = best_model['tags.mlflow.runName'].values[0]
version_desc = f"This model version has an F1 validation metric of {round(best_score,4)*100}%. Follow the link to its training run for more details."

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description=version_desc
)

# We can also tag the model version with the F1 score for visibility
client.set_model_version_tag(
  name=model_details.name,
  version=model_details.version,
  key="f1_score",
  value=f"{round(best_score,4)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set the latest model version as the Baseline/Challenger model
# MAGIC
# MAGIC We will set this newly registered model version as the __Challenger__ _(or __Baseline__) model_. Challenger models are candidate models to replace the Champion model, which is the model currently in use.
# MAGIC
# MAGIC We will use the model's alias to indicate the stage it is at in its lifecycle.

# COMMAND ----------

# Set this version as the Challenger model, using its model alias
client.set_registered_model_alias(
  name=model_name,
  alias="Challenger", # Baseline
  version=model_details.version
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now, visually inspect the model versions in Unity Catalog Explorer. You should see the version description and `Challenger` alias applied to the version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Validation of the Challenger model
# MAGIC
# MAGIC At this point, with the __Challenger__ model registered, we would like to validate the model. The validation steps are implemented in a notebook so that the validation process can be automated as part of a Databricks Workflow job.
# MAGIC
# MAGIC If the model passes all the tests, it'll be promoted to `Champion`.
# MAGIC
# MAGIC Next: Find out how the model is being tested before being promoted as `Champion` [using the model validation notebook]($./04_challenger_validation)
