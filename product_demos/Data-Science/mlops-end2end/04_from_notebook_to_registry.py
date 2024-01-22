# Databricks notebook source
# MAGIC %md
# MAGIC ### Managing the model lifecycle with Model Registry
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-4.png" width="1200">
# MAGIC 
# MAGIC One of the primary challenges among data scientists and ML engineers is the absence of a central repository for models, their versions, and the means to manage them throughout their lifecycle.  
# MAGIC 
# MAGIC [The MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) addresses this challenge and enables members of the data team to:
# MAGIC <br><br>
# MAGIC * **Discover** registered models, current stage in model development, experiment runs, and associated code with a registered model
# MAGIC * **Transition** models to different stages of their lifecycle
# MAGIC * **Deploy** different versions of a registered model in different stages, offering MLOps engineers ability to deploy and conduct testing of different model versions
# MAGIC * **Test** models in an automated fashion
# MAGIC * **Document** models throughout their lifecycle
# MAGIC * **Secure** access and permission for model registrations, transitions or modifications
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F04_deploy_to_registry&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Move model to registry and request transition to STAGING.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->
# MAGIC                  

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Use the Model Registry
# MAGIC Typically, data scientists who use MLflow will conduct many experiments, each with a number of runs that track and log metrics and parameters. During the course of this development cycle, they will select the best run within an experiment and register its model with the registry.  Think of this as **committing** the model to the registry, much as you would commit code to a version control system.  
# MAGIC 
# MAGIC The registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="hive_metastore"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sending our model to the registry
# MAGIC 
# MAGIC We'll programatically select the best model from our last Auto-ML run and deploy it in the registry. We can easily do that using MLFlow `search_runs` API:

# COMMAND ----------

#Let's get our last auto ml run. This is specific to the demo, it just gets the experiment ID of the last Auto ML run.
experiment_id = get_automl_churn_run()['experiment_id']

best_model = mlflow.search_runs(experiment_ids=[experiment_id], order_by=["metrics.val_f1_score DESC"], max_results=1, filter_string="status = 'FINISHED'")
best_model

# COMMAND ----------

# MAGIC %md Once we have our best model, we can now deploy it in production using it's run ID

# COMMAND ----------

run_id = best_model.iloc[0]['run_id']

#add some tags that we'll reuse later to validate the model
client = mlflow.tracking.MlflowClient()
client.set_tag(run_id, key='demographic_vars', value='seniorCitizen,gender_Female')
client.set_tag(run_id, key='db_table', value=f'{dbName}.dbdemos_mlops_churn_features')

#Deploy our autoML run in MLFlow registry
model_details = mlflow.register_model(f"runs:/{run_id}/model", "dbdemos_mlops_churn")

# COMMAND ----------

# MAGIC %md
# MAGIC At this point the model will be in `None` stage.  Let's update the description before moving it to `Staging`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update Description
# MAGIC We'll do this for the registered model overall, and the particular version.

# COMMAND ----------

model_version_details = client.get_model_version(name="dbdemos_mlops_churn", version=model_details.version)

#The main model description, typically done once.
client.update_registered_model(
  name=model_details.name,
  description="This model predicts whether a customer will churn.  It is used to update the Telco Churn Dashboard in DB SQL."
)

#Gives more details on this specific model version
client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description="This model version was built using XGBoost. Eating too much cake is the sin of gluttony. However, eating too much pie is okay because the sin of pie is always zero."
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Request Transition to Staging
# MAGIC 
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_move_to_stating.gif">
# MAGIC 
# MAGIC Our model is now read! Let's request a transition to Staging. 
# MAGIC 
# MAGIC While this example is done using the API, we can also simply click on the Model Registry button.

# COMMAND ----------

request_transition(model_name = "dbdemos_mlops_churn", version = model_details.version, stage = "Staging")

# COMMAND ----------

# MAGIC %md #### Leave Comment in Registry

# COMMAND ----------

# Leave a comment for the ML engineer who will be reviewing the tests
comment = "This was the best model from AutoML, I think we can use it as a baseline."

model_comment(model_name = "dbdemos_mlops_churn",
             version = model_details.version,
             comment = comment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: MLOps model testing and validation
# MAGIC 
# MAGIC Because we defined our webhooks earlier, a job will automatically start, testing the new model being deployed and validating the request.
# MAGIC 
# MAGIC Remember our webhook setup ? That's the orange part in the diagram.
# MAGIC 
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 >
# MAGIC 
# MAGIC If the model passes all the tests, it'll be accepted and moved into STAGING. Otherwise it'll be rejected, and a slack notification will be sent.
# MAGIC 
# MAGIC Next: 
# MAGIC  * Find out how the model is being tested befored moved to STAGING [using the Databricks Staging test notebook]($./05_job_staging_validation) (optional)
# MAGIC  * Or discover how to [run Batch and Real-time inference from our STAGING model]($./06_staging_inference)
