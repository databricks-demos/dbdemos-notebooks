# Databricks notebook source
# MAGIC %md
# MAGIC ## Setting up Model Registry Webhooks
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-3.png" width="1200">
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F03_webhook&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Define MLFLow webhook",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What is a webhook?
# MAGIC A webhook will call a web URL whenever a given event occurs. In our case, that's when a model is updated or deployed.
# MAGIC 
# MAGIC __A primary goal with MLOps is to introduce more robust testing and automation into how we deploy machine learning models.__  
# MAGIC 
# MAGIC To aid in this effort, the MLflow Model Registry supports webhooks that are triggered during the following events in the lifecycle of a model. This will allow us to automatically:
# MAGIC 
# MAGIC 
# MAGIC * Perform general validation checks and tests on any model added to the Registry
# MAGIC * Send notification / slack alert when a new model is updated
# MAGIC * Introducing a new model to acccept traffic for A/B testing
# MAGIC * ...
# MAGIC 
# MAGIC 
# MAGIC *Note that we only have to do this once for our churn model.*

# COMMAND ----------

# MAGIC %md
# MAGIC #### Webhook Supported events
# MAGIC * A new model is added to the Registry
# MAGIC * A new version of a registered model is added to the Registry
# MAGIC * A model lifecycle transition request is made (e.g., from _Production_ to _Archived_)
# MAGIC * A transition request is accepted or rejected
# MAGIC * A comment is made on a model version

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example
# MAGIC 
# MAGIC In the following example, we have two notebooks - the first commits the model to the Model Registry, and the second runs a series of general validation checks and tests on it. You can see the entire workflow illustrated below.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook.png" width=1000 >
# MAGIC <br><br>
# MAGIC Let's look at how this workflow plays out chronologically:<br><br>
# MAGIC 
# MAGIC 1. Data Scientist finishes model training and commits best model to Registry
# MAGIC 2. Data Scientist requests lifecyle transition of best model to _Staging_
# MAGIC 3. Webhooks are set for transition request event, and trigger a Databricks Job to test the model, and a Slack message to let the organization know that the lifecycle event is occurring
# MAGIC 4. The testing job is launched
# MAGIC 5. Depending on testing results, the lifecycle transition request is accepted or rejected
# MAGIC 6. Webhooks trigger and send another Slack message to report the results of testing

# COMMAND ----------

# DBTITLE 1,Install Databricks webhooks utility
# MAGIC %pip install databricks-registry-webhooks

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="hive_metastore"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create Webhooks
# MAGIC 
# MAGIC Setting up webhooks is simple using the Databricks REST API.  There are some helper functions in the `./_resources/API_Helpers` notebook, so if you want to see additional details you can check there.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model testing - Staging transition request
# MAGIC 
# MAGIC A testing notebook has been created by the ML Engineer team (we'll cover that in details soon).
# MAGIC 
# MAGIC To accept the STAGING request, we'll run this notebook as a Databricks Job whenever we receive a request to move a model to STAGING.
# MAGIC 
# MAGIC The job will be in charge to validate or reject the transition upon completion.

# COMMAND ----------

#DEMO SETUP
#For this demo, the job is programatically created if it doesn't exist. See ./_resources/API_Helpers for more details
job_id = get_churn_staging_job_id()
#This should be run once. For the demo We'll reset other webhooks to prevent from duplicated call
reset_webhooks(model_name = "dbdemos_mlops_churn")

#Once we have the id of the job running the tests, we add the hook:
create_job_webhook(model_name = "dbdemos_mlops_churn", job_id = job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notification
# MAGIC We also want to send slack notification when the model changes from one stage to another:

# COMMAND ----------

create_notification_webhook(model_name = "dbdemos_mlops_churn", slack_url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Webhooks

# COMMAND ----------

# DBTITLE 1,Let's review the webhooks we created so far
# List
list_webhooks("dbdemos_mlops_churn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on MLflow Model Registry webhook?  
# MAGIC **A:** Check out the <a href="https://databricks.com/blog/2020/11/19/mlflow-model-registry-on-databricks-simplifies-mlops-with-ci-cd-features.html" target="_blank"> blog post with the latest documentation</a>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Our Automated model validation is in place!
# MAGIC 
# MAGIC Using MLFlow webhook, we'll trigger a slack notification and a validation job as soon as a Data Scientist request a model to be moved to Stating!
# MAGIC 
# MAGIC Next: let's see how a Data Scientist can [deploy the Auto ML model to registry and request to move it to STAGING]($./04_from_notebook_to_registry)
