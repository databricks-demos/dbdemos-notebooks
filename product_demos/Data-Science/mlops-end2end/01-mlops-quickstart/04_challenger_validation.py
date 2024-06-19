# Databricks notebook source
# MAGIC %md
# MAGIC # Challenger model validation
# MAGIC
# MAGIC This notebook performs validation tasks on the candidate __Challenger__ model.
# MAGIC
# MAGIC It goes through a few steps to validate the model before labelling it (by setting its alias) to `Challenger`.
# MAGIC
# MAGIC When organizations first start to put MLOps processes in place, they should consider having a "human-in-the-loop" to perform visual analyses to validate models before promoting them. As they get more familiar with the process, they can consider automating the steps in a __Workflow__ . The benefits of automation is to ensure that these validation checks are systematically performed before new models are integrated into inference pipelines or deployed for realtime serving. Of course, organizations can opt to retain a "human-in-the-loop" in any part of the process and put in place the degree of automation that suits its business needs.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-5.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F05_job_staging_validation&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Trigger Model testing and validation job.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## General Validation Checks
# MAGIC
# MAGIC <!--img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 -->
# MAGIC
# MAGIC In the context of MLOps, there are more tests than simply how accurate a model will be.  To ensure the stability of our ML system and compliance with any regulatory requirements, we will subject each model added to the registry to a series of validation checks.  These include, but are not limited to:
# MAGIC <br>
# MAGIC * __Model documentation__
# MAGIC * __Inference on production data__
# MAGIC * __Champion-Challenger testing to ensure that business KPIs are acceptable__
# MAGIC
# MAGIC In this notebook we explore some approaches to performing these tests, and how we can add metadata to our models with tagging if they have passed a given test or not.
# MAGIC
# MAGIC This part is typically specific to your line of business and quality requirements.
# MAGIC
# MAGIC For each test, we'll add information using tags to know what has been validated in the model. We can also add Comments to a model if needed.

# COMMAND ----------

# MAGIC %pip install "mlflow-skinny[databricks]>=2.11"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information
# MAGIC
# MAGIC We will fetch the model information for the __Challenger__ model from Unity Catalog.

# COMMAND ----------

# We are interested in validating the Challenger model
model_alias = "Challenger"

client = MlflowClient()
model_details = client.get_model_version_by_alias(model_name, model_alias)
model_version = int(model_details.version)

print(f"Validating {model_alias} model for {model_name} on model version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model checks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description check
# MAGIC
# MAGIC Has the data scientist provided a description of the model being submitted?

# COMMAND ----------

# If there's no description or an insufficient number of charaters, tag accordingly
if not model_details.description:
  client.set_model_version_tag(name=model_name, version=model_details.version, key="has_description", value=False)
  print("Please add model description")
elif not len(model_details.description) > 20:
  client.set_model_version_tag(name=model_name, version=model_details.version, key="has_description", value=False)
  print("Please add detailed model description (40 char min).")
else:
  client.set_model_version_tag(name=model_name, version=str(model_details.version), key="has_description", value=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model performance metric
# MAGIC
# MAGIC We want to validate the model performance metric. Typically, we want to compare this metric obtained for the Challenger model agaist that of the Champion model. Since we have yet to register a Champion model, we will only retrieve the metric for the Challenger model without doing a comparison.
# MAGIC
# MAGIC The registered model captures information about the MLflow experiment run, where the model metrics were logged during training. This gives you traceability from the deployed model back to the initial training runs.
# MAGIC
# MAGIC Here, we will use the F1 score for the out-of-sample test data that was set aside at training time.

# COMMAND ----------

model_run_id = model_details.run_id
f1_score = mlflow.get_run(model_run_id).data.metrics['test_f1_score']

# Tag that F1 metric check has passed
client.set_model_version_tag(name=model_name, version=model_details.version, key="metric_f1_passed", value=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation results
# MAGIC
# MAGIC That's it! We have demonstrated some simple checks on the model. Let's take a look at the validation results.
# MAGIC
# MAGIC In practice, these checks may also involve business KPIs.

# COMMAND ----------

results = client.get_model_version(model_name, model_version)
results.tags}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting the Challenger to Champion
# MAGIC
# MAGIC When we are satisfied with the results of the __Challenger__ model, we can then promote it to Champion. This is done by setting its alias to `@Champion`. Inference pipelines that load the model using the `@Champion` alias will then be loading this new model. The alias on the older Champion model, if there is one, will be automatically unset. The model retains its `@Challenger` alias until a newer Challenger model is deployed with the alias to replace it.

# COMMAND ----------

client.set_registered_model_alias(
  name=model_name,
  alias="Champion",
  version=model_version
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations, our model is now validated and promoted accordingly
# MAGIC
# MAGIC We now have the certainty that our model is ready to be used in inference pipelines and in realtime serving endpoints, as it matches our validation standards.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our newly promoted Champion model]($./05_batch_inference)
