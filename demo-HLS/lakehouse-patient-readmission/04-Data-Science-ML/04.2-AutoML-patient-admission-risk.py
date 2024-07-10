# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Accelerating Data Science with Databricks AutoML
# MAGIC
# MAGIC ##  Predicting patient readmission risk: Single click deployment with AutoML
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/patient-risk-ds-flow-2.png?raw=true" width="700px" style="float: right; margin-left: 10px;" />
# MAGIC
# MAGIC
# MAGIC In this notebook, we will explore how to use Databricks AutoML to generate the best notebooks to predict our patient readmission risk and deploy our model in production.
# MAGIC
# MAGIC Databricks AutoML allows you to quickly generate baseline models and notebooks. 
# MAGIC
# MAGIC ML experts can accelerate their workflow by fast-forwarding through the usual trial-and-error and focus on customizations using their domain knowledge, and citizen data scientists can quickly achieve usable results with a low-code approach.

# COMMAND ----------

# DBTITLE 1,Make sure we have the latset sdk (used in the helper)
# MAGIC %pip install databricks-sdk -U
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting our training dataset 
# MAGIC
# MAGIC Let's use the our training dataset determinining the readmission after 30 days for all our population. We will use that as our training label and what we want our model to predict.

# COMMAND ----------

training_dataset = spark.table('training_dataset')
training_dataset.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define what features to look up for our model
# MAGIC
# MAGIC Let's only keep the relevant features for our model training. We are removing columns such as `SSN` or `IDs`.
# MAGIC
# MAGIC This step could also be done selecting the training_dataset table from the UI and selecting the column of interest.
# MAGIC
# MAGIC *Note: this could also be retrived from our Feature Store tables. For more details on that open the companion notebook.*

# COMMAND ----------

feature_names = ['MARITAL_M', 'MARITAL_S', 'RACE_asian', 'RACE_black', 'RACE_hawaiian', 'RACE_other', 'RACE_white', 'ETHNICITY_hispanic', 'ETHNICITY_nonhispanic', 'GENDER_F', 'GENDER_M', 'INCOME'] \
              + ['BASE_ENCOUNTER_COST', 'TOTAL_CLAIM_COST', 'PAYER_COVERAGE', 'enc_length', 'ENCOUNTERCLASS_ambulatory', 'ENCOUNTERCLASS_emergency', 'ENCOUNTERCLASS_hospice', 'ENCOUNTERCLASS_inpatient', 'ENCOUNTERCLASS_outpatient', 'ENCOUNTERCLASS_wellness'] \
              + ['age_at_encounter'] \
              + ['30_DAY_READMISSION']

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating patient readmission model creation using MLFlow and Databricks AutoML
# MAGIC  
# MAGIC MLFlow is an open source project allowing model tracking, packaging and deployment. Every time your Data Science team works on a model, Databricks will track all parameters and data used and will auto-log them. This ensures ML traceability and reproductibility, making it easy to know what parameters/data were used to build each model and model version.
# MAGIC
# MAGIC ### A glass-box solution that empowers data teams without taking control away
# MAGIC
# MAGIC While Databricks simplifies model deployment and governance (MLOps) with MLFlow, bootstraping new ML projects can still be a long and inefficient process.
# MAGIC
# MAGIC Instead of creating the same boilerplate for each new project, Databricks AutoML can automatically generate state of the art models for Classifications, Regression, and Forecasting.
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks worth of effort.
# MAGIC
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/MLFlowAutoML.png"/>
# MAGIC
# MAGIC ### Using Databricks Auto ML with our readmission risk
# MAGIC
# MAGIC AutoML is available in the "Machine Learning" menu. All we have to do is start a new AutoML Experiments and select the feature table we just created (`creditdecisioning_features`)
# MAGIC
# MAGIC Our prediction target is the `30_DAY_READMISSION` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

from databricks import automl
summary = automl.classify(training_dataset.select(feature_names), target_col="30_DAY_READMISSION", primary_metric="roc_auc", timeout_minutes=6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying our model in production
# MAGIC
# MAGIC Our model is now ready. We can review the notebook generated by the auto-ml run and customize if if required.
# MAGIC
# MAGIC For this demo, we'll consider that our model is ready and deploy it in production in our Unity Catalog Model Registry:

# COMMAND ----------

model_name = "dbdemos_hls_patient_readmission"

#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')
    
model_registered = mlflow.register_model(f"runs:/{summary.best_trial.mlflow_run_id}/model", f"{catalog}.{db}.{model_name}")

#Move the model in production
print("registering model version "+model_registered.version+" as production model")
client = mlflow.tracking.MlflowClient()
client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)

#Make sure all other users can access the model for our demo(see _resource/00-global-setup for details)
set_model_permission(f"{catalog}.{db}.{model_name}", "ALL_PRIVILEGES", "account users")

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our automl model as production ready! 
# MAGIC
# MAGIC Open the Unity Catalog [the dbdemos_hls_patient_readmission model](/explore/data/models/dbdemos/hls_patient_readmission/dbdemos_hls_patient_readmission) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our model predicting default risks is now deployed in production
# MAGIC
# MAGIC So far we have:
# MAGIC * ingested all required data in a single source of truth using the OMOP data model,
# MAGIC * properly secured all data (including granting granular access controls, masked PII data, applied column level filtering),
# MAGIC * enhanced that data through feature engineering (and Feature Store as an option),
# MAGIC * used MLFlow AutoML to track experiments and build a machine learning model,
# MAGIC * registered the model.
# MAGIC
# MAGIC ### Next steps
# MAGIC We're now ready to use our model use it for:
# MAGIC
# MAGIC - Batch inferences in notebook [04.3-Batch-Scoring-patient-readmission]($./04.3-Batch-Scoring-patient-readmission) to start using it for identifying patient at risk and providing cusom care to reduce readmission risk,
# MAGIC - Real time inference with [04.4-Model-Serving-patient-readmission]($./04.4-Model-Serving-patient-readmission) to enable realtime capabilities and instantly get insight for a specific patient.
# MAGIC - Explain model for our entire population or a specific patient to understand the risk factors and further personalize care with [04.5-Explainability-patient-readmission]($./04.5-Explainability-patient-readmission)
