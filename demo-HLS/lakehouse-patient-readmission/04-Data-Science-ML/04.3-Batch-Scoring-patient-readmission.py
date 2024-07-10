# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Use the best AutoML generated model to analyze our entire patient cohort
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-ml-experiment.png" style="float: right" width="600px">
# MAGIC
# MAGIC
# MAGIC Databricks AutoML runs experiments across a grid and creates many models and metrics to determine the best models among all trials. This is a glass-box approach to create a baseline model, meaning we have all the code artifacts and experiments available afterwards. 
# MAGIC
# MAGIC Here, we selected the Notebook from the best run from the AutoML experiment.
# MAGIC
# MAGIC All the code below has been automatically generated. As data scientists, we can tune it based on our business knowledge, or use the generated model as-is.
# MAGIC
# MAGIC This saves data scientists hours of developement and allows team to quickly bootstrap and validate new projects, especally when we may not know the predictors for alternative data such as the telco payment data.
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.3-Batch-Scoring-patient-readmission&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Running batch inference to score our existing database
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/patient-risk-ds-flow-3.png?raw=true" width="700px" style="float: right; margin-left: 10px;" />
# MAGIC
# MAGIC Our model was created and deployed in production within the MLFlow registry.
# MAGIC
# MAGIC We can now easily load it calling the `Production` stage, and use it in any Data Engineering pipeline (a job running every night, in streaming or even within a Delta Live Table pipeline).
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC We'll then save this information as a new table so that we can add this information in dashboards or external OLTP databases.

# COMMAND ----------

#Make sure we use Mlflow with UC registry
mlflow.set_registry_uri('databricks-uc')
# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.dbdemos_hls_patient_readmission@prod", result_type='double')

# COMMAND ----------

features = loaded_model.metadata.get_input_schema().input_names()

#For this demo, reuse our dataset to test the batch inferences
test_dataset = spark.table('training_dataset')

patient_risk_df =  test_dataset \
                   .withColumn("risk_prediction", loaded_model(struct(*features))) \
                   .select('ENCOUNTER_ID', 'PATIENT_ID', 'risk_prediction')

display(patient_risk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC In the scored dataframe above, we have essentially created an end-to-end process to predict readmission risk for any patient. 
# MAGIC
# MAGIC We have a binary prediction which captures this and incorporates all the intellience from Databricks AutoML and curated features, but this could also return a probability between 0 and 1 depending on how you want your results.

# COMMAND ----------

# DBTITLE 1,Let's save our prediction as a new table
patient_risk_df.write.mode("overwrite").saveAsTable(f"patient_readmission_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Next steps
# MAGIC
# MAGIC at risk and providing cusom care to reduce readmission risk,
# MAGIC - Deploy Real time inference with [04.4-Model-Serving-patient-readmission]($./04.4-Model-Serving-patient-readmission) to enable realtime capabilities and instantly get insight for a specific patient (Databricks Serverless Model Serving).
# MAGIC
# MAGIC Or
# MAGIC
# MAGIC - Explain the model for our entire population or a specific patient to understand the risk factors and further personalize care with [04.5-Explainability-patient-readmission]($./04.5-Explainability-patient-readmission)
