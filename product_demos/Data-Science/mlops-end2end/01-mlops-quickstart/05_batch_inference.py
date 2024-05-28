# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC ## Inference with the Challenger model
# MAGIC
# MAGIC With models registered in the Unity Catalog Model Registry, they can be loaded for use in batch inference pipelines. The generated predictions can used to devise customer retention strategies, or be used for analytics. The model in use is the __Champion__ model, and we will load this for use in our pipeline.
# MAGIC
# MAGIC ## TODO: REMOVE - Champion-Challenger testing
# MAGIC
# MAGIC In earlier steps, we have registered a __Challenger__ model. Later on in this notebook, we will look at the concept of Champion-Challenger testing, which ensures that the __Challenger__ model would not cause adverse business impact before letting it replace the Champion model.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-6.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F06_staging_inference&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Load the model from MLFLow and run inferences, in batch or realtime.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install "mlflow-skinny[databricks]>=2.11"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $setup_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC
# MAGIC Now that our model is available in the Unity Catalog Model Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry !

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inferences

# COMMAND ----------

model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version # Get champion version
print(f"Champion model version for {model_name}: {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will load the model as a Spark UDF and generate predictions for our customer records.
# MAGIC
# MAGIC For simplicity, we assume that features have been extracted for the new customer records and these are already stored in the feature table. These are typically done by separate feature engineering pipelines.

# COMMAND ----------

# DBTITLE 1,In a python notebook
# Load customer features to be scored
inference_df = spark.read.table(f"{catalog}.{db}.mlops_churn_inference")

# Load champion model as a Spark UDF
champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}@Champion")

# Batch score
preds_df = inference_df.withColumn('predictions', champion_model(*inference_df.columns))

display(preds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! Our data can now be saved as a table and re-used by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC This is all for the quickstart demo! We have looked at basic concepts of MLOps and how Databricks helps you achieve them. They include:
# MAGIC
# MAGIC - Feature engineering and storing feature tables in Databricks
# MAGIC - AutoML, model training and experiement tracking in MLflow
# MAGIC - Register models in the Unity Catalog Model Registry for use by runtime systems
# MAGIC - Model validation and promotion
# MAGIC - Batch inference
# MAGIC - Champion-Challenger testing
# MAGIC
# MAGIC We hope you've enjoyed this demo. As the next step, look out for our Advanced End-to-end MLOps demo, which will include more in-depth walkthroughs on the following aspects of MLOps:
# MAGIC
# MAGIC - Feature serving and Feature Store
# MAGIC - Data and model monitoring
# MAGIC - Deployment for real-time inference
