# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC ## Inference with the Champion model
# MAGIC
# MAGIC With Models in Unity Catalog, they can be loaded for use in batch inference pipelines. The generated predictions can used to devise customer retention strategies or be used for analytics. The model in use is the __Champion__ model, and we will load this for use in our pipeline.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-5.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or disable the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05_batch_inference&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow==2.19
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $setup_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <!--img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" /-->
# MAGIC
# MAGIC Now that our model is available in the Unity Catalog Model Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use the MLflow function to load a pyspark UDF and distribute our inference in the entire cluster. We can load the model with plain Python and use a Pandas Dataframe if the data is small.
# MAGIC
# MAGIC If you don't know how to start, you can get sample code from the __"Artifacts"__ page of the model's experiment run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inferences

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.mlops_churn@Champion").download_artifacts(artifact_path="requirements.txt") # download model from remote registry

# COMMAND ----------

# MAGIC %pip install --quiet -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will load the model as a Spark UDF and generate predictions for our customer records.
# MAGIC
# MAGIC For simplicity, we assume that features have been extracted for the new customer records already stored in the feature table. These are typically done by separate feature engineering pipelines.

# COMMAND ----------

# DBTITLE 1,In a Python notebook
import mlflow
# Load customer features to be scored
inference_df = spark.read.table(f"mlops_churn_inference")
# Load champion model as a Spark UDF
champion_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.mlops_churn@Champion")

# Batch score
preds_df = inference_df.withColumn('predictions', champion_model(*champion_model.metadata.get_input_schema().input_names()))

display(preds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! Our data can now be saved as a table and re-used by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!
# MAGIC
# MAGIC Your data will also be available within Genie to answer any churn-related question using plain text English!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC This is all for the quickstart demo! We have looked at the basic concepts of MLOps and how Databricks helps you achieve them. They include:
# MAGIC
# MAGIC - Feature engineering and storing feature tables with labels in Databricks
# MAGIC - AutoML, model training, and experiment tracking in MLflow
# MAGIC - Registering models as Models in Unity Catalog for governed usage
# MAGIC - Model validation, Champion-Challenger testing, and model promotion
# MAGIC - Batch inference by loading the model as a pySpark UDF
# MAGIC
# MAGIC We hope you've enjoyed this demo. As the next step, look out for our Advanced End-to-end MLOps demo, which will include more in-depth walkthroughs on the following aspects of MLOps:
# MAGIC
# MAGIC - Feature serving and Feature Store
# MAGIC - Data and model monitoring
# MAGIC - Deployment for real-time inference
