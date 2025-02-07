# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data-centric Machine Learning
# MAGIC
# MAGIC In Databricks, machine learning is not a separate product or service that needs to be "connected" to the data. The Lakehouse being a single, unified product, machine learning in Databricks "sits" on top of the data, so challenges like inability to discover and access data no longer exist.
# MAGIC
# MAGIC <br />
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/06-Responsible-AI/images/architecture_3.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Credit Scoring default prediction
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Single click deployment with AutoML
# MAGIC
# MAGIC
# MAGIC Let's see how we can now leverage the credit decisioning data to build a model predicting and explaining customer creditworthiness.
# MAGIC
# MAGIC We'll start by retrieving our data from the feature store and creating our training dataset.
# MAGIC
# MAGIC We'll then use Databricks AutoML to automatically build our model.

# COMMAND ----------

# DBTITLE 1,Loading the training dataset from the Databricks Feature Store
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

features_set = fe.read_table(name=f"{catalog}.{db}.credit_decisioning_features")
display(features_set)

# COMMAND ----------

# DBTITLE 1,Creating the label: "defaulted"
credit_bureau_label = (spark.table("credit_bureau_gold")
                            .withColumn("defaulted", F.when(col("CREDIT_DAY_OVERDUE") > 60, 1)
                                                      .otherwise(0))
                            .select("cust_id", "defaulted"))
#As you can see, we have a fairly imbalanced dataset
df = credit_bureau_label.groupBy('defaulted').count().toPandas()
px.pie(df, values='count', names='defaulted', title='Credit default ratio')

# COMMAND ----------

# DBTITLE 1,Build our training dataset (join features and label)
training_dataset = credit_bureau_label.join(features_set, "cust_id", "inner")
training_dataset.write.mode('overwrite').saveAsTable('credit_decisioning_features_labels')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Balancing our dataset
# MAGIC
# MAGIC Let's downsample and upsample our dataset to improve our model performance

# COMMAND ----------

major_df = training_dataset.filter(col("defaulted") == 0)
minor_df = training_dataset.filter(col("defaulted") == 1)

# duplicate the minority rows
oversampled_df = minor_df.union(minor_df)

# downsample majority rows
undersampled_df = major_df.sample(oversampled_df.count()/major_df.count()*3, 42)


# COMMAND ----------

# combine both oversampled minority rows and undersampled majority rows, this will improve our balance while preseving enough information.
train_df = undersampled_df.unionAll(oversampled_df).drop('cust_id').na.fill(0)

# COMMAND ----------

# Save it as a table to be able to select it with the AutoML UI.
train_df.write.mode('overwrite').saveAsTable('credit_risk_train_df')
px.pie(train_df.groupBy('defaulted').count().toPandas(), values='count', names='defaulted', title='Credit default ratio')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating credit scoring model creation using MLFlow and Databricks AutoML
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
# MAGIC ### Using Databricks Auto ML with our Credit Scoring dataset
# MAGIC
# MAGIC AutoML is available in the "Machine Learning" space. All we have to do is start a new AutoML Experiments and select the feature table we just created (`creditdecisioning_features`)
# MAGIC
# MAGIC Our prediction target is the `defaulted` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

from databricks import automl
xp_path = "/Shared/rai/experiments/credit-decisioning"
xp_name = f"automl_rai_credit_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
automl_run = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = train_df.sample(0.1),
    target_col = "defaulted",
    timeout_minutes = 5
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register our model in Unity Catalog
# MAGIC
# MAGIC Our model is now ready. We can review the notebook generated by the auto-ml run and customize if if required.
# MAGIC
# MAGIC For this demo, we'll consider that our model is ready to be registered in Unity Catalog:

# COMMAND ----------

import mlflow

#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')
client = mlflow.MlflowClient()

# Register the model to Unity Catalog
try:
    result = mlflow.register_model(model_uri=f"runs:/{automl_run.best_trial.mlflow_run_id}/model", name=f"{catalog}.{db}.{model_name}")
    print(f"Model registered with version: {result.version}")
except mlflow.exceptions.MlflowException as e:
    print(f"Error registering model: {e}")

# Flag it as Production ready using UC Aliases
client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="None", version=result.version)

# COMMAND ----------


