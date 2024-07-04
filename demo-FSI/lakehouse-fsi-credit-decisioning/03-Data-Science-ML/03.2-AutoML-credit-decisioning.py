# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science on the Databricks Lakehouse
# MAGIC
# MAGIC ## ML is key to disruption & personalization
# MAGIC
# MAGIC Being able to ingest and query our credit-related database is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC
# MAGIC Customers now expect real time personalization and new form of comunication. Modern data company achieve this with AI.
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px;height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="font-family: 'DM Sans'">
# MAGIC   <div style="width: 500px; color: #1b3139; margin-left: 50px; float: left">
# MAGIC     <div style="color: #ff5f46; font-size:80px">90%</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC       Enterprise applications will be AI-augmented by 2025 — IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px">$10T+</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC        Projected business value creation by AI in 2030 — PwC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC
# MAGIC   <div class="right_box">
# MAGIC       But a huge challenge is getting ML to work at scale!<br/><br/>
# MAGIC       Most ML projects still fail before getting to production.
# MAGIC   </div>
# MAGIC   
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03.2-AutoML-credit-decisioning&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## So what makes machine learning and data science difficult?
# MAGIC
# MAGIC These are the top challenges we have observed companies struggle with:
# MAGIC 1. Inability to ingest the required data in a timely manner,
# MAGIC 2. Inability to properly control the access of the data,
# MAGIC 3. Inability to trace problems in the feature store to the raw data,
# MAGIC
# MAGIC ... and many other data-related problems.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data-centric Machine Learning
# MAGIC
# MAGIC In Databricks, machine learning is not a separate product or service that needs to be "connected" to the data. The Lakehouse being a single, unified product, machine learning in Databricks "sits" on top of the data, so challenges like inability to discover and access data no longer exist.
# MAGIC
# MAGIC <br />
# MAGIC <img src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/MLontheLakehouse.png" width="1300px" />

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Credit Scoring default prediction
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-2.png" style="float: right" width="800px">
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
fs = feature_store.FeatureStoreClient()
features_set = fs.read_table(name=f"{catalog}.{db}.credit_decisioning_features")
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

# combine both oversampled minority rows and undersampled majority rows, this will improve our balance while preseving enough information.
train_df = undersampled_df.unionAll(oversampled_df).drop('cust_id').na.fill(0)
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
xp_path = "/Shared/dbdemos/experiments/lakehouse-fsi-credit-decisioning"
xp_name = f"automl_credit_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
automl_run = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = train_df.sample(0.1),
    target_col = "defaulted",
    timeout_minutes = 10
)
#Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying our model in production
# MAGIC
# MAGIC Our model is now ready. We can review the notebook generated by the auto-ml run and customize if if required.
# MAGIC
# MAGIC For this demo, we'll consider that our model is ready and deploy it in production in our Model Registry:

# COMMAND ----------

model_name = "dbdemos_fsi_credit_decisioning"
from mlflow import MlflowClient

#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()
try:
  #Get the model if it is already registered to avoid re-deploying the endpoint
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
  print(f"Our model is already deployed on UC: {catalog}.{db}.{model_name}")
except:  
  #Enable Unity Catalog with mlflow registry
  #Add model within our catalog
  latest_model = mlflow.register_model(f'runs:/{summary.best_trial.mlflow_run_id}/model', f"{catalog}.{db}.{model_name}")
  # Flag it as Production ready using UC Aliases
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)
  DBDemos.set_model_permission(f"{catalog}.{db}.{model_name}", "ALL_PRIVILEGES", "account users")

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our automl model as production ready! 
# MAGIC
# MAGIC Open [the dbdemos_fsi_credit_decisioning model](#mlflow/models/dbdemos_fsi_credit_decisioning) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our model predicting default risks is now deployed in production
# MAGIC
# MAGIC
# MAGIC So far we have:
# MAGIC * ingested all required data in a single source of truth,
# MAGIC * properly secured all data (including granting granular access controls, masked PII data, applied column level filtering),
# MAGIC * enhanced that data through feature engineering,
# MAGIC * used MLFlow AutoML to track experiments and build a machine learning model,
# MAGIC * registered the model.
# MAGIC
# MAGIC ### Next steps
# MAGIC We're now ready to use our model use it for:
# MAGIC
# MAGIC - Batch inferences in notebook [03.3-Batch-Scoring-credit-decisioning]($./03.3-Batch-Scoring-credit-decisioning) to start using it for identifying currently underbanked customers with good credit-worthiness (**increase the revenue**) and predict current credit-owners who might default so we can prevent such defaults from happening (**manage risk**),
# MAGIC - Real time inference with [03.4-model-serving-BNPL-credit-decisioning]($./03.4-model-serving-BNPL-credit-decisioning) to enable ```Buy Now, Pay Later``` capabilities within the bank.
# MAGIC
# MAGIC Extra: review model explainability & fairness with [03.5-Explainability-and-Fairness-credit-decisioning]($./03.5-Explainability-and-Fairness-credit-decisioning)
