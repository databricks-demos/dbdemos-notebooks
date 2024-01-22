# Databricks notebook source
# MAGIC %md
# MAGIC ## Churn Prediction Inference - Batch or real-time
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

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="hive_metastore"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC 
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC 
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC 
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC 
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry !

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Model
# MAGIC 
# MAGIC Loading as a Spark UDF to set us up for future scale.

# COMMAND ----------

model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/dbdemos_mlops_churn/Staging")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Features
# MAGIC 
# MAGIC The features names are linked to our model. We can extract them from the model we just loaded.

# COMMAND ----------

model_features = model.metadata.get_input_schema().input_names()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run inferences

# COMMAND ----------

fs = FeatureStoreClient()
features = fs.read_table(f'{dbName}.dbdemos_mlops_churn_features')

predictions = features.withColumn('churnPredictions', model(*model_features))
display(predictions.select("customer_id", "churnPredictions"))

#Note: this could also be executed in SQL:
#spark.udf.register("predict_churn", model)
#spark.sql(f"""SELECT customer_id, predict_churn(struct(`{"`,`".join(model_features)}`)) as prediction from {dbName}.dbdemos_mlops_churn_features""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta Lake
# MAGIC That's it! Our data can now be saved as a table and re-used by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!

# COMMAND ----------

predictions.write.mode("overwrite").saveAsTable("churn_customers_predictions")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for real-time inferences
# MAGIC 
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_realtime_inference.gif" />
# MAGIC 
# MAGIC Our marketing team also needs to run inferences in real-time using REST api (send a customer ID and get back the inference).
# MAGIC 
# MAGIC While Feature store integration in real-time serving will come with Model Serving v2, you can deploy your Databricks Model in a single click.
# MAGIC 
# MAGIC Open the Model page and click on "Serving". It'll start your model behind a REST endpoint and you can start sending your HTTP requests!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Next: Building a dashboard with Customer Churn information
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-dashboard.png" width="600px" style="float:right"/>
# MAGIC 
# MAGIC We now have all our data ready, including customer churn. 
# MAGIC 
# MAGIC The Churn table containing analysis and Churn predictions can be shared with the Analyst and Marketing team.
# MAGIC 
# MAGIC With Databricks SQL, we can build our Customer Churn monitoring Dashboard to start tracking our Marketing campaign effect!

# COMMAND ----------

# MAGIC %md
# MAGIC Next:
# MAGIC * [Automate model re-training]($./07_retrain_churn_automl) (Optional)
# MAGIC * [Explore DBSQL Churn Dashboard](/sql/dashboards/f25702b4-56d8-40a2-a69d-d2f0531a996f-churn-prediction-dashboard---universal?o=1444828305810485#)
