# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Use the best AutoML generated model to batch score credit worthiness
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
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03.3-Batch-Scoring-credit-decisioning&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Running batch inference to score our existing database
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-5.png" style="float: right" width="800px">
# MAGIC
# MAGIC <br/><br/>
# MAGIC Now that our model was created and deployed in production within the MLFlow registry.
# MAGIC
# MAGIC <br/>
# MAGIC We can now easily load it calling the `Production` stage, and use it in any Data Engineering pipeline (a job running every night, in streaming or even within a Delta Live Table pipeline).
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC We'll then save this information as a new table without our FS database, and start building dashboards and alerts on top of it to run live analysis.

# COMMAND ----------

model_name = "dbdemos_fsi_credit_decisioning"
mlflow.set_registry_uri('databricks-uc')

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@prod", result_type='double')

# COMMAND ----------

features = loaded_model.metadata.get_input_schema().input_names()

underbanked_df = spark.table("credit_decisioning_features").fillna(0) \
                   .withColumn("prediction", loaded_model(F.struct(*features))).cache()

display(underbanked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In the scored data frame above, we have essentially created an end-to-end process to predict credit worthiness for any customer, regardless of whether the customer has an existing bank account. We have a binary prediction which captures this and incorporates all the intellience from Databricks AutoML and curated features from our feature store.

# COMMAND ----------

underbanked_df.write.mode("overwrite").saveAsTable(f"underbanked_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Next steps
# MAGIC
# MAGIC * Deploy your model for real time inference with [03.4-model-serving-BNPL-credit-decisioning]($./03.4-model-serving-BNPL-credit-decisioning) to enable ```Buy Now, Pay Later``` capabilities within the bank.
# MAGIC
# MAGIC Or
# MAGIC
# MAGIC * Making sure your model is fair towards customers of any demographics are extremely important parts of building production-ready ML models for FSI use cases. <br/>
# MAGIC Explore your model with [03.5-Explainability-and-Fairness-credit-decisioning]($./03.5-Explainability-and-Fairness-credit-decisioning) on the Lakehouse.
