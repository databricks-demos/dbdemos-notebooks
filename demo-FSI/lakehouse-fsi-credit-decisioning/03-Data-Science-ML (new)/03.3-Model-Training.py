# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Model Training
# MAGIC
# MAGIC In Databricks, machine learning is not a separate product or service that needs to be "connected" to the data. The Lakehouse being a single, unified product, machine learning in Databricks "sits" on top of the data, so challenges like inability to discover and access data no longer exist.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/06-Responsible-AI/images/architecture_3.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Building a Responsible Credit Scoring Model
# MAGIC
# MAGIC With our credit decisioning data prepared, we can now leverage it to build a predictive model assessing customer creditworthiness. Our approach will emphasize transparency, fairness, and governance at every step.
# MAGIC
# MAGIC Steps in Model Training:
# MAGIC - **Retrieve Data from the Feature Store:** We begin by accessing the curated and validated features stored in the Databricks Feature Store. This ensures consistency, traceability, and compliance with feature engineering best practices.
# MAGIC - **Create the Training Dataset:** We assemble a well-balanced training dataset by selecting relevant features and handling missing or biased data points.
# MAGIC - **Leverage Databricks AutoML:** To streamline model development, we use Databricks AutoML to automatically build and evaluate multiple models. This step ensures we select the most effective model while adhering to Responsible AI principles.

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
# MAGIC ## Adressing the class imbalance of our dataset
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

# MAGIC %md
# MAGIC ## Addressing Demographic or Ethical Bias
# MAGIC
# MAGIC When dealing with demographic or ethical bias—where disparities exist across sensitive attributes like gender, race, or age—standard class balancing techniques may be insufficient. Instead, more targeted strategies are used to promote fairness. Pre-processing methods like reweighing assign different instance weights to ensure equitable representation across groups. Techniques such as the disparate impact remover modify feature values to reduce bias while preserving predictive utility. In-processing approaches like adversarial debiasing involve training the main model alongside an adversary that attempts to predict the sensitive attribute, thereby encouraging the model to learn representations that are less biased. Additionally, fair sampling methods, such as Kamiran’s preferential sampling, selectively sample training data to correct for group imbalances. These approaches aim to improve fairness metrics like demographic parity or equal opportunity while maintaining model performance.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating credit scoring model creation using MLFlow and Databricks AutoML
# MAGIC
# MAGIC MLFlow is an open source project allowing model tracking, packaging and deployment. Every time your Data Science team works on a model, Databricks will track all parameters and data used and will auto-log them. This ensures ML transparency, traceability, and reproducibility, making it easy to know what parameters/data were used to build each model and model version.
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

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Generate Model Documentation
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/2023-06/model-risk-management-with-ey-inbody.png?v=1738330714" width="480px"  style="float: right; border: 10px solid white;"/>
# MAGIC
# MAGIC Once we have trained the model, we generate comprehensive model documentation using [Databricks' solutions accelerator](https://www.databricks.com/solutions/accelerators/model-risk-management-with-ey). This step is critical for maintaining compliance, governance, and transparency in financial services.
# MAGIC
# MAGIC #### Benefits of Automated Model Documentation:
# MAGIC
# MAGIC - Streamlined Model Governance: Automatically generate structured documentation for both machine learning and non-machine learning models, ensuring regulatory alignment.
# MAGIC - Integrated Data Visualization & Reporting: Provide insights into model performance with built-in dashboards and visual analytics.
# MAGIC - Risk Identification for Banking Models: Help model validation teams detect and mitigate risks associated with incorrect or misused models in financial decisioning.
# MAGIC - Foundations for Explainable AI: Enhance trust by making every stage of the model lifecycle transparent, accelerating model validation and deployment processes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC With our model trained and documented, the next step is validation. In the [06.4-Model-Validation]($./06-Responsible-AI/06.4-Model-Validation) notebook, we will conduct compliance checks, pre-deployment tests, and fairness evaluations. This ensures that our model meets regulatory requirements and maintains transparency before it is deployed into production.

# COMMAND ----------


