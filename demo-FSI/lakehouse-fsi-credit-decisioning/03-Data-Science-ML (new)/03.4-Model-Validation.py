# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Model Validation
# MAGIC
# MAGIC Model validation is a critical step in ensuring the compliance, fairness, and reliability of our credit scoring model before deployment. This notebook performs key compliance checks to align with Responsible AI principles. Specifically, we:
# MAGIC
# MAGIC - Validate model fairness for existing credit customers.
# MAGIC - Analyze feature importance and model behavior using Shapley values.
# MAGIC - Log custom metrics for auditing and transparency.
# MAGIC - Ensure compliance with regulatory fairness constraints.
# MAGIC
# MAGIC In addition, we also implement the champion-challenger framework to ensure that only the most effective and responsible model is promoted to a higher enviroment. This approach allows us to:
# MAGIC - Compare the current production model (champion model) with a newly trained model (challenger model).
# MAGIC - Incorporate human oversight to validate the challenger model before deployment.
# MAGIC - Maintain full traceability and accountability at each stage of model deployment.
# MAGIC
# MAGIC Model validation includes both the model compliance checks and champion-challenger testing. It is only after a model passes these two checks that it is allowed to progress to a high environment. To do so, we:
# MAGIC - Register the validated model in Unity Catalog and transition it to the next stage.
# MAGIC
# MAGIC By following this structured validation approach, we ensure that model transitions are transparent, fair, and aligned with Responsible AI principles.
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/06-Responsible-AI/images/architecture_4.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install --quiet shap==0.46.0
# MAGIC dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load Data
# MAGIC
# MAGIC To validate our model, we first load the necessary data from `credit_decisioning_features` and `credit_bureau_gold` tables. These datasets provide customer financial and credit bureau insights necessary for validation.

# COMMAND ----------

feature_df = spark.table("credit_decisioning_features")
credit_bureau_label = spark.table("credit_bureau_gold")
                   
df = (feature_df.join(credit_bureau_label, "cust_id", how="left")
               .withColumn("defaulted", F.when(col("CREDIT_DAY_OVERDUE").isNull(), 2)
                                         .when(col("CREDIT_DAY_OVERDUE") > 60, 1)
                                         .otherwise(0))
               .drop('CREDIT_DAY_OVERDUE')
               .fillna(0))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load model
# MAGIC
# MAGIC We retrieve our trained model from the Unity Catalog model registry

# COMMAND ----------

import mlflow

mlflow.set_registry_uri('databricks-uc')

model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.{model_name}@none")
features = model.metadata.get_input_schema().input_names()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ensuring model fairness for existing credit customers
# MAGIC
# MAGIC In this example, we'll make sure that our model behaves as expected and is fair for our existing customers.
# MAGIC
# MAGIC We'll select our existing customers not having credit and make sure that our model is fair and behave the same among different group of the population.

# COMMAND ----------

underbanked_df = df[df.defaulted==2].toPandas() # Features for underbanked customers
banked_df = df[df.defaulted!=2].toPandas() # Features for our existing credit customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance using Shapley values
# MAGIC
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation.<br />
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the underbanked set to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC - SHAP cannot explain models using data with nulls; if your dataset has any, both the background data and
# MAGIC   examples to explain will be imputed using the mode (most frequent values). This affects the computed
# MAGIC   SHAP values, as the imputed samples may not match the actual data distribution.
# MAGIC
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).

# COMMAND ----------

import shap

mlflow.autolog(disable=True)
mlflow.sklearn.autolog(disable=True)

train_sample = banked_df[features].sample(n=np.minimum(100, banked_df.shape[0]), random_state=42)
underbanked_sample = underbanked_df.sample(n=np.minimum(100, underbanked_df.shape[0]), random_state=42)

# Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
predict = lambda x: model.predict(pd.DataFrame(x, columns=features).astype(train_sample.dtypes.to_dict()))

explainer = shap.KernelExplainer(predict, train_sample, link="identity")
shap_values = explainer.shap_values(underbanked_sample[features], l1_reg=False, nsamples=100)

# COMMAND ----------

# DBTITLE 1,Save feature importance
import matplotlib.pyplot as plt
import os

shap.summary_plot(shap_values, underbanked_sample[features], show=False)
plt.savefig(f"{os.getcwd()}/images/shap_feature_importance.png") 

# COMMAND ----------

# MAGIC %md
# MAGIC Shapley values can also help for the analysis of local, instance-wise effects. 
# MAGIC
# MAGIC We can also easily explain which feature impacted the decision for a given user. This can helps agent to understand the model an apply additional checks or control if required.

# COMMAND ----------

# DBTITLE 1,Explain feature importance for a single customer
#shap.initjs()
#We'll need to add shap bundle js to display nice graph
with open(shap.__file__[:shap.__file__.rfind('/')]+"/plots/resources/bundle.js", 'r') as file:
   shap_bundle_js = '<script type="text/javascript">'+file.read()+';</script>'

html = shap.force_plot(explainer.expected_value, shap_values[0,:], banked_df[features].iloc[0,:])
displayHTML(shap_bundle_js + html.html())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model fairness using Shapley values
# MAGIC
# MAGIC In order to detect discriminatory outcomes in Machine Learning predictions, it is important to evaluate how the model treats various customer groups. This can be achieved by devising a metric, such as such as demographic parity, equal opportunity or equal odds, that defines fairness within the model. For example, when considering credit decisioning, we can compare the credit approval rates of male and female customers. In the notebook, we utilize Demographic Parity as a statistical measure of fairness, which asserts that there should be no difference between groups obtaining positive outcomes (e.g., credit approvals) in an ideal scenario. However, such perfect equality is rare, underscoring the need to monitor and address any gaps or discrepancies.

# COMMAND ----------

gender_array = banked_df['gender'].replace({'Female':0, 'Male':1}).to_numpy()[:100]
shap.group_difference_plot(shap_values.sum(1), \
                           gender_array, \
                           xmin=-1.0, xmax=1.0, \
                           xlabel="Demographic parity difference\nof model output for women vs. men")

# COMMAND ----------


shap_df = pd.DataFrame(shap_values, columns=features).add_suffix('_shap')
shap.group_difference_plot(shap_df[['age_shap', 'tenure_months_shap']].to_numpy(), \
                           gender_array, \
                           feature_names=['age', 'tenure_months'], 
                           xmin=-0.5, xmax=0.5, \
                           xlabel="Demographic parity difference\nof SHAP values for women vs. men")                        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging custom metrics/artifacts with **MLflow**

# COMMAND ----------

# Retrieve model version by alias
client = mlflow.tracking.MlflowClient()
model_version_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="none")

# Log new artifacts in the same experiment
with mlflow.start_run(run_id=model_version_info.run_id):
    # Log SHAP feature importance
    mlflow.log_artifact(f"{os.getcwd()}/images/shap_feature_importance.png")

    #Log Demographic parity difference\nof model output for women vs. men
    mean_shap_male = np.mean(shap_values[gender_array == 1])
    mean_shap_female = np.mean(shap_values[gender_array == 0])
    mean_difference = mean_shap_male - mean_shap_female
    mlflow.log_metric("shap_demo_parity_diff_wm", mean_shap_male - mean_shap_female)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compliance checks
# MAGIC
# MAGIC Our model is demographic parity difference metric is logged with the model. The model is registered in Unity Catalog with the 'None' alias. 
# MAGIC
# MAGIC Let's assume that the absolute demographic parity difference of model output for women vs. men should be less than 0.1 to model to pass the compliance checks.
# MAGIC

# COMMAND ----------

import mlflow

# Retrieve experiment run by alias
client = mlflow.tracking.MlflowClient()
model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="none")
run = client.get_run(model_info.run_id)

# Retrieve a specific metric, such as 'shap_demo_parity_diff_wm'
shap_demo_parity_diff_wm = run.data.metrics.get("shap_demo_parity_diff_wm")

# COMMAND ----------

# Check whether the metric passes the requirements

compliance_checks_passed = False

if abs(shap_demo_parity_diff_wm) < 0.1:
  compliance_checks_passed = True
  print("compliance checks passed")
else:
  print("compliance checks failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Champion-Challenger testing
# MAGIC
# MAGIC ### Identify the current champion and challenger models
# MAGIC
# MAGIC If there is a model already in production, we define it as the current `champion model`. The model with the 'None' alias is defined as the `challenger model`.

# COMMAND ----------

# Set the Challenger model to the model with the None alias
challenger_model_info = model_info
challenger_run = run

# Set the Champion model to the model with the Production alias
try:
    champion_model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="production")
except Exception as e:
    print(e)
    champion_model_info = None
if champion_model_info is not None:
  champion_run = client.get_run(champion_model_info.run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we compare the perfomance of the two models. In this case, we use the `val_f1_score` metric. The model with the highest `val_f1_score` is the new candidate for the `champion model`.

# COMMAND ----------

champion_challenger_test_passed = False

if champion_model_info is None:
  # No champion model. Challenger model becomes the champion. Mark test as passed.
  champion_challenger_test_passed = True
  print("No champion model. Challenger model becomes the champion.")
elif challenger_run.data.metrics['val_f1_score'] > champion_run.data.metrics['val_f1_score']:
  # Challenger model is better than champion model. Mark test as passed.
  champion_challenger_test_passed = True
  print("Challenger model performs better than champion.")
else:
  print("Challenger model does not perform better than champion.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update validation status
# MAGIC
# MAGIC Having done both compliance check and champion-challenger testing, we will now update the model's validation status. Only models that have passed both checks are approved to progress further into higher environments.
# MAGIC
# MAGIC For auditability, we also apply tags on the version of the model in Unity Catalog that we are now reviewing to record the status of the tests and validation check.

# COMMAND ----------

# Indicate if compliance checks has passed
pass_fail = "failed"
if compliance_checks_passed:
  pass_fail = "passed"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, "compliance_checks", pass_fail)

# Indicate if champion-challenger test has passed
pass_fail = "failed"
if champion_challenger_test_passed:
  pass_fail = "passed"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, "champion_challenger_test", pass_fail)

# Model Validation Status is 'approved' only if both compliance checks and champion-challenger test have passed
# Otherwise Model Validation Status is 'rejected'
validation_status = "Not validated"
if compliance_checks_passed & champion_challenger_test_passed:
  validation_status = "approved"
else:
  validation_status = "rejected"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, "validation_status", validation_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote model to Staging
# MAGIC
# MAGIC Our model is now ready to be moved to the next stage. For this demo, we'll consider that our model is approved after going through the validation checks. It's now ready to be promoted to `Staging`. In `Staging`, the model will go through integration testing, before being deployed to `Prodution`.
# MAGIC
# MAGIC Otheriwse, if the model is rejected after going through the validation checks, we archive the model by setting its alias to `Archived`.
# MAGIC
# MAGIC Before promoting the model into `Staging`, there can be a human validation of the model involved.

# COMMAND ----------

# The production model is the champion model used in champion-challenger testing above
# We will use the prod_model_info variable here for the code to be easier to understand
prod_model_info = champion_model_info

if prod_model_info is None:
  # No model in production. Set this model as the candidate production model by promoting it to Staging
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="none")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="staging", version=model_info.version)
  print(f'{model_info.version} of {catalog}.{db}.{model_name} is now promoted to Staging.')
elif validation_status == "approved":
  # This model has passed validation checks. Promote it to Staging.
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="none")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="staging", version=model_info.version)
  print(f'{model_info.version} of {catalog}.{db}.{model_name} is now promoted to Staging.')
else:
  # This model did not pass validation checks. Set it to Archived.
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="none")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="archived", version=model_info.version)
  client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, f"archived", "true")
  print(f'{model_info.version} of {catalog}.{db}.{model_name} is transitioned to Archived. No model promoted to Staging.')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Store Data (into Delta format) for Downstream Usage
# MAGIC
# MAGIC Finally, we store the validated dataset in Delta format for auditing and future reference

# COMMAND ----------

#Let's load the underlying model to get the proba
try:
  skmodel = mlflow.sklearn.load_model(model_uri=f"models:/{catalog}.{db}.{model_name}@Staging")
  underbanked_sample['default_prob'] = skmodel.predict_proba(underbanked_sample[features])[:,1]
  underbanked_sample['prediction'] = skmodel.predict(underbanked_sample[features])
  final_df = pd.concat([underbanked_sample.reset_index(), shap_df], axis=1)

  final_df = spark.createDataFrame(final_df).withColumn("default_prob", col("default_prob").cast('double'))
  display(final_df)
  final_df.drop('CREDIT_CURRENCY', '_rescued_data', 'index') \
        .write.mode("overwrite").option('OverwriteSchema', True).saveAsTable(f"shap_explanation")
except:
  print("No model in staging.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next step [06.5-Model-Integration]($./06-Responsible-AI/06.5-Model-Integration), we will conduct integration testing on the model with other components that use it. An example of that is a batch inference pipeline. This is how the model is deployed responsibly, ensuring traceability and accountability at each decision point, while ensuring the integrity of systems and application where the model is used.
