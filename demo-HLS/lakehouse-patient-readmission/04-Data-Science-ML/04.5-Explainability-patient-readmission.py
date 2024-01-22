# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Patient readmission - Model Explainability 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/patient-risk-ds-flow-5.png?raw=true" width="700px" style="float: right; margin-left: 10px;" />
# MAGIC
# MAGIC Being able to understand our model prediction and which condition or criteria the readmission risk is key to increase healthcare quality.
# MAGIC
# MAGIC In this example, we'll explain our model for the entire cohort, but also provide explanation for a specific patient.
# MAGIC
# MAGIC This information can be used to improve global care but also provide more context for a specific patient.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false $catalog=dbdemos $db=hls_patient_readmission

# COMMAND ----------

#For this demo, reuse our dataset to test the batch inferences
dataset_to_explain = spark.table('training_dataset')
dataset_to_explain.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Model from the registry

# COMMAND ----------

#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')
client = mlflow.tracking.MlflowClient()

model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.dbdemos_hls_patient_readmission@prod")
features = model.metadata.get_input_schema().input_names()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance using Shapley values
# MAGIC
# MAGIC
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot of the relationship between features and model output. Features are ranked in descending order of importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation.<br />
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the cohort to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).

# COMMAND ----------

mlflow.autolog(disable=True)
mlflow.sklearn.autolog(disable=True)

df = dataset_to_explain.sample(fraction=0.1).toPandas()

train_sample = df[features].sample(n=np.minimum(100, df.shape[0]), random_state=42)

# Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
predict = lambda x: model.predict(pd.DataFrame(x, columns=features).astype(train_sample.dtypes.to_dict()))

explainer = shap.KernelExplainer(predict, train_sample, link="identity")
shap_values = explainer.shap_values(train_sample, l1_reg=False, nsamples=100)

# COMMAND ----------

# DBTITLE 1,Plot most important features
import plotly.express as px
mean_abs_shap = np.absolute(shap_values).mean(axis=0).tolist()
df = pd.DataFrame(list(zip(mean_abs_shap,features)), columns=['SHAP_value', 'feature'])
px.bar(df.sort_values('SHAP_value', ascending=False).head(10), x='feature', y='SHAP_value')

# COMMAND ----------

# DBTITLE 1,Feature impact
shap.summary_plot(shap_values, train_sample)

# COMMAND ----------

# MAGIC %md
# MAGIC Shapely values can also help for the analysis of local, instance-wise effects. 
# MAGIC
# MAGIC We can also easily explain which feature impacted the decision for a given user. This can helps agent to understand the model an personalized health care further.

# COMMAND ----------

# DBTITLE 1,Explain risk for an individual
#We'll need to add shap bundle js to display nice graph
with open(shap.__file__[:shap.__file__.rfind('/')]+"/plots/resources/bundle.js", 'r') as file:
   shap_bundle_js = '<script type="text/javascript">'+file.read()+'</script>'

html = shap.force_plot(explainer.expected_value, shap_values[0,:], train_sample.iloc[0,:])
displayHTML(shap_bundle_js + html.html())

# COMMAND ----------

# MAGIC %md
# MAGIC If we take many individual explanations such as the one shown above, rotate them 90 degrees, and then stack them horizontally, we can see explanations for an entire dataset:

# COMMAND ----------

plot_html = shap.force_plot(explainer.expected_value, shap_values, train_sample)
displayHTML(shap_bundle_js + plot_html.html())

# COMMAND ----------

# MAGIC %md To understand how a single feature effects the output of the model we can plot the SHAP value of that feature vs. the value of the feature for all the examples in a dataset. 
# MAGIC
# MAGIC Since SHAP values represent a feature's responsibility for a change in the model output, the plot below represents the change in the readmission risk as TOTAL_CLAIM_COST changes. 
# MAGIC
# MAGIC To help reveal these interactions dependence_plot can selects another feature for coloring, in this case TOTAL_CLAIM_COST, showing in this case no direct relation.

# COMMAND ----------

shap.dependence_plot("INCOME", shap_values, train_sample[features], interaction_index="TOTAL_CLAIM_COST")

# COMMAND ----------

# MAGIC %md #### Computing SHAP values on the entier dataset:
# MAGIC These graph are great to understand the model against a subset of data. If we want to to further analyze based on the shap values on millions on rows, we can use spark to compute the shap values.
# MAGIC
# MAGIC We can use spark 3 `mapInPandas` function, or create a `@pandas_udf`:
# MAGIC

# COMMAND ----------

import pandas as pd
def compute_shap_values(iterator):
  for X in iterator:
    yield pd.DataFrame(explainer.shap_values(X, check_additivity=False))

df = dataset_to_explain.mapInPandas(compute_shap_values, schema=", ".join([x+"_shap_value float" for x in features]))

# Skip as this can take some time to run
#display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Conclusion: the power of the Lakehouse
# MAGIC
# MAGIC In this demo, we've seen an end 2 end flow with the Lakehouse:
# MAGIC
# MAGIC
# MAGIC - Data ingestion made simple with Delta Live Table
# MAGIC - Leveraging Databricks notebooks and SQL warehouse to create, anaylize and share our dashboards 
# MAGIC - Model Training with AutoML for citizen Data Scientist
# MAGIC - Ability to tune our model for better results, improving our patient journey quality
# MAGIC - Ultimately, the ability to deploy and make explainable ML predictions, made possible with the full Lakehouse capabilities.
# MAGIC
# MAGIC [Go back to the introduction]($../00-patient-readmission-introduction) or discover how to use Databricks Workflow to orchestrate this tasks: [05-Workflow-Orchestration-patient-readmission]($../05-Workflow-Orchestration/05-Workflow-Orchestration-patient-readmission)
