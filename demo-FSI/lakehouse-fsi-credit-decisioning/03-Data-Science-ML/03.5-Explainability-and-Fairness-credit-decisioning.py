# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Credit Decisioning - Model Explainability and Fairness
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-4.png" style="float: right" width="800px">
# MAGIC
# MAGIC
# MAGIC Machine learning (ML) models are increasingly being used in credit decisioning to automate lending processes, reduce costs, and improve accuracy. 
# MAGIC
# MAGIC As ML models become more complex and data-driven, their decision-making processes can become opaque, making it challenging to understand how decisions are made, and to ensure that they are fair and non-discriminatory. 
# MAGIC
# MAGIC Therefore, it is essential to develop techniques that enable model explainability and fairness in credit decisioning to ensure that the use of ML does not perpetuate existing biases or discrimination. 
# MAGIC
# MAGIC In this context, explainability refers to the ability to understand how an ML model is making its decisions, while fairness refers to ensuring that the model is not discriminating against certain groups of people. 
# MAGIC
# MAGIC ## Ensuring model fairness for new credit customers
# MAGIC
# MAGIC In this example, we'll make sure that our model behaves as expected and is fair for our new customers.
# MAGIC
# MAGIC We'll select our existing customers not having credit (We'll flag them as `defaulted = 2`) and make sure that our model is fair and behave the same among different group of the population.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03.5-Explainability-and-Fairness-credit-decisioning&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

# COMMAND ----------

# MAGIC %pip install --quiet shap==0.46.0 mlflow==2.19.0 scikit-learn==1.3.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here we are merging several PII columns (hence we read from the ```customer_silver``` table) with the model prediction output table for visualizing them on the dashboard for end user consumption

# COMMAND ----------

feature_df = spark.table("credit_decisioning_features")
credit_bureau_label = spark.table("credit_bureau_gold")
customer_df = spark.table(f"customer_silver").select("cust_id", "gender", "first_name", "last_name", "email", "mobile_phone")
                   
df = (feature_df.join(customer_df, "cust_id", how="left")
               .join(credit_bureau_label, "cust_id", how="left")
               .withColumn("defaulted", F.when(col("CREDIT_DAY_OVERDUE").isNull(), 2)
                                         .when(col("CREDIT_DAY_OVERDUE") > 60, 1)
                                         .otherwise(0))
               .drop('CREDIT_DAY_OVERDUE')
               .fillna(0))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Model from the registry

# COMMAND ----------

model_name = "dbdemos_fsi_credit_decisioning"
import mlflow
mlflow.set_registry_uri('databricks-uc')

model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.{model_name}@prod")
features = model.metadata.get_input_schema().input_names()

# COMMAND ----------

underbanked_df = df[df.defaulted==2].toPandas() # Features for underbanked customers
banked_df = df[df.defaulted!=2].toPandas() # Features for rest of the customers

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

mlflow.autolog(disable=True)
mlflow.sklearn.autolog(disable=True)

import shap
train_sample = banked_df[features].sample(n=np.minimum(100, banked_df.shape[0]), random_state=42)
underbanked_sample = underbanked_df.sample(n=np.minimum(100, underbanked_df.shape[0]), random_state=42)

# Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
predict = lambda x: model.predict(pd.DataFrame(x, columns=features).astype(train_sample.dtypes.to_dict()))

explainer = shap.KernelExplainer(predict, train_sample, link="identity")
shap_values = explainer.shap_values(underbanked_sample[features], l1_reg=False, nsamples=100)

# COMMAND ----------

shap.summary_plot(shap_values, underbanked_sample[features])

# COMMAND ----------

# MAGIC %md
# MAGIC Shapely values can also help for the analysis of local, instance-wise effects. 
# MAGIC
# MAGIC We can also easily explain which feature impacted the decision for a given user. This can helps agent to understand the model an apply additional checks or control if required.

# COMMAND ----------

# DBTITLE 1,Explain feature importance for a single customer
#shap.initjs()
#We'll need to add shap bundle js to display nice graph
with open(shap.__file__[:shap.__file__.rfind('/')]+"/plots/resources/bundle.js", 'r') as file:
   shap_bundle_js = '<script type="text/javascript">'+file.read()+';</script>'

html = shap.force_plot(explainer.expected_value, shap_values[0,:], underbanked_sample[features].iloc[0,:])
displayHTML(shap_bundle_js + html.html())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model fairness using Shapley values
# MAGIC
# MAGIC In order to detect discriminatory outcomes in Machine Learning predictions, it is important to evaluate how the model treats various customer groups. This can be achieved by devising a metric, such as such as demographic parity, equal opportunity or equal odds, that defines fairness within the model. For example, when considering credit decisioning, we can compare the credit approval rates of male and female customers. In the notebook, we utilize Demographic Parity as a statistical measure of fairness, which asserts that there should be no difference between groups obtaining positive outcomes (e.g., credit approvals) in an ideal scenario. However, such perfect equality is rare, underscoring the need to monitor and address any gaps or discrepancies.

# COMMAND ----------

gender_array = underbanked_df['gender'].replace({'Female':0, 'Male':1}).to_numpy()
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
# MAGIC
# MAGIC ## Store Data (into Delta format) for Downstream Usage
# MAGIC
# MAGIC Since we want to add the Explainability and Fairness assessment in the business dashboards, we will persist this data into Delta format and query it later.

# COMMAND ----------

#Let's load the underlying model to get the proba
skmodel = mlflow.sklearn.load_model(model_uri=f"models:/{catalog}.{db}.{model_name}@prod")
underbanked_sample['default_prob'] = skmodel.predict_proba(underbanked_sample[features])[:,1]
underbanked_sample['prediction'] = skmodel.predict(underbanked_sample[features])
final_df = pd.concat([underbanked_sample.reset_index(), shap_df], axis=1)

final_df = spark.createDataFrame(final_df).withColumn("default_prob", col("default_prob").cast('double'))
display(final_df)
final_df.drop('CREDIT_CURRENCY', '_rescued_data') \
        .write.mode("overwrite").option('mergeSchema', True).saveAsTable(f"shap_explanation")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Conclusion: the power of the Lakehouse
# MAGIC
# MAGIC In this demo, we've seen an end 2 end flow with the Lakehouse:
# MAGIC
# MAGIC - Data ingestion made simple with Delta Live Table
# MAGIC - Leveraging Databricks warehouse to making credit decisions
# MAGIC - Model Training with AutoML for citizen Data Scientist
# MAGIC - Ability to tune our model for better results, improving our revenue
# MAGIC - Ultimately, the ability to deploy and make explainable ML predictions, made possible with the full Lakehouse capabilities.
# MAGIC
# MAGIC [Go back to the introduction]($../00-Credit-Decisioning) or discover how to use Databricks Workflow to orchestrate this tasks: [05-Workflow-Orchestration-credit-decisioning]($../05-Workflow-Orchestration/05-Workflow-Orchestration-credit-decisioning)
