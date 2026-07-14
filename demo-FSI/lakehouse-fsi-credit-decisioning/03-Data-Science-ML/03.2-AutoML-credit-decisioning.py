# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science on the Databricks Lakehouse
# MAGIC
# MAGIC ## ML is key to disruption & personalization
# MAGIC
# MAGIC Being able to ingest and query our credit-related database is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC
# MAGIC Customers now expect real-time personalization and new forms of communication. Modern data companies achieve this with AI.
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

# MAGIC %uv pip install databricks-sdk mlflow==3.14.0 databricks-feature-engineering scikit-learn optuna
# MAGIC dbutils.library.restartPython()

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

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Balancing our dataset
# MAGIC
# MAGIC Let's downsample and upsample our dataset to improve our model performance

# COMMAND ----------

major_df = training_dataset.filter(col("defaulted") == 0)
minor_df = training_dataset.filter(col("defaulted") == 1)

# Duplicate the minority rows
oversampled_df = minor_df.union(minor_df)

# Downsample majority rows
undersampled_df = major_df.sample(oversampled_df.count() / major_df.count() * 3, 42)

# Combine both oversampled minority rows and undersampled majority rows
train_df = undersampled_df.unionAll(oversampled_df).drop('cust_id').na.fill(0)

# Save it as a table to be able to select it with the AutoML UI
train_df.write.mode('overwrite').saveAsTable('credit_risk_train_df')
train_df = spark.table('credit_risk_train_df')

# Visualize the credit default ratio
px.pie(train_df.groupBy('defaulted').count().toPandas(), values='count', names='defaulted', title='Credit default ratio')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating credit scoring model creation with Databricks Genie Code
# MAGIC The Data Science Agent (Genie Code) elevates the Databricks Assistant from a helpful copilot into a true autonomous partner for data science and analytics. Fully integrated with Databricks Notebooks and the SQL Editor, the [Data Science Agent](https://www.databricks.com/blog/introducing-databricks-assistant-data-science-agent) let's you analyze your data and build ML models in a few clicks/prompt.
# MAGIC <img src="https://www.databricks.com/sites/default/files/2025-09/AgentModeOG1Border.png?v=1756901406" width="500px" style="float: right"/>
# MAGIC
# MAGIC - It transforms Databricks Assistant into an autonomous partner for data science and analytics tasks in Notebooks and the SQL Editor.
# MAGIC
# MAGIC - It can explore data, generate and run code, and fix errors, all from a single prompt. This can cut hours of work to minutes.
# MAGIC
# MAGIC - Purpose-built for common data science tasks and grounded in Unity Catalog for seamless, governed access to your data.
# MAGIC
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC ### Using Databricks Genie Code with our Credit Scoring dataset
# MAGIC
# MAGIC All we have to do is describe our problem to Genie Code and point it at the feature table we just created (`credit_decisioning_features`), with `defaulted` as our prediction target.
# MAGIC
# MAGIC Below is the training code Genie Code generated for us — a standard scikit-learn pipeline, fully tracked in MLflow (parameters, metrics and **dataset lineage**) for reproducibility and governance.

# COMMAND ----------

# DBTITLE 1,Training code generated by Databricks Genie Code
import mlflow, optuna
from mlflow.models.signature import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import f1_score

model_name = "dbdemos_fsi_credit_decisioning"
target_col = "defaulted"
# Shared experiment path (must be a workspace /Shared path, not a local one, so it works in
# jobs running from the GitHub repo). set_experiment creates it if missing, or reuses it.
xp_path = "/Shared/dbdemos/experiments/lakehouse-fsi-credit-decisioning"
# xp_path is a directory (experiments are created under it); set_experiment needs a
# leaf experiment path, so append a run name to avoid a DIRECTORY-vs-EXPERIMENT conflict.
mlflow.set_experiment(f"{xp_path}/genie_code_run")
DBDemos.set_experiment_permission(xp_path)

pdf = train_df.sample(0.1).toPandas()
X = pdf.drop(columns=[target_col])
y = pdf[target_col]
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Standard sklearn preprocessing: impute numerics, ordinal-encode categoricals.
num_cols = X.select_dtypes(include="number").columns.tolist()
cat_cols = [c for c in X.columns if c not in num_cols]
preprocessor = ColumnTransformer([
    ("num", SimpleImputer(strategy="median"), num_cols),
    ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                      ("enc", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1))]), cat_cols),
])

def build_pipeline(params):
    return Pipeline([("preprocessor", preprocessor),
                     ("classifier", RandomForestClassifier(random_state=42, n_jobs=-1, **params))])

mlflow.sklearn.autolog(disable=True)
with mlflow.start_run(run_name="genie_code_credit_scoring") as genie_run:
    # Track dataset lineage: link the training data to this run for full governance.
    training_dataset = mlflow.data.from_pandas(pdf, name="credit_decisioning_features", targets=target_col)
    mlflow.log_input(training_dataset, context="training")

    # Hyperparameter search with Optuna, each trial logged to MLflow as a nested run.
    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 300),
            "max_depth": trial.suggest_int("max_depth", 3, 12),
            "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 20),
        }
        with mlflow.start_run(nested=True, run_name=f"trial_{trial.number}"):
            mlflow.log_params(params)
            score = cross_val_score(build_pipeline(params), X_train, y_train, cv=3, scoring="f1_weighted").mean()
            mlflow.log_metric("cv_f1_weighted", score)
        return score

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=20)
    mlflow.log_params(study.best_params)

    # Refit the best model on the full training split and evaluate on the hold-out set.
    best_model = build_pipeline(study.best_params)
    best_model.fit(X_train, y_train)
    val_f1 = f1_score(y_val, best_model.predict(X_val), average="weighted")
    mlflow.log_metric("val_f1_score", val_f1)

    signature = infer_signature(X_train, best_model.predict(X_train))
    # cloudpickle keeps the model self-contained (no databricks-automl-runtime) so it loads on serverless.
    mlflow.sklearn.log_model(best_model, name="model", input_example=X_train.iloc[[0]],
                             signature=signature, serialization_format="cloudpickle")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying our model in production
# MAGIC
# MAGIC Our model is now ready. We can review the code generated by Genie Code and customize it if required.
# MAGIC
# MAGIC For this demo, we'll consider that our model is ready and deploy it in production in our Model Registry:

# COMMAND ----------

model_name = "dbdemos_fsi_credit_decisioning"
from mlflow import MlflowClient
import mlflow

#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()

#Add model within our catalog
latest_model = mlflow.register_model(f'runs:/{genie_run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
# Flag it as Production ready using UC Aliases
client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)
#DBDemos.set_model_permission(f"{catalog}.{db}.{model_name}", "ALL_PRIVILEGES", "account users")

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our Genie Code-generated model as production ready!
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
