# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science with Databricks
# MAGIC
# MAGIC ## ML is key to disruption & risk reduction
# MAGIC
# MAGIC Being able to ingest and query our banking database is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC
# MAGIC Banking customers now expect real-time personalization and protection. Modern data companies achieve this with AI.
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
# MAGIC       Enterprise applications will be AI-augmented by 2025 —IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px">$10T+</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC        Projected business value creation by AI in 2030 —PWC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC
# MAGIC   <div class="right_box">
# MAGIC       But—huge challenges getting ML to work at scale!<br/><br/>
# MAGIC       Most ML projects still fail before getting to production
# MAGIC   </div>
# MAGIC   
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC ## Machine learning is data + transforms.
# MAGIC
# MAGIC ML is hard because delivering value to business lines isn't only about building a Model. <br>
# MAGIC The ML lifecycle is made of data pipelines: Data-preprocessing, feature engineering, training, inference, monitoring and retraining...<br>
# MAGIC Stepping back, all pipelines are data + code.
# MAGIC
# MAGIC
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-4.png" />
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="float: left;" width="80px"> 
# MAGIC <h3 style="padding: 10px 0px 0px 5px">Marc, as a Data Scientist, needs a data + ML platform accelerating all the ML & DS steps:</h3>
# MAGIC
# MAGIC <div style="font-size: 19px; margin-left: 73px; clear: left">
# MAGIC <div class="badge_b"><div class="badge">1</div> Build Data Pipeline supporting real time (with Lakeflow Pipelines)</div>
# MAGIC <div class="badge_b"><div class="badge">2</div> Data Exploration</div>
# MAGIC <div class="badge_b"><div class="badge">3</div> Feature creation</div>
# MAGIC <div class="badge_b"><div class="badge">4</div> Build & train model</div>
# MAGIC <div class="badge_b"><div class="badge">5</div> Deploy Model (Batch or serverless real-time)</div>
# MAGIC <div class="badge_b"><div class="badge">6</div> Monitoring</div>
# MAGIC </div>
# MAGIC
# MAGIC **Marc needs A Lakehouse**. Let's see how we can deploy a real-time Fraud Detection model in production within the Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Fraud Detection - Single click deployment with Databricks Genie Code
# MAGIC
# MAGIC Let's see how we can now leverage the Banking data to build a model rating our Fraud risk on each transaction.
# MAGIC
# MAGIC Our first step as Data Scientist is to analyze and build the features we'll use to train our model.
# MAGIC
# MAGIC The transaction table enriched with customer data has been saved within our Lakeflow Pipelines. All we have to do is read this information, analyze it and let Genie Code generate our model.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-ds.png" width="1000px">
# MAGIC
# MAGIC *Note: Make sure you switched to the "Machine Learning" persona on the top left menu.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.1-AutoML-FSI-fraud&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

# COMMAND ----------

# MAGIC %pip install databricks-sdk mlflow==3.14.0 databricks-feature-engineering scikit-learn optuna
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration and analysis
# MAGIC
# MAGIC Let's review our dataset and start analyzing the data we have to detect fraud

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1- How much fraud are we talking about?
# MAGIC Based on the existing rules, while 3% of the transactions are fraudulent, it takes into account of the 9% of the total amount.   

# COMMAND ----------

# DBTITLE 1,Leverage built-in visualizations to explore your dataset
# MAGIC %sql
# MAGIC select 
# MAGIC   is_fraud,
# MAGIC   count(1) as `Transactions`, 
# MAGIC   sum(amount) as `Total Amount` 
# MAGIC from gold_transactions
# MAGIC group by is_fraud
# MAGIC
# MAGIC --Visualization Pie chart: Keys: is_fraud, Values: [Transactions, Total Amount]

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, financial fraud is by nature very imbalanced between fraudulent and normal transactions

# COMMAND ----------

# MAGIC %md ### 2- What type of transactions are associated with fraud?
# MAGIC Reviewing the rules-based model, it appears that most fraudulent transactions are in the category of `Transfer` and `Cash_Out`.

# COMMAND ----------

# DBTITLE 1,Run analysis using your usual python plot libraries
df = spark.sql('select type, is_fraud, count(1) as count from gold_transactions group by type, is_fraud').toPandas()

fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'}, {'type':'domain'}]])
fig.add_trace(go.Pie(labels=df[df['is_fraud']]['type'], values=df[df['is_fraud']]['count'], title="Fraud Transactions", hole=0.6), 1, 1)
fig.add_trace(go.Pie(labels=df[~df['is_fraud']]['type'], values=df[~df['is_fraud']]['count'], title="Normal Transactions", hole=0.6) ,1, 2)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Saving our dataset to Feature Store (Optional)
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Under the hood, feature stores are backed by a Delta Lake table.
# MAGIC
# MAGIC This will allow discoverability and reusability of our feature across our organization, increasing team efficiency.
# MAGIC
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent on which set of features. It also simplifies real-time serving.
# MAGIC
# MAGIC Make sure you're using the "Machine Learning" menu to have access to your feature store using the UI.

# COMMAND ----------

# DBTITLE 1,Create the final features using pandas API
# Convert to koalas
dataset = spark.table('gold_transactions').dropDuplicates(['id']).pandas_api()
# Drop columns we don't want to use in our model
# Typical DS project would include more transformations / cleanup here
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'customer_id'])

# Drop missing values
dataset.dropna()
dataset.describe()

# COMMAND ----------

# DBTITLE 1,Save them to our feature store
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

try:
  #drop table if exists
  fe.drop_table(name=f'{catalog}.{db}.transactions_features')
except:
  pass

fe.create_table(
  name=f'{catalog}.{db}.transactions_features',
  primary_keys='id',
  df=dataset.to_spark(),
  description='These features are derived from the gold_transactions table in the lakehouse.  We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the transaction is a fraud or not.  No aggregations were performed.')

features = fe.read_table(name=f'{catalog}.{db}.transactions_features')
display(features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Fraud detection model creation with Databricks Genie Code
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
# MAGIC ### Using Databricks Genie Code with our Transaction dataset
# MAGIC
# MAGIC All we have to do is describe our problem to Genie Code and point it at the feature table we just created (`transactions_features`), with `is_fraud` as our prediction target.
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

model_name = "dbdemos_fsi_fraud"
target_col = "is_fraud"
# Shared experiment path (must be a workspace /Shared path, not a local one, so it works in
# jobs running from the GitHub repo). set_experiment creates it if missing, or reuses it.
xp_path = "/Shared/dbdemos/experiments/lakehouse-fsi-fraud-detection"
# xp_path is a directory (experiments are created under it); set_experiment needs a
# leaf experiment path, so append a run name to avoid a DIRECTORY-vs-EXPERIMENT conflict.
mlflow.set_experiment(f"{xp_path}/genie_code_run")
DBDemos.set_experiment_permission(xp_path)

# drastically reduce the training size to speedup the demo
pdf = features.sample(0.02).toPandas().drop(columns=["id"], errors="ignore")
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
with mlflow.start_run(run_name="genie_code_fraud_detection") as genie_run:
    # Track dataset lineage: link the training data to this run for full governance.
    training_dataset = mlflow.data.from_pandas(pdf, name="transactions_features", targets=target_col)
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
# MAGIC Our Genie Code-generated model is ready. Let's register it in Unity Catalog and flag it as production-ready:

# COMMAND ----------

from mlflow import MlflowClient
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()
latest_model = mlflow.register_model(f'runs:/{genie_run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC Our model is now registered as production-ready. [Open the dbdemos_fsi_fraud model](#mlflow/models/dbdemos_fsi_fraud) to explore its artifacts and lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: deploying our model for Real Time fraud detection serving
# MAGIC
# MAGIC Now that our model has been created with Databricks Genie Code, we can start a Model Endpoint to serve low-latencies fraud detection.
# MAGIC
# MAGIC We'll be able to rate the Fraud likelihood in ms to reduce fraud in real-time.
# MAGIC
# MAGIC Open the [04.3-Model-serving-realtime-inference-fraud]($./04.3-Model-serving-realtime-inference-fraud) to deploy our model or review the code generated by Genie Code [04.2-automl-generated-notebook-fraud]($./04.2-automl-generated-notebook-fraud).
