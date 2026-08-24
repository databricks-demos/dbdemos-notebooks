# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science with Databricks
# MAGIC
# MAGIC ## ML is key to disruption & personalization
# MAGIC
# MAGIC Being able to ingest and query our C360 database is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC
# MAGIC Customers now expect real time personalization and new forms of communication. Modern data companies achieve this with AI.
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:600px;height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
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
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/marc.png" style="float: left;" width="80px"> 
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
# MAGIC **Marc needs A Databricks Platform**. Let's see how we can deploy a Churn model in production within the Lakehouse
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.1-automl-churn-prediction&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Churn Prediction - Single click deployment with Genie Code
# MAGIC
# MAGIC Let's see how we can now leverage the C360 data to build a model predicting and explaining customer Churn.
# MAGIC
# MAGIC Our first step as Data Scientist is to analyze and build the features we'll use to train our model.
# MAGIC
# MAGIC The users table enriched with churn data has been saved within our Lakeflow pipeline. All we have to do is read this information, analyze it and let Genie Code generate our model.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-churn-ds-flow.png" width="1000px">
# MAGIC
# MAGIC *Note: Make sure you switched to the "Machine Learning" persona on the top left menu.*

# COMMAND ----------

# MAGIC %uv pip install databricks-sdk mlflow==3.14.0 databricks-feature-engineering==0.16.0 scikit-learn==1.9.0 optuna==4.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration and analysis
# MAGIC
# MAGIC Let's review our dataset and start analyzing the data we have to predict our churn

# COMMAND ----------

# DBTITLE 1,Read our churn gold table
# Read our churn_features table
churn_dataset = spark.table("churn_features")
display(churn_dataset)

# COMMAND ----------

# DBTITLE 1,Data Exploration and analysis
import seaborn as sns
g = sns.PairGrid(churn_dataset.sample(0.01).toPandas()[['age_group','total_amount','order_count']], diag_sharey=False)
g.map_lower(sns.kdeplot)
g.map_diag(sns.kdeplot, lw=3)
g.map_upper(sns.regplot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Further data analysis and preparation using pandas API
# MAGIC
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use `pandas on spark` to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC
# MAGIC Typically Data Science projects would involve more advanced preparation and likely require extra data prep steps, including more complex feature preparation. We'll keep it simple for this demo.
# MAGIC
# MAGIC *Note: Starting from `spark 3.2`, koalas is builtin and we can get an Pandas Dataframe using `pandas_api()`.*

# COMMAND ----------

# DBTITLE 1,Custom pandas transformation / code on top of your entire dataset
# Convert to koalas
dataset = churn_dataset.pandas_api()
dataset.describe()  
# Drop columns we don't want to use in our model
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'last_event'])
# Drop missing values
dataset = dataset.dropna()   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Write to Feature Store (Optional)
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Under the hood, features store are backed by a Delta Lake table.
# MAGIC
# MAGIC This will allow discoverability and reusability of our feature across our organization, increasing team efficiency.
# MAGIC
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent on which set of features. It also simplifies real-time serving.
# MAGIC
# MAGIC Make sure you're using the "Machine Learning" menu to have access to your feature store using the UI.

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

try:
  #drop table if exists
  fe.drop_table(name=f'{catalog}.{db}.churn_user_features')
except:
  pass
#Note: You might need to delete the FS table using the UI
churn_feature_table = fe.create_table(
  name=f'{catalog}.{db}.churn_user_features',
  primary_keys='user_id',
  df=dataset.to_spark(),
  description='These features are derived from the churn_bronze_customers table in the lakehouse.  We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not.  No aggregations were performed.'
)

features = fe.read_table(name=f'{catalog}.{db}.churn_user_features')
display(features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Churn model creation with Databricks Genie Code
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
# MAGIC ### Using Databricks Genie Code with our Churn dataset
# MAGIC
# MAGIC All we have to do is describe our problem to Genie Code and point it at the feature table we just created (`churn_user_features`), with `churn` as our prediction target.
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

model_name = "dbdemos_customer_churn"
target_col = "churn"
# Shared experiment path (must be a workspace /Shared path, not a local one, so it works in
# jobs running from the GitHub repo). set_experiment creates it if missing, or reuses it.
xp_path = "/Shared/dbdemos/experiments/lakehouse-retail-c360"
# xp_path is a directory (experiments are created under it); set_experiment needs a
# leaf experiment path, so append a run name to avoid a DIRECTORY-vs-EXPERIMENT conflict.
mlflow.set_experiment(f"{xp_path}/genie_code_run")
DBDemos.set_experiment_permission(xp_path)

pdf = fe.read_table(name=f'{catalog}.{db}.churn_user_features').toPandas()
# user_id is the feature-store primary key, not a feature.
X = pdf.drop(columns=[target_col, "user_id"], errors="ignore")
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
with mlflow.start_run(run_name="genie_code_churn") as genie_run:
    # Track dataset lineage: link the training data to this run for full governance.
    training_dataset = mlflow.data.from_pandas(pdf, name="churn_user_features", targets=target_col)
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

# DBTITLE 1,Register the Genie Code model as production
from mlflow import MlflowClient
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()
latest_model = mlflow.register_model(f'runs:/{genie_run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC Genie Code saved our best model in the MLflow registry and we promoted it to `@prod` in Unity Catalog. Open the experiment to explore its artifacts, parameters and dataset lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The model generated by Genie Code is ready to be used in our Lakeflow pipeline to detect customers about to churn.
# MAGIC
# MAGIC Our Data Engineer can now easily retrieve the model `dbdemos_customer_churn` generated by Genie Code and predict churn within our Lakeflow pipeline.<br>
# MAGIC Re-open the Lakeflow pipeline to see how this is done.
# MAGIC
# MAGIC #### Track churn impact over the next month and campaign impact
# MAGIC
# MAGIC This churn prediction can be re-used in our dashboard to analyse future churn, take action and measure churn reduction. 
# MAGIC
# MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to set up this pipeline end-to-end and we have potential gain of $129,914 / month!
# MAGIC
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
# MAGIC
# MAGIC <a href='/sql/dashboards/f25702b4-56d8-40a2-a69d-d2f0531a996f'>Open the Churn prediction DBSQL dashboard</a> | [Go back to the introduction]($../00-churn-introduction-lakehouse)
# MAGIC
# MAGIC #### More advanced model deployment (batch or serverless real-time)
# MAGIC
# MAGIC We can also use the model `dbdemos_custom_churn` and run our predict in a standalone batch or real-time inferences! 
# MAGIC
# MAGIC Next step:  [Explore the code generated by Genie Code]($./04.2-automl-generated-notebook) and [Run inferences in production]($./04.3-running-inference)
