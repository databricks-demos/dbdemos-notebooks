# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science with Databricks
# MAGIC
# MAGIC ## ML is key to disruption & risk reduction
# MAGIC
# MAGIC Being able to ingest and query our banking database is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC
# MAGIC Banking customers now expect real time personalization and protection. Modern data company achieve this with AI.
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
# MAGIC <div class="badge_b"><div class="badge">1</div> Build Data Pipeline supporting real time (with DLT)</div>
# MAGIC <div class="badge_b"><div class="badge">2</div> Data Exploration</div>
# MAGIC <div class="badge_b"><div class="badge">3</div> Feature creation</div>
# MAGIC <div class="badge_b"><div class="badge">4</div> Build & train model</div>
# MAGIC <div class="badge_b"><div class="badge">5</div> Deploy Model (Batch or serverless realtime)</div>
# MAGIC <div class="badge_b"><div class="badge">6</div> Monitoring</div>
# MAGIC </div>
# MAGIC
# MAGIC **Marc needs A Lakehouse**. Let's see how we can deploy a real-time Fraud Detection model in production within the Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Fraud Detection - Single click deployment with AutoML
# MAGIC
# MAGIC Let's see how we can now leverage the Banking data to build a model rating our Fraud risk on each transaction.
# MAGIC
# MAGIC Our first step as Data Scientist is to analyze and build the features we'll use to train our model.
# MAGIC
# MAGIC The transaction table enriched with customer data has been saved within our Delta Live Table pipeline. All we have to do is read this information, analyze it and start an Auto-ML run.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-ds.png" width="1000px">
# MAGIC
# MAGIC *Note: Make sure you switched to the "Machine Learning" persona on the top left menu.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.1-AutoML-FSI-fraud&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.36.0 mlflow==2.19.0 databricks-feature-store==0.17.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration and analysis
# MAGIC
# MAGIC Let's review our dataset and start analyze the data we have to detect fraud

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
# MAGIC As expected, financial fraud is by nature very imbalanced between fraudulant and normal transactions

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
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Under the hood, features store are backed by a Delta Lake table.
# MAGIC
# MAGIC This will allow discoverability and reusability of our feature accross our organization, increasing team efficiency.
# MAGIC
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent of which set of features. It also simplify realtime serving.
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
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table(f'{catalog}.{db}.transactions_features')
except:
  pass

fs.create_table(
  name=f'{catalog}.{db}.transactions_features',
  primary_keys='id',
  schema=dataset.spark.schema(),
  description='These features are derived from the gold_transactions table in the lakehouse.  We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the transaction is a fraud or not.  No aggregations were performed.')

fs.write_table(df=dataset.to_spark(), name=f'{catalog}.{db}.transactions_features', mode='overwrite')
features = fs.read_table(f'{catalog}.{db}.transactions_features')
display(features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Fraud detection model creation using MLFlow and Databricks Auto-ML
# MAGIC
# MAGIC MLflow is an open source project allowing model tracking, packaging and deployment. Everytime your datascientist team work on a model, Databricks will track all the parameter and data used and will save it. This ensure ML traceability and reproductibility, making it easy to know which model was build using which parameters/data.
# MAGIC
# MAGIC ### AutoML: A glass-box solution that empowers data teams without taking away control
# MAGIC
# MAGIC While Databricks simplify model deployment and governance (MLOps) with MLFlow, bootstraping new ML projects can still be long and inefficient. 
# MAGIC
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC
# MAGIC ### Using Databricks Auto ML with our Transaction dataset
# MAGIC
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`gold_transactions`)
# MAGIC
# MAGIC Our prediction target is the `is_fraud` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

# DBTITLE 1,Starting AutoML run using Databricks API
xp_path = "/Shared/dbdemos/experiments/lakehouse-fsi-fraud-detection"
xp_name = f"automl_fraud_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
try:
    from databricks import automl
    automl_run = automl.classify(
        experiment_name = xp_name,
        experiment_dir = xp_path,
        dataset = features.sample(0.02), #drastically reduce the training size to speedup the demo
        target_col = "is_fraud",
        timeout_minutes = 20
    )
    #Make sure all users can access dbdemos shared experiment
    DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")
except Exception as e:
    if "cannot import name 'automl'" in str(e):
        # Note: cannot import name 'automl' from 'databricks' likely means you're using serverless. Dbdemos doesn't support autoML serverless API - this will be improved soon.
        # Adding a temporary workaround to make sure it works well for now - ignore this for classic run
        DBDemos.create_mockup_automl_run(f"{xp_path}/{xp_name}", features.sample(0.02).toPandas())
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC AutoML saved our best model in the MLFlow registry. [Open the dbdemos_fsi fraud model](#mlflow/models/dbdemos_fsi_fraud) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.
# MAGIC
# MAGIC If we're ready, we can move this model into Production stage in a click, or using the API.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next: deploying our model for Real Time fraud detection serving 
# MAGIC
# MAGIC Now that our model has been created with Databricks AutoML, we can start a Model Endpoint to serve low-latencies fraud detection.
# MAGIC
# MAGIC We'll be able to rate the Fraud likelihood in ms to reduce fraud in real-time.
# MAGIC
# MAGIC Open the [04.3-Model-serving-realtime-inference-fraud]($./04.3-Model-serving-realtime-inference-fraud) to deploy our model or review the notebook generated by AutoML [04.2-automl-generated-notebook-fraud]($./04.2-automl-generated-notebook-fraud).
