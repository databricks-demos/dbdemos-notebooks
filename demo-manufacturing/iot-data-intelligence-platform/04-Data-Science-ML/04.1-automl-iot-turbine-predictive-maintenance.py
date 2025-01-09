# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Science with Databricks
# MAGIC
# MAGIC ## ML is key to wind turbine farm optimization
# MAGIC
# MAGIC The current market makes energy even more strategic than before. Being able to ingest and analyze our Wind turbine state is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC
# MAGIC We need to go further to optimize our energy production, reduce maintenance cost and reduce downtime. Modern data company achieve this with AI.
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
# MAGIC <div style="font-family: 'DM Sans'; display: flex; align-items: flex-start;">
# MAGIC   <!-- Left Section -->
# MAGIC   <div style="width: 50%; color: #1b3139; padding-right: 20px;">
# MAGIC     <div style="color: #ff5f46; font-size:80px;">90%</div>
# MAGIC     <div style="font-size:30px; margin-top: -20px; line-height: 30px;">
# MAGIC       Enterprise applications will be AI-augmented by 2025 —IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px;">$10T+</div>
# MAGIC     <div style="font-size:30px; margin-top: -20px; line-height: 30px;">
# MAGIC        Projected business value creation by AI in 2030 —PWC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Right Section -->
# MAGIC   <div class="right_box", style="width: 50%; color: red; font-size: 30px; line-height: 1.5; padding-left: 20px;">
# MAGIC     But—huge challenges getting ML to work at scale!<br/><br/>
# MAGIC     In fact, most ML projects still fail before getting to production
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ## Machine learning is data + transforms.
# MAGIC
# MAGIC ML is hard because delivering value to business lines isn't only about building a Model. <br>
# MAGIC The ML lifecycle is made of data pipelines: Data-preprocessing, feature engineering, training, inference, monitoring and retraining...<br>
# MAGIC Stepping back, all pipelines are data + code.
# MAGIC
# MAGIC
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/team_flow_marc.png" />
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/marc.png" style="float: left;" width="80px"> 
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
# MAGIC **Marc needs a Data Intelligence Platform**. Let's see how we can deploy a Predictive Maintenance model in production with Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Predictive maintenance - Single click deployment with AutoML
# MAGIC
# MAGIC Let's see how we can now leverage the sensor data to build a model predictive maintenance model.
# MAGIC
# MAGIC Our first step as Data Scientist is to analyze and build the features we'll use to train our model.
# MAGIC
# MAGIC The sensor table enriched with turbine data has been saved within our Delta Live Table pipeline. All we have to do is read this information, analyze it and start an Auto-ML run.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-ds-flow.png" width="1000px">
# MAGIC
# MAGIC *Note: Make sure you switched to the "Machine Learning" persona on the top left menu.*
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.1-automl-iot-turbine-predictive-maintenance&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sdk==0.29.0 databricks-feature-engineering==0.6.0 mlflow==2.17.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration and analysis
# MAGIC
# MAGIC Let's review our dataset and start analyze the data we have to predict our churn

# COMMAND ----------

# DBTITLE 1,Quick data exploration leveraging pandas on spark (Koalas): sensor from 2 wind turbines
def plot(sensor_report):
  turbine_id = spark.table('turbine_training_dataset').where(f"abnormal_sensor = '{sensor_report}' ").limit(1).collect()[0]['turbine_id']
  #Let's explore a bit our datasets with pandas on spark.
  df = spark.table('sensor_bronze').where(f"turbine_id == '{turbine_id}' ").orderBy('timestamp').pandas_api()
  df.plot(x="timestamp", y=["sensor_B"], kind="line", title=f'Sensor report: {sensor_report}').show()
plot('ok')
plot('sensor_B')

# COMMAND ----------

# MAGIC %md As we can see in these graph, we can clearly see some anomaly on the readings we get from sensor F. Let's continue our exploration and use the std we computed in our main feature table

# COMMAND ----------

# Read our churn_features table
turbine_dataset = spark.table('turbine_training_dataset').withColumn('damaged', col('abnormal_sensor') != 'ok')
display(turbine_dataset)

# COMMAND ----------

# DBTITLE 1,Damaged sensors clearly have a different distribution
import seaborn as sns
g = sns.PairGrid(turbine_dataset.sample(0.01).toPandas()[['std_sensor_A', 'std_sensor_E', 'damaged','avg_energy']], diag_sharey=False, hue="damaged")
g.map_lower(sns.kdeplot).map_diag(sns.kdeplot, lw=3).map_upper(sns.regplot).add_legend()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Further data analysis and preparation using pandas API
# MAGIC
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use `pandas on spark` to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC
# MAGIC Typicaly Data Science project would involve more advanced preparation and likely require extra data prep step, including more complex feature preparation. We'll keep it simple for this demo.
# MAGIC
# MAGIC *Note: Starting from `spark 3.2`, koalas is builtin and we can get an Pandas Dataframe using `pandas_api()`.*

# COMMAND ----------

# DBTITLE 1,Custom pandas transformation / code on top of your entire dataset (koalas)
 # Convert to pandas (koalas)
dataset = turbine_dataset.pandas_api()
# Drop columns we don't want to use in our model
dataset = dataset[['turbine_id', 'hourly_timestamp', 'std_sensor_A', 'std_sensor_B','std_sensor_C', 'std_sensor_D','std_sensor_E', 'std_sensor_F', 'abnormal_sensor']]
# Drop missing values
dataset = dataset.dropna()   
dataset.describe()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Write to Feature Store (Optional)
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

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
try:
  #drop table if exists
  spark.sql('drop table if exists turbine_hourly_features')
  fe.drop_table(name=f'{catalog}.{db}.turbine_hourly_features')
except:
  pass
#Note: You might need to delete the FS table using the UI
churn_feature_table = fe.create_table(
  name=f'{catalog}.{db}.turbine_hourly_features',
  primary_keys=['turbine_id','hourly_timestamp'],
  schema=dataset.spark.schema(),
  description='These features are derived from the turbine_training_dataset table in the data intelligence platform.  We made some basic transformations and removed NA value.'
)

fe.write_table(df=dataset.drop_duplicates(subset=['turbine_id', 'hourly_timestamp']).to_spark(), name=f'{catalog}.{db}.turbine_hourly_features')
features = fe.read_table(name=f'{catalog}.{db}.turbine_hourly_features')
display(features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Predictive maintenance model creation using MLFlow and Databricks Auto-ML
# MAGIC
# MAGIC MLflow is an open source project allowing model tracking, packaging and deployment. Everytime your datascientist team work on a model, Databricks will track all the parameter and data used and will save it. This ensure ML traceability and reproductibility, making it easy to know which model was build using which parameters/data.
# MAGIC
# MAGIC ### A glass-box solution that empowers data teams without taking away control
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
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`turbine_hourly_features`)
# MAGIC
# MAGIC Our prediction target is the `abnormal_sensor` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

# DBTITLE 1,We have already started a run for you, you can explore it here:
from databricks import automl
xp_path = "/Shared/dbdemos/experiments/lakehouse-iot-platform"
xp_name = f"automl_iot_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"

training_dataset = fe.read_table(name=f'{catalog}.{db}.turbine_hourly_features').drop('turbine_id', 'hourly_timestamp').sample(0.1) #Reduce the dataset size to speedup the demo
automl_run = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = training_dataset,
    target_col = "abnormal_sensor",
    timeout_minutes = 10
)
#Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC AutoML saved our best model in the MLFlow registry. Open the experiment from the AutoML run to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.
# MAGIC
# MAGIC If we're ready, we can move this model into Production stage in a click, or using the API. Let' register the model to Unity Catalog and move it to production:

# COMMAND ----------

from mlflow import MlflowClient

# retrieve best model trial run
trial_id = automl_run.best_trial.mlflow_run_id
model_uri = "runs:/{}/model".format(automl_run.best_trial.mlflow_run_id)
#Use Databricks Unity Catalog to save our model
latest_model = mlflow.register_model(model_uri, f"{catalog}.{db}.{model_name}")
# Flag it as Production ready using UC Aliases
MlflowClient().set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### The model generated by AutoML is ready to be used in our DLT pipeline to detect Wind Turbine requiring potential maintenance.
# MAGIC
# MAGIC Our Data Engineer can now easily retrive the model `dbdemos_turbine_maintenance` from our Auto ML run and detect anomalies within our Delta Live Table Pipeline.<br>
# MAGIC Re-open the DLT pipeline to see how this is done.
# MAGIC
# MAGIC #### Adjust spare stock based on predictive maintenance result
# MAGIC
# MAGIC These predictions can be re-used in our dashboard to not only measure equipment failure probability, but take action to schedule maintenance and ajust spare part stock accordingly. 
# MAGIC
# MAGIC The pipeline created with the Data Intelligence Platform will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $Million / month!
# MAGIC
# MAGIC <img width="800px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
# MAGIC
# MAGIC <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Open the Predictive Maintenance DBSQL dashboard</a> | [Go back to the introduction]($../00-IOT-wind-turbine-introduction-DI-platform)
# MAGIC
# MAGIC #### More advanced model deployment (batch or serverless realtime)
# MAGIC
# MAGIC We can also use the model `dbdemos_turbine_maintenance` and run our predict in a standalone batch or realtime inferences! 
# MAGIC
# MAGIC Next step:  [Explore the generated Auto-ML notebook]($./04.2-automl-generated-notebook-iot-turbine) and [Run inferences in production]($./04.3-running-inference-iot-turbine)
