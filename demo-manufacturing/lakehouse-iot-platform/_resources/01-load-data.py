# Databricks notebook source
# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker databricks-sdk==0.39.0 mlflow==2.19.0 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

data_exists = False
try:
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/historical_turbine_status")
  dbutils.fs.ls(folder+"/parts")
  dbutils.fs.ls(folder+"/turbine")
  dbutils.fs.ls(folder+"/incoming_data")
  data_exists = True
  print("data already exists")
except Exception as e:
  print(f"folder doesn't exists, generating the data...")


def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

data_downloaded = True
if not data_exists:
    try:
        DBDemos.download_file_from_git(folder+'/historical_turbine_status', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/historical_turbine_status")
        DBDemos.download_file_from_git(folder+'/parts', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/parts")
        DBDemos.download_file_from_git(folder+'/turbine', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/turbine")
        DBDemos.download_file_from_git(folder+'/incoming_data', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/incoming_data")
        spark.sql("CREATE TABLE IF NOT EXISTS turbine_power_prediction ( hour INT, min FLOAT, max FLOAT, prediction FLOAT);")
        spark.sql("insert into turbine_power_prediction values (0, 377, 397, 391), (1, 393, 423, 412), (2, 399, 455, 426), (3, 391, 445, 404), (4, 345, 394, 365), (5, 235, 340, 276), (6, 144, 275, 195), (7, 93, 175, 133), (8, 45, 105, 76), (9, 55, 125, 95), (10, 35, 99, 77), (11, 14, 79, 44)")
    except Exception as e: 
        data_downloaded = False
        print(f"Error trying to download the file from the repo: {str(e)}. Will generate the data instead...")    
        raise e

# COMMAND ----------

#As we need a model in the DLT pipeline and the model depends of the DLT pipeline too, let's build an empty one.
#This wouldn't make sense in a real-world system where we'd have 2 jobs / pipeline (1 for ingestion, and 1 to build the model / run inferences)
import random
import mlflow
from  mlflow.models.signature import ModelSignature

# define a custom model randomly flagging 10% of sensor for the demo init (it'll be replace with proper model on the training part.)
class MaintenanceEmptyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        import random
        sensors = ['sensor_F', 'sensor_D', 'sensor_B']  # List of sensors
        return model_input['avg_energy'].apply(lambda x: 'ok' if random.random() < 0.9 else random.choice(sensors))
 
#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')
model_name = "dbdemos_turbine_maintenance"
#Only register empty model if model doesn't exist yet
client = mlflow.tracking.MlflowClient()
try:
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Model doesn't exist - saving an empty one")
        # setup the experiment folder
        DBDemos.init_experiment_for_batch("lakehouse-iot-platform", "predictive_maintenance_mock")
        # save the model
        churn_model = MaintenanceEmptyModel()
        import pandas as pd

        signature = ModelSignature.from_dict({'inputs': '[{"name": "hourly_timestamp", "type": "datetime"}, {"name": "avg_energy", "type": "double"}, {"name": "std_sensor_A", "type": "double"}, {"name": "std_sensor_B", "type": "double"}, {"name": "std_sensor_C", "type": "double"}, {"name": "std_sensor_D", "type": "double"}, {"name": "std_sensor_E", "type": "double"}, {"name": "std_sensor_F", "type": "double"}, {"name": "percentiles_sensor_A", "type": "string"}, {"name": "percentiles_sensor_B", "type": "string"}, {"name": "percentiles_sensor_C", "type": "string"}, {"name": "percentiles_sensor_D", "type": "string"}, {"name": "percentiles_sensor_E", "type": "string"}, {"name": "percentiles_sensor_F", "type": "string"}, {"name": "location", "type": "string"}, {"name": "model", "type": "string"}, {"name": "state", "type": "string"}]',
'outputs': '[{"type": "tensor", "tensor-spec": {"dtype": "object", "shape": [-1]}}]'})
        with mlflow.start_run() as run:
            model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=churn_model, signature=signature, pip_requirements=['scikit-learn==1.3.0', 'mlflow=='+mlflow.__version__])

        #Register & move the model in production
        model_registered = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)
    else:
        print(f"ERROR: couldn't access model for unknown reason - DLT pipeline will likely fail as model isn't available: {e}")

# COMMAND ----------

if data_downloaded:
    dbutils.notebook.exit(f"Data Downloaded to {folder}")

# COMMAND ----------

from mandrova.data_generator import SensorDataGenerator as sdg
import numpy as np
import pandas as pd
import random
import time
import uuid
import pyspark.sql.functions as F


def generate_sensor_data(turbine_id, sensor_conf, faulty = False, sample_size = 1000, display_graph = True, noise = 2, delta = -3):
  dg = sdg()
  rd = random.Random()
  rd.seed(turbine_id)
  dg.seed(turbine_id)
  sigma = sensor_conf['sigma']
  #Faulty, change the sigma with random value
  if faulty:
    sigma *= rd.randint(8,20)/10
    
  dg.generation_input.add_option(sensor_names="normal", distribution="normal", mu=0, sigma = sigma)
  dg.generation_input.add_option(sensor_names="sin", eq=f"2*exp(sin(t))+{delta}", initial={"t":0}, step={"t":sensor_conf['sin_step']})
  dg.generate(sample_size)
  sensor_name = "sensor_"+ sensor_conf['name']
  dg.sum(sensors=["normal", "sin"], save_to=sensor_name)
  max_value = dg.data[sensor_name].max()
  min_value = dg.data[sensor_name].min()
  if faulty:
    n_outliers = int(sample_size*0.15)
    outliers = np.random.uniform(-max_value*rd.randint(2,3), max_value*rd.randint(2,3), n_outliers)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
    dg.inject(value=outliers, sensor=sensor_name, index=indicies)

  n_outliers = int(sample_size*0.01)
  outliers = np.random.uniform(min_value*noise, max_value*noise, n_outliers)
  indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
  dg.inject(value=outliers, sensor=sensor_name, index=indicies)
  
  if display_graph:
    dg.plot_data(sensors=[sensor_name])
  return dg.data[sensor_name]

# COMMAND ----------

sensors = [{"name": "A", "sin_step": 0, "sigma": 1},
           {"name": "B", "sin_step": 0, "sigma": 2},
           {"name": "C", "sin_step": 0, "sigma": 3},
           {"name": "D", "sin_step": 0.1, "sigma": 1.5},
           {"name": "E", "sin_step": 0.01, "sigma": 2},
           {"name": "F", "sin_step": 0.2, "sigma": 1}]
current_time = int(time.time()) - 3600*30

#Sec between 2 metrics
frequency_sec = 10
#X points per turbine (1 point per frequency_sec second)
sample_size = 2125
turbine_count = 512
dfs = []

# COMMAND ----------

def generate_turbine_data(turbine):
  turbine = int(turbine)
  rd = random.Random()
  rd.seed(turbine)
  damaged = turbine > turbine_count*0.6
  if turbine % 10 == 0:
    print(f"generating turbine {turbine} - damage: {damaged}")
  df = pd.DataFrame()
  damaged_sensors = []
  rd.shuffle(sensors)
  for s in sensors:
    #30% change to have 1 sensor being damaged
    #Only 1 sensor can send damaged metrics at a time to simplify the model. A C and E won't be damaged for simplification
    if damaged and len(damaged_sensors) == 0 and s['name'] not in ["A", "C", "E"]:
      damaged_sensor = rd.randint(1,10) > 5
    else:
      damaged_sensor = False
    if damaged_sensor:
      damaged_sensors.append('sensor_'+s['name'])
    plot = turbine == 0
    df['sensor_'+s['name']] = generate_sensor_data(turbine, s, damaged_sensor, sample_size, plot)

  dg = sdg()
  #Damaged turbine will produce less
  factor = 50 if damaged else 30
  energy = dg.generation_input.add_option(sensor_names="energy", eq="x", initial={"x":0}, step={"x":np.absolute(np.random.randn(sample_size).cumsum()/factor)})
  dg.generate(sample_size, seed=rd.uniform(0,10000))
  #Add some null values in some timeseries to get expectation metrics
  if damaged and rd.randint(0,9) >7:
    n_nulls = int(sample_size*0.005)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_nulls))
    dg.inject(value=None, sensor="energy", index=indicies)

  if plot:
    dg.plot_data()
  df['energy'] = dg.data['energy']

  df.insert(0, 'timestamp', range(current_time, current_time + len(df)*frequency_sec, frequency_sec))
  df['turbine_id'] = str(uuid.UUID(int=rd.getrandbits(128)))
  #df['damaged'] = damaged
  df['abnormal_sensor'] = "ok" if len(damaged_sensors) == 0 else damaged_sensors[0]
  return df

from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf, col  

df_schema=spark.createDataFrame(generate_turbine_data(0)) 

def generate_turbine(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
  for pdf in iterator:
    for i, row in pdf.iterrows():
      yield generate_turbine_data(row["id"])

spark_df = spark.range(0, turbine_count).repartition(int(turbine_count/10)).mapInPandas(generate_turbine, schema=df_schema.schema)

# COMMAND ----------

root_folder = folder
folder_sensor = root_folder+'/incoming_data'
spark_df.drop('damaged').drop('abnormal_sensor').orderBy('timestamp').repartition(100).write.mode('overwrite').format('parquet').save(folder_sensor)

#Cleanup meta file to only keep names
def cleanup(folder):
  for f in dbutils.fs.ls(folder):
    if not f.name.startswith('part-00'):
      if not f.path.startswith('dbfs:/Volumes'):
        raise Exception(f"unexpected path, {f} throwing exception for safety")
      dbutils.fs.rm(f.path)
      
cleanup(folder_sensor)

# COMMAND ----------

from faker import Faker
from pyspark.sql.types import ArrayType, FloatType, StringType
import pyspark.sql.functions as F

Faker.seed(0)
faker = Faker()
fake_latlng = F.udf(lambda: list(faker.local_latlng(country_code = 'US')), ArrayType(StringType()))

# COMMAND ----------

rd = random.Random()
rd.seed(0)
folder = root_folder+'/turbine'
(spark_df.select('turbine_id').drop_duplicates()
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.col('fake_lat_long').getItem(0))
   .withColumn('long', F.col('fake_lat_long').getItem(1))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long')
 .orderBy(F.rand()).repartition(1).write.mode('overwrite').format('json').save(folder))

#Add some turbine with wrong data for expectations
fake_null_uuid = F.udf(lambda: None if rd.randint(0,9) > 2 else str(uuid.uuid4()))
df_error = (spark_df.select('turbine_id').limit(30)
   .withColumn('turbine_id', fake_null_uuid())
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.lit("ERROR"))
   .withColumn('long', F.lit("ERROR"))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long').repartition(1).write.mode('append').format('json').save(folder))
cleanup(folder)

folder_status = root_folder+'/historical_turbine_status'
(spark_df.select('turbine_id', 'abnormal_sensor').drop_duplicates()
         .withColumn('start_time', (F.lit(current_time-1000)-F.rand()*2000).cast('int'))
         .withColumn('end_time', (F.lit(current_time+3600*24*30)+F.rand()*4000).cast('int'))
         .repartition(1).write.mode('overwrite').format('json').save(folder_status))
cleanup(folder_status)

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/5/52/EERE_illust_large_turbine.gif">

# COMMAND ----------

#see https://blog.enerpac.com/wind-turbine-maintenance-components-strategies-and-tools/
#Get the list of states where our wind turbine are
states = spark.read.json(folder).select('state').distinct().collect()
states = [s['state'] for s in states]
#For each state, we'll generate supply chain parts
part_categories = [{'name': 'blade'}, {'name': 'Yaw drive'}, {'name': 'Brake'}, {'name': 'anemometer'}, {'name': 'controller card #1'}, {'name': 'controller card #2'}, {'name': 'Yaw motor'}, {'name': 'hydraulics'}, {'name': 'electronic guidance system'}]
sensors = [c for c in spark.read.parquet(folder_sensor).columns if "sensor" in c]
parts = []
for p in part_categories:
  for _ in range (1, rd.randint(30, 100)):
    part = {}
    part['EAN'] = None if rd.randint(0,100) > 95 else faker.ean(length=8)
    part['type'] = p['name']
    part['width'] = rd.randint(100,2000)
    part['height'] = rd.randint(100,2000)
    part['weight'] = rd.randint(100,20000)
    part['stock_available'] = rd.randint(0, 5)
    part['stock_location'] =   random.sample(states, 1)[0]
    part['production_time'] = rd.randint(0, 5)
    part['approvisioning_estimated_days'] = rd.randint(30,360)
    part['sensors'] = random.sample(sensors, rd.randint(1,3))
    parts.append(part)
df = spark.createDataFrame(parts)
folder_parts = root_folder+'/parts'
df.repartition(3).write.mode('overwrite').format('json').save(folder_parts)
cleanup(folder_parts)
