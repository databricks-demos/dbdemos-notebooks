# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %uv pip install faker databricks-sdk mlflow==3.14.0 cloudpickle numpy pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

import sys
major, minor = sys.version_info[:2]
assert (major, minor) >= (3, 11), f"This demo expect python version 3.11, but found {major}.{minor}. \nUse DBR15.4 or above. \nIf you're on serverless compute, open the 'Environment' menu on the right of your notebook, set it to >=2 and apply."

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

data_exists = False
try:
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/orders")
  dbutils.fs.ls(folder+"/users")
  dbutils.fs.ls(folder+"/events")
  dbutils.fs.ls(folder+"/ml_features")
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

# MAGIC %md 
# MAGIC ## Let's download the data from dbdemos resource repo
# MAGIC If this fails, fallback on generating the data

# COMMAND ----------

data_downloaded = False
if not data_exists:
    try:
        DBDemos.download_file_from_git(folder+'/events', "databricks-demos", "dbdemos-dataset", "/retail/c360/events")
        DBDemos.download_file_from_git(folder+'/orders', "databricks-demos", "dbdemos-dataset", "/retail/c360/orders")
        DBDemos.download_file_from_git(folder+'/users', "databricks-demos", "dbdemos-dataset", "/retail/c360/users")
        DBDemos.download_file_from_git(folder+'/ml_features', "databricks-demos", "dbdemos-dataset", "/retail/c360/ml_features")
        data_downloaded = True
    except Exception as e: 
        print(f"Error trying to download the file from the repo: {str(e)}. Will generate the data instead...")    
else:
    data_downloaded = True

# COMMAND ----------

#As we need a model in the Lakeflow pipeline and the model depends of the Lakeflow pipeline too, let's build an empty one.
#This wouldn't make sense in a real-world system where we'd have 2 jobs / pipeline (1 for ingestion, and 1 to build the model / run inferences)
import random
import mlflow
from  mlflow.models.signature import ModelSignature
import cloudpickle
from unittest import mock
import numpy as np

# define a custom model
class ChurnEmptyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        import random
        return model_input['user_id'].apply(lambda x: random.randint(0, 1)).astype('int32')

#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')
model_name = "dbdemos_customer_churn"
#Only register empty model if model doesn't exist yet
client = mlflow.tracking.MlflowClient()
try:
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
except Exception as e:

    if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
        print("Model doesn't exist - saving an empty one")
        # setup the experiment folder
        DBDemos.init_experiment_for_batch("lakehouse-retail-c360", "customer_churn_mock")
        # save the model
        churn_model = ChurnEmptyModel()
        import pandas as pd
        signature = ModelSignature.from_dict({'inputs': '[{"name": "user_id", "type": "string"}, {"name": "age_group", "type": "long"}, {"name": "canal", "type": "string"}, {"name": "country", "type": "string"}, {"name": "gender", "type": "long"}, {"name": "order_count", "type": "long"}, {"name": "total_amount", "type": "long"}, {"name": "total_item", "type": "long"}, {"name": "last_transaction", "type": "datetime"}, {"name": "platform", "type": "string"}, {"name": "event_count", "type": "long"}, {"name": "session_count", "type": "long"}, {"name": "days_since_creation", "type": "long"}, {"name": "days_since_last_activity", "type": "long"}, {"name": "days_last_event", "type": "long"}]',
'outputs': '[{"type": "tensor", "tensor-spec": {"dtype": "int32", "shape": [-1]}}]'})
        #Temporary pin python to 3.11.10
        with mlflow.start_run(run_name="mockup_model") as run, mock.patch("mlflow.utils.environment.PYTHON_VERSION", DBDemos.get_python_version_mlflow()):
            model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=churn_model, signature=signature, pip_requirements=['mlflow=='+mlflow.__version__, 'pandas=='+pd.__version__, 'numpy=='+np.__version__, 'cloudpickle=='+cloudpickle.__version__])

        #Register & move the model in production
        model_registered = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)
    else:
        print(f"ERROR: couldn't access model for unknown reason - Lakeflow pipeline will likely fail as model isn't available: {e}")

# COMMAND ----------

if data_downloaded:
    dbutils.notebook.exit("data downloaded")

# COMMAND ----------

# DBTITLE 1,users data
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
fake = Faker()
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# Generate all fake data with Faker in pandas (driver-side), then create Spark
# DataFrames. Spark Python Faker-UDFs + non-deterministic columns (rand,
# monotonically_increasing_id) trip the serverless Spark Connect analyzer with a
# spurious MISSING_GROUP_BY on write; pandas + createDataFrame is deterministic
# and serverless-safe.
canal_vals = ["WEBAPP", "MOBILE", "PHONE", None]
canal_p = np.array([0.5, 0.1, 0.3, 0.01]); canal_p = canal_p / canal_p.sum()
countries = ['FR', 'USA', 'SPAIN']

def _fake_id(n):
  # ~2% None to mimic the original uuid generator
  return [str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None for _ in range(n)]

def _date_between(n, months):
  start = datetime.now() - timedelta(days=30*months)
  return [fake.date_between_dates(date_start=start, date_end=start + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S") for _ in range(n)]

def _date_this_month(n):
  return [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(n)]

def get_pdf(size, month):
  return pd.DataFrame({
    "id": _fake_id(size),
    "firstname": [fake.first_name() for _ in range(size)],
    "lastname": [fake.last_name() for _ in range(size)],
    "email": [fake.ascii_company_email() for _ in range(size)],
    "address": [fake.address().replace('\n', ' ') for _ in range(size)],
    "canal": np.random.choice(canal_vals, size, p=canal_p),
    "country": np.random.choice(countries, size),
    "creation_date": _date_between(size, month),
    "last_activity_date": _date_this_month(size),
    "gender": np.random.randint(0, 2, size),
    "age_group": np.random.randint(0, 11, size),
  })

# First cohort: oldest customers (creation_date 2012-2015)
pdf_first = get_pdf(133, 12*30)
pdf_first["creation_date"] = [fake.date_between_dates(date_start=datetime(2012,1,1), date_end=datetime(2015,12,31)).strftime("%m-%d-%Y %H:%M:%S") for _ in range(len(pdf_first))]
pdfs = [pdf_first]
for i in range(1, 24):
  pdfs.append(get_pdf(2000+i*200, 24-i))
pdf_customers = pd.concat(pdfs, ignore_index=True)

df_customers = spark.createDataFrame(pdf_customers)

ids = pdf_customers["id"].tolist()


# COMMAND ----------

#Number of order per customer to generate a nicely distributed dataset
import numpy as np
np.random.seed(0)
mu, sigma = 3, 2 # mean and standard deviation
s = np.random.normal(mu, sigma, int(len(ids)))
s = [i if i > 0 else 0 for i in s]

#Most of our customers have ~3 orders
import matplotlib.pyplot as plt
count, bins, ignored = plt.hist(s, 30, density=False)
plt.show()
s = [int(i) for i in s]

order_user_ids = list()
action_user_ids = list()
for i, id in enumerate(ids):
  for j in range(1, s[i]):
    order_user_ids.append(id)
    #Let's make 5 more actions per order (5 click on the website to buy something)
    for j in range(1, 5):
      action_user_ids.append(id)

print(f"Generated {len(order_user_ids)} orders and  {len(action_user_ids)} actions for {len(ids)} users")

# COMMAND ----------

# DBTITLE 1,order data
# Build all fake columns in pandas (serverless-safe, avoids Spark Faker-UDFs).
n_orders = len(order_user_ids)
item_count = np.round(np.random.rand(n_orders)*2) + 1
pdf_orders = pd.DataFrame({
  "user_id": order_user_ids,
  "id": _fake_id(n_orders),
  "transaction_date": _date_this_month(n_orders),
  "item_count": item_count,
  "amount": item_count * (np.round(np.random.rand(n_orders)*30 + 10)),
})
orders = spark.createDataFrame(pdf_orders)
orders.repartition(10).write.format("json").mode("overwrite").save(folder+"/orders")
cleanup_folder(folder+"/orders")

# COMMAND ----------

# DBTITLE 1,website actions
#Website interaction
import re

platform_vals = ["ios", "android", "other", None]
platform_p = np.array([0.5, 0.1, 0.3, 0.01]); platform_p = platform_p / platform_p.sum()
action_vals = ["view", "log", "click", None]
action_p = np.array([0.5, 0.1, 0.3, 0.01]); action_p = action_p / action_p.sum()

n_actions = len(order_user_ids)
pdf_actions = pd.DataFrame({
  "user_id": order_user_ids,
  "event_id": _fake_id(n_actions),
  "platform": np.random.choice(platform_vals, n_actions, p=platform_p),
  "date": _date_this_month(n_actions),
  "action": np.random.choice(action_vals, n_actions, p=action_p),
  "session_id": _fake_id(n_actions),
  "url": [re.sub(r'https?:\/\/.*?\/', "https://databricks.com/", fake.uri()) for _ in range(n_actions)],
})
actions = spark.createDataFrame(pdf_actions).repartition(20)
actions.write.format("csv").option("header", True).mode("overwrite").save(folder+"/events")
cleanup_folder(folder+"/events")

# COMMAND ----------

# DBTITLE 1,Compute churn and save users
#Let's generate the Churn information. We'll fake it based on the existing data & let our ML model learn it
from pyspark.sql.functions import col
import pyspark.sql.functions as F

churn_proba_action = actions.groupBy('user_id').agg({'platform': 'first', '*': 'count'}).withColumnRenamed("count(1)", "action_count")
#Let's count how many order we have per customer.
churn_proba = orders.groupBy('user_id').agg({'item_count': 'sum', '*': 'count'})
churn_proba = churn_proba.join(churn_proba_action, ['user_id'])
churn_proba = churn_proba.join(df_customers, churn_proba.user_id == df_customers.id)

#Customer having > 5 orders are likely to churn
churn_proba = (churn_proba.withColumn("churn_proba", 5 +  F.when(((col("count(1)") >=5) & (col("first(platform)") == "ios")) |
                                                                 ((col("count(1)") ==3) & (col("gender") == 0)) |
                                                                 ((col("count(1)") ==2) & (col("gender") == 1) & (col("age_group") <= 3)) |
                                                                 ((col("sum(item_count)") <=1) & (col("first(platform)") == "android")) |
                                                                 ((col("sum(item_count)") >=10) & (col("first(platform)") == "ios")) |
                                                                 (col("action_count") >=4) |
                                                                 (col("country") == "USA") |
                                                                 ((F.datediff(F.current_timestamp(), col("creation_date")) >= 90)) |
                                                                 ((col("age_group") >= 7) & (col("gender") == 0)) |
                                                                 ((col("age_group") <= 2) & (col("gender") == 1)), 80).otherwise(20)))

churn_proba = churn_proba.withColumn("churn", F.rand()*100 < col("churn_proba"))
churn_proba = churn_proba.drop("user_id", "churn_proba", "sum(item_count)", "count(1)", "first(platform)", "action_count")
churn_proba.repartition(100).write.format("json").mode("overwrite").save(folder+"/users")
cleanup_folder(folder+"/users")
