# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # ML & Feature store: Leverage Databricks Feature Tables to share cohorts
# MAGIC
# MAGIC ### Register Feature Store Tables
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="650" />
# MAGIC
# MAGIC Databricks provide Feature Store capabilities, simplifying operational work and increasing discoverability among your Data Scientist team, accelerating Analysis.
# MAGIC
# MAGIC Under the hood, feature store are backed by a Delta Lake table. This will allow discoverability and reusability of our feature across our organization, increasing team efficiency. <br/>
# MAGIC These tables can be backed with Online tables (such as DynamoDB or CosmoDB), provinding instant feature lookup and transformations (ms) for realtime inferences.
# MAGIC
# MAGIC Databricks Feature Store brings advanced capabilities to accelerate and simplify your ML journey, such as point in time support and online-store, fetching your features within ms for real time Serving. 
# MAGIC
# MAGIC ### Why use Databricks Feature Store?
# MAGIC
# MAGIC Databricks Feature Store is fully integrated with other components of Databricks.
# MAGIC
# MAGIC * **Discoverability**. The Feature Store UI, accessible from the Databricks workspace, lets you browse and search for existing features.
# MAGIC
# MAGIC * **Lineage**. When you create a feature table with Feature Store, the data sources used to create the feature table are saved and accessible. For each feature in a feature table, you can also access the models, notebooks, jobs, and endpoints that use the feature.
# MAGIC
# MAGIC * **Batch and Online feature lookup for real time serving**. When you use features from Feature Store to train a model, the model is packaged with feature metadata. When you use the model for batch scoring or online inference, it automatically retrieves features from Feature Store. The caller does not need to know about them or include logic to look up or join features to score new data. This makes model deployment and updates much easier.
# MAGIC
# MAGIC * **Point-in-time lookups**. Feature Store supports time series and event-based use cases that require point-in-time correctness.
# MAGIC
# MAGIC
# MAGIC For more details about Databricks Feature Store, run `dbdemos.install('feature-store')`

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false $catalog=dbdemos $db=hls_patient_readmission

# COMMAND ----------

# MAGIC %md 
# MAGIC ## How to use this notebook
# MAGIC This notebook is added as extra, and would typically be used as replacement instead of the first [04.1-Feature-Engineering-patient-readmission]($./04.1-Feature-Engineering-patient-readmission) notebook.
# MAGIC
# MAGIC For more details on feature store, install `dbdemos.install('feature-store')`

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Patient Features
# MAGIC Let's start with our patient features. We'll encode our categories leveraging pandas on spark API and the `get_dummies()` function.
# MAGIC
# MAGIC Pandas On Spark (previously Koalas) makes transformation at scale easy, leveraging the well-known pandas API with spark distributed backend.

# COMMAND ----------

import pyspark.pandas as ps

# Define Patient Features logic
def compute_pat_features(data):
  data = data.pandas_api()
  data = ps.get_dummies(data, columns=['MARITAL', 'RACE', 'ETHNICITY', 'GENDER'],dtype = 'int64').to_spark()
  return data

# COMMAND ----------

pat_features_df = compute_pat_features(spark.table('patients_ml').dropDuplicates(["Id"]))
pat_features_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Creating the Feature Table
# MAGIC Now that our features are ready, let's save them as a Feature Table

# COMMAND ----------

# DBTITLE 1,Patient Feature Table
# Instantiate the Feature Store Client
fs = feature_store.FeatureStoreClient()

drop_fs_table(f'{dbName}.pat_features')

pat_feature_table = fs.create_table(
  name=f'{dbName}.pat_features',
  primary_keys=['Id'],
  df=pat_features_df,
  description='Base Features from the Patient Table'
)

fs.write_table(df=pat_features_df, name=f'{dbName}.pat_features', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encounter Feature Table
# MAGIC We can now repeat the same steps with a new encounter Feature Table

# COMMAND ----------

# DBTITLE 1,Encounter features
import pyspark.pandas as ps
def compute_enc_features(data):
  data = data.dropDuplicates(["Id"])
  data = data.withColumn('enc_length', F.unix_timestamp(col('stop'))- F.unix_timestamp(col('start')))
  data = data.pandas_api()
#   return data
  data = ps.get_dummies(data, columns=['ENCOUNTERCLASS'],dtype = 'int64').to_spark()
  
  return (
    data
    .select(
      col('Id').alias('ENCOUNTER_ID'),
      'BASE_ENCOUNTER_COST',
      'TOTAL_CLAIM_COST',
      'PAYER_COVERAGE',
      'enc_length',
      'ENCOUNTERCLASS_ambulatory',
      'ENCOUNTERCLASS_emergency',
      'ENCOUNTERCLASS_hospice',
      'ENCOUNTERCLASS_inpatient',
      'ENCOUNTERCLASS_outpatient',
      'ENCOUNTERCLASS_wellness',
    )
  )

# COMMAND ----------

# DBTITLE 1,Save as Feature Table
enc_features_df = compute_enc_features(spark.table('encounters_ml'))

drop_fs_table(f'{dbName}.enc_features')

#Note: You might need to delete the FS table using the UI
enc_feature_table = fs.create_table(
  name=f'{dbName}.enc_features',
  primary_keys=['ENCOUNTER_ID'],
  df=enc_features_df,
  description='Base and derived features from the Encounter Table'
)

fs.write_table(df=enc_features_df, name=f'{dbName}.enc_features', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding extra Feature Table for each patient: age at encounter

# COMMAND ----------

# DBTITLE 1,Create the features
def compute_age_at_enc(encounters, patients):
  #for the demo, we drop potential duplicates in case data is re-inserted in the ingestion part
  encounters = encounters.dropDuplicates(["Id"])
  patients = patients.dropDuplicates(["Id"])
  return (
    encounters
    .join(patients, patients['id'] == encounters['PATIENT'])
    .select(
      encounters.Id.alias('encounter_id'),
      patients.Id.alias('patient_id'),
      ((F.datediff(col('START'), col('BIRTHDATE'))) / 365.25).alias('age_at_encounter')
    )
  )

# COMMAND ----------

# DBTITLE 1,Save as feature table
aae_features_df = compute_age_at_enc(spark.table('encounters_ml'), spark.table('patients_ml'))

drop_fs_table(f'{dbName}.age_at_enc_features')

#Note: You might need to delete the FS table using the UI
aae_feature_table = fs.create_table(
  name=f'{dbName}.age_at_enc_features',
  primary_keys=['encounter_id'],
  df=aae_features_df,
  description='determine the age of the patient at the time of the encounter'
)

fs.write_table(df=aae_features_df, name=f'{dbName}.age_at_enc_features', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC %md
# MAGIC ### Building a dataset from the feature store
# MAGIC
# MAGIC Now that our feature tables are created, we can query them to build training datasets.
# MAGIC
# MAGIC This is done by building Feature Lookups, specifying the key used to retrive the data. 
# MAGIC
# MAGIC This is conceptually close to a SQL JOIN on the key between between your label dataset and the feature store tables. For offline batch this is done with Spark as a backend, and for realtime feature lookup an Key/Value backend can be used to request the features for a given key.

# COMMAND ----------

# DBTITLE 1,Get our labels
#Note that labels should not be part of the features
from pyspark.sql import Window
# Let's create our label: we'll predict the  30 days readmission risk
windowSpec = Window.partitionBy("PATIENT").orderBy("START")
labels = spark.table('encounters_ml').select("PATIENT", "Id", "START", "STOP") \
              .withColumn('30_DAY_READMISSION', F.when(col('START').cast('long') - F.lag(col('STOP')).over(windowSpec).cast('long') < 30*24*60*60, 1).otherwise(0))
display(labels)

# COMMAND ----------

# DBTITLE 1,Feature lookups
from databricks.feature_store import FeatureLookup
patient_feature_lookups = [
   FeatureLookup( 
     table_name = f'{dbName}.pat_features',
     feature_names = [
      'MARITAL_M',
      'MARITAL_S',
      'RACE_asian',
      'RACE_black',
      'RACE_hawaiian',
      'RACE_other',
      'RACE_white',
      'ETHNICITY_hispanic',
      'ETHNICITY_nonhispanic',
      'GENDER_F',
      'GENDER_M',
      'INCOME'],
     lookup_key = ["PATIENT"]
   )
]
 
encounter_feature_lookups = [
   FeatureLookup( 
     table_name = f'{dbName}.enc_features',
     feature_names = ['BASE_ENCOUNTER_COST', 'TOTAL_CLAIM_COST', 'PAYER_COVERAGE', 'enc_length', 'ENCOUNTERCLASS_ambulatory', 'ENCOUNTERCLASS_emergency', 'ENCOUNTERCLASS_hospice', 'ENCOUNTERCLASS_inpatient', 'ENCOUNTERCLASS_outpatient', 'ENCOUNTERCLASS_wellness',],
     lookup_key = ["Id"]
   )
]

age_at_enc_feature_lookups = [
   FeatureLookup( 
     table_name = f'{dbName}.age_at_enc_features',
     feature_names = ['age_at_encounter'],
     lookup_key = ["Id"]
   )
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Feature Store to Create Dataset Based on Lookups

# COMMAND ----------

fs = feature_store.FeatureStoreClient()
training_set = fs.create_training_set(
  labels,
  feature_lookups = patient_feature_lookups + encounter_feature_lookups + age_at_enc_feature_lookups,
  label = "30_DAY_READMISSION",
  exclude_columns = ['START', 'STOP']
)

training_df = training_set.load_df()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congrats! our training dataset is ready, retreiving features from our Feature Tables.
# MAGIC
# MAGIC We now continue our ML steps such as calling AutoML with the training Dataset.
# MAGIC
# MAGIC For more details on Databricks Feature Store and more advanced capabilities (Online Store for realtime lookup, streaming updates, timeseries processing...), install the feature store demo: `dbdemos.install('feature-store')` 

# COMMAND ----------

# DBTITLE 1,Running our model training from the feature store training set with automl
from databricks import automl
#summary = automl.classify(training_dataset.select(feature_names), target_col="30_DAY_READMISSION", primary_metric="roc_auc", timeout_minutes=6)
# ...
# See 04.2-AutoML-patient-admission-risk for more details on how to deploy the AutoML model.


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next: Continue the model deployment and serving 
# MAGIC
# MAGIC Our Feature Store tables are now available for all the organization and can be leverage to train models on any patient cohorts. 
# MAGIC
# MAGIC Data Scientists can easily explore these tables, and leverage them to bootstrap their experience.
# MAGIC
# MAGIC Once your feature store is deployed, the same steps apply for model training and inference. See the [04.3-Batch-Scoring-patient-readmission]($./04.3-Batch-Scoring-patient-readmission) for the next steps.
