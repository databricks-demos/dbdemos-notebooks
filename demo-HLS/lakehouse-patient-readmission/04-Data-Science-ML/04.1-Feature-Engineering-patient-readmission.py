# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # ML: Predict and reduce 30 Day Readmissions
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-flow-4.png" style="float: right; margin-left: 30px; margin-top:10px" width="650px" />
# MAGIC
# MAGIC We now have our data cleaned and secured. We saw how to create and analyze our first patient cohorts.
# MAGIC
# MAGIC Let's now take it to the next level and start building a Machine Learning model to predict wich patients are at risk. 
# MAGIC
# MAGIC We'll then be able to explain the model at a statistical level, understanding which features increase the readmission risk. This information will be critical in how we can specialize care for a specific patient population.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # 1/ Building our Features for Patient readmission analysis
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/patient-risk-ds-flow-1.png?raw=true" width="700px" style="float: right; margin-left: 10px;" />
# MAGIC Our first step is now to merge our different tables adding extra features to be able to train our model.
# MAGIC
# MAGIC We will use the `encounters_readmissions` table as the label we want to predict:
# MAGIC
# MAGIC is there a readmission within 30 days.

# COMMAND ----------

# DBTITLE 1,Get the patients and add the 30_DAY_READMISSION label
from pyspark.sql import Window
# Let's create our label: we'll predict the  30 days readmission risk
windowSpec = Window.partitionBy("PATIENT").orderBy("START")
labels = spark.table('encounters').select("PATIENT", "Id", "START", "STOP") \
              .withColumn('30_DAY_READMISSION', F.when(col('START').cast('long') - F.lag(col('STOP')).over(windowSpec).cast('long') < 30*24*60*60, 1).otherwise(0))
display(labels)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join readmission information with patient features
# MAGIC
# MAGIC Let's add features describing our patients cohort. 
# MAGIC
# MAGIC In this case we're doing some one hot encoding leveraging `get_dummies` from the Pandas API on Spark to transform categories into vector our model will be able to process.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: leverage Databricks Feature Store
# MAGIC
# MAGIC Being able to save our features in dedicated feature store simplify data management cross teamn, allowing features to be shared but also used in real-time leveraging realtime feature serving (automatically backed by ELTP databases).
# MAGIC
# MAGIC To keep this notebook simple, we won't be using the Feature store. If you are interested, open the [03.6-Feature-Store-ML-patient-readmission](/advanced-feature-store/03.6-Feature-Store-ML-patient-readmission) for a complete example.

# COMMAND ----------

import pyspark.pandas as ps

# Define Patient Features logic
def compute_pat_features(data):
  data = data.pandas_api()
  data = ps.get_dummies(data, columns=['MARITAL', 'RACE', 'ETHNICITY', 'GENDER'],dtype = 'int64').to_spark()
  return data

# COMMAND ----------

# DBTITLE 1,Select the cohort we want to analyze
cohort_name = 'COVID-19-cohort' #or could be all_patients
cohort = spark.sql(f"SELECT p.* FROM cohort c INNER JOIN patients p on c.patient=p.id WHERE c.name='{cohort_name}'") \
              .dropDuplicates(["id"])
cohort_features_df = compute_pat_features(cohort)
cohort_features_df.display()

# COMMAND ----------

# DBTITLE 1,Encounter features
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
enc_features_df = compute_enc_features(spark.table('encounters'))
display(enc_features_df)

# COMMAND ----------

enc_features_df = compute_enc_features(spark.table('encounters'))
training_dataset = cohort_features_df.join(labels, [labels.PATIENT==cohort_features_df.Id], "inner") \
                                     .join(enc_features_df, [labels.Id==enc_features_df.ENCOUNTER_ID], "inner") \
                                     .drop("Id", "_rescued_data", "SSN", "DRIVERS", "PASSPORT", "FIRST", "LAST", "ADDRESS", "BIRTHPLACE")
### Adding extra feature such as patient age at encounter
training_dataset = training_dataset.withColumnRenamed("PATIENT", "patient_id") \
                                   .withColumn("age_at_encounter", ((F.datediff(col('START'), col('BIRTHDATE'))) / 365.25))

training_dataset.write.mode('overwrite').saveAsTable("training_dataset")
display(spark.table("training_dataset"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXTRA: Going further in feature engineering with Databricks Feature store
# MAGIC
# MAGIC In this demo, we simply created a table to save our Features. Databricks offers more advanced capabilities through the use of Feature Store including collaboration, discoverabilities and realtime backend.
# MAGIC
# MAGIC For more details, open [04.6-EXTRA-Feature-Store-ML-patient-readmission]($./04.6-EXTRA-Feature-Store-ML-patient-readmission).
# MAGIC
# MAGIC *If you're starting your Data Science journey with Databricks, we recommend skipping this step and revisiting Databricks Feature Store later.*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next steps: AutoML
# MAGIC
# MAGIC Data processing for data Modeling and Machine learning is simple leveraging the lakehouse. Our features are now saved as a new table or alternatively as Feature Store Tables (see Feature store notebook for more details).
# MAGIC
# MAGIC We can now leverage Databricks AutoML to test multiple algorithms and generate the model training notebook including best practices. 
# MAGIC
# MAGIC Open [04.2-AutoML-patient-admission-risk]($./04.2-AutoML-patient-admission-risk) to start your AutoML run.
