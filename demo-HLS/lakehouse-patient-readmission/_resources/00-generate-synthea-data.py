# Databricks notebook source
# MAGIC %md
# MAGIC # Simulating patient records with 
# MAGIC
# MAGIC ## TODO: let's move this as a one-off operation and save the data to our dataset repo, and download the data from the repo instead of generating it like this
# MAGIC
# MAGIC [Synthea](https://synthetichealth.github.io/synthea/)
# MAGIC
# MAGIC
# MAGIC `Synthea(TM) is a Synthetic Patient Population Simulator. The goal is to output synthetic, realistic (but not real), patient data and associated health records in a variety of formats.`
# MAGIC In this notebook, we show how to simulate patient records in parallele for patients accross the US. You can modify the code for your experiments
# MAGIC
# MAGIC Borrowed from: [hls-solution-accelerators](https://github.com/databricks/hls-solution-accelerators)
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00-generate-synthea-data&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

demo_data_folder = volume_folder
synth_out = demo_data_folder+'/data'
landing_data_folder = demo_data_folder+'/landing_zone'
landing_data_folder_parquet = demo_data_folder+'/landing_zone_parquet'

#Cleanup any existing folders
assert demo_data_folder.startswith("/Volume/") and len(demo_data_folder) > 20  #make sure we don't delete something outside of dbdemos
dbutils.fs.rm(demo_data_folder, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cluster setup
# MAGIC Use a single node cluster

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC rm -rf /databricks/driver/synthea || true
# MAGIC cd /databricks/driver/
# MAGIC git clone https://github.com/synthetichealth/synthea.git
# MAGIC cd /databricks/driver/synthea
# MAGIC GRADLE_OPTS="-Xmx4g" && ./gradlew build check -x test -Dorg.gradle.java.home=/usr/lib/jvm/zulu11

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run synthea simulations with specified module

# COMMAND ----------

module_url="https://raw.githubusercontent.com/synthetichealth/synthea/master/src/main/resources/modules/congestive_heart_failure.json"
# module_url="https://raw.githubusercontent.com/synthetichealth/synthea/master/src/main/resources/modules/covid19/diagnose_blood_clot.json"
module_name=module_url.split('/')[-1].replace('.json','')
synth_out=f"{synth_out}/{module_name}"
module_path=f"{synth_out}/{module_name}.json"
print(f"synth_out:{synth_out}\nmodule_name:{module_name}\nmodule_path:{module_path}")

# COMMAND ----------

try:
  dbutils.fs.ls(synth_out.replace('/dbfs',''))
except:
  dbutils.fs.mkdirs(synth_out.replace('/dbfs',''))

# COMMAND ----------

import os
os.environ['POP_SIZE']="20000"
os.environ['MODULE_URL']=module_url
os.environ['SYNTH_OUT']="/dbfs"+synth_out
os.environ['MODULE_PATH']="/dbfs"+module_path

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $SYNTH_OUT || true
# MAGIC wget $MODULE_URL -O $MODULE_PATH
# MAGIC ls $MODULE_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC Next step is to define a python wrapper function to run synthea. This function takes state and population size as parameter. `run-parameters` can be modified based on synthea configurations.

# COMMAND ----------

# DBTITLE 1,Generate data.
# MAGIC %sh
# MAGIC cd /databricks/driver/synthea
# MAGIC ./gradlew run -Params="['Massachusetts','-p','$POP_SIZE','--exporter.fhir.export','false','--exporter.csv.export','true','--exporter.baseDirectory','$SYNTH_OUT','--exporter.csv.folder_per_run','false','--generate.log_patients.detail','none','--exporter.clinical_note.export','false','--m','$MODULE_PATH',]" -Dorg.gradle.java.home=/usr/lib/jvm/zulu11

# COMMAND ----------

#keep only a subset of the tables to simplify the data
table_dependencies = ['encounters', 'patients', 'conditions', 'procedures', 'observations', 'medications', 'immunizations']
#move individual file within a folder
for folder in dbutils.fs.ls(synth_out+"/csv"):
  table = folder.name[:-len(".csv")]
  if table in table_dependencies:
    print(folder)
    dbutils.fs.mv(folder.path, landing_data_folder+'/'+table+'/01-'+table+'.csv')
#dbutils.fs.rm(synth_out)

# COMMAND ----------

dbutils.fs.ls(landing_data_folder)

# COMMAND ----------

#Create the data as parquet to make smaller & reuse-them later (ex: save them in an external repo/bucket or other to avoid re-running the data generation which can be slow)
for folder in dbutils.fs.ls(landing_data_folder):
  if folder.name not in ["csv/", "fhir/", "metadata/"]:
    print(folder)
    table = folder.name
    #dbutils.fs.mv(folder.path, '/dbdemos/hls/synthea/data/congestive_heart_failure/'+table+'/')
    (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", f"{volume_folder}/ckpt/schema/{table}")
                  .option("cloudFiles.inferColumnTypes", "true")
                  .load(folder.path)
                  .writeStream.trigger(once=True).option("checkpointLocation", f"{volume_folder}/ckpt/{table}").format("parquet").start(f"{landing_data_folder_parquet}/{table}"))

# COMMAND ----------

#TODO add location_ref ?
"""
spark.table("hive_metastore.field_demos_hls_omop.location_ref").write.format("json").save(landing_data_folder+"/location_ref")
spark.table("hive_metastore.field_demos_hls_omop.location_ref").write.format("parquet").save(landing_data_folder_parquet+"/location_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Smolder |Apache-2.0 License| https://github.com/databrickslabs/smolder | https://github.com/databrickslabs/smolder/blob/master/LICENSE|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
# MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
# MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|
