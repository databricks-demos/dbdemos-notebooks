# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Policy, Claims & Telematics DLT Ingestion pipeline
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-0.png?raw=true" style="float: right" width="800px">
# MAGIC
# MAGIC Using <b>Delta Live Tables</b> (DLT) for ETL helps simplify and operationalize the pipeline with its support for autoloader, data quality via constraints, efficient auto-scaling for streaming workloads, resiliency via restart on failure, execution of administrative operations among others.
# MAGIC
# MAGIC We'll show how Databricks makes it easy to incrementally ingest and transform our incoming claims data.
# MAGIC
# MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Ingestion made it easy with Databricks Assistant
# MAGIC
# MAGIC Databricks Data Intelligent Platform simplify our journey, empowering Data Analyst to do more and build robust Data Pipelines.
# MAGIC
# MAGIC Questions can be asked in plain text to our assistant, which will suggest how to build the pipeline, fix bug and offer potential improvements.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01.1-DLT-Ingest-Policy-Claims&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC This demo started for you a <a dbdemos-pipeline-id="dlt-fsi-smart-claims" href="#joblist/pipelines/bf6b21bb-ff10-480c-bdae-c8c91c76d065" target="_blank">DLT Pipeline</a> using this notebook! Explore it to see its execution out of the box.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Let's start our Raw Data ingestion.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-1.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC The raw data is saved within our Unity Catalog Volume.
# MAGIC
# MAGIC Databricks makes it easy to incrementally ingest raw files and save them as a Delta Table using  `cloud_files`. 
# MAGIC
# MAGIC In this notebook, we'll be using Python, but we could have declare the same transformation in pure SQL.  You could ask the assistant to convert it from python to SQL ! 

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, col
from pyspark.sql import types as T

catalog = "main"
db = "fsi_smart_claims"
volume_name = "volume_claims"

# COMMAND ----------

@dlt.table(comment="The raw claims data loaded from json files.")
def raw_claim():
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .load(f"/Volumes/{catalog}/{db}/{volume_name}/Claims"))

# COMMAND ----------

@dlt.table(comment="Policy data loaded from csv files.")
def raw_policy():
    return (
      spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaHints", "ZIPCODE int")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"/Volumes/{catalog}/{db}/{volume_name}/Policies"))

# COMMAND ----------

@dlt.table(comment="Load Telematics (IoT) streaming data")
def raw_telematics():
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "parquet")
          .load(f"/Volumes/{catalog}/{db}/{volume_name}/Telematics"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Clean data and tables
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-2.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC Once our raw data are ingested, we can start adding some transformation and business logic to cleanup our data.
# MAGIC
# MAGIC These tables are typically made available to downstream teams, such as Data Scientists our Data Analysts.

# COMMAND ----------

def flatten_struct(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            for child in field.dataType:
                df = df.withColumn(field.name + '_' + child.name, F.col(field.name + '.' + child.name))
            df = df.drop(field.name)
    return df

# COMMAND ----------

@dlt.table(comment="Average telematics")
def telematics():
  return (dlt.read('raw_telematics').groupBy("chassis_no").agg(
                F.avg("speed").alias("telematics_speed"),
                F.avg("latitude").alias("telematics_latitude"),
                F.avg("longitude").alias("telematics_longitude")))
                
@dlt.table
@dlt.expect_all({"valid_policy_number": "policy_no IS NOT NULL"})
def policy():
    # Read the staged policy records into memory
    return (dlt.readStream("raw_policy")
                .withColumn("premium", F.abs(col("premium")))
                # Reformat the incident date values
                .withColumn("pol_eff_date", F.to_date(col("pol_eff_date"), "dd-MM-yyyy"))
                .withColumn("pol_expiry_date", F.to_date(col("pol_expiry_date"), "dd-MM-yyyy"))
                .withColumn("pol_issue_date", F.to_date(col("pol_issue_date"), "dd-MM-yyyy"))
                .withColumn("address", concat(col("BOROUGH"), lit(", "), col("ZIP_CODE").cast("string")))
                .drop('_rescued_data'))

@dlt.table
@dlt.expect_all({"valid_claim_number": "claim_no IS NOT NULL"})
def claim():
    # Read the staged claim records into memory
    claim = dlt.readStream("raw_claim")
    claim = flatten_struct(claim)  
    
    # Update the format of all date/time features
    return (claim.withColumn("claim_date", F.to_date(F.col("claim_date")))
                 .withColumn("incident_date", F.to_date(F.col("incident_date"), "yyyy-MM-dd"))
                 .withColumn("driver_license_issue_date", F.to_date(F.col("driver_license_issue_date"), "dd-MM-yyyy"))
                 .drop('_rescued_data'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Joining our table together
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-3.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC Let's now join our policy and claim information within a single table.

# COMMAND ----------

@dlt.table(comment = "Curated claim joined with policy records")
def claim_policy():
    # Read the staged policy records
    policy = dlt.read("policy")
    # Read the staged claim records
    claim = dlt.readStream("claim")
    
    return claim.join(policy, on="policy_no")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, we demonstrated how <b> policy, claims & telematics </b> data flow into the system at different speeds, in different formats, from different sources and are consolidated in the claims lakehouse to weave the story of what happened and why and what to do next. 
# MAGIC
# MAGIC
# MAGIC In the next notebook, we'll use an open-source geocoding library to augment telematics data to provide more details on the exact location of the driver and vehicle at the time of the incident.
# MAGIC
# MAGIC Open the [01.2-Policy-Location]($./01.2-Policy-Location) Notebook to explore a more advanced transformation, or go back to the [00-Smart-Claims-Introduction]($../00-Smart-Claims-Introduction) to explore AI and Data Visualization.
# MAGIC
