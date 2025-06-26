# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Data Engineering
# MAGIC
# MAGIC ## Data Overview for the Demo
# MAGIC
# MAGIC The dataset used in this demo comes from Kaggle: [Iron Ore Flotation Data.](https://www.kaggle.com/datasets/edumagalhaes/quality-prediction-in-a-mining-process)
# MAGIC
# MAGIC It contains two types of data collected from an iron ore flotation plant:
# MAGIC
# MAGIC - Flotation Process Data
# MAGIC   - Includes key operational variables that directly impact the final ore quality.
# MAGIC   - This high-frequency data captures the real-time behavior of the flotation process.
# MAGIC
# MAGIC - Lab Analysis Data 
# MAGIC   - Includes quality measurements of the feed iron ore before entering the flotation process.
# MAGIC   - And includes final quality of the concentrate, measured in the lab.
# MAGIC   - Sampled every hour.
# MAGIC
# MAGIC ## Scope of this demo
# MAGIC
# MAGIC This section of the demo showcases how to implement data engineering best practices on Databricks, split across two notebooks:
# MAGIC
# MAGIC - Notebook 01a covers:
# MAGIC   - Delta Lake: Manage reliable, scalable, and ACID-compliant data pipelines.
# MAGIC   - Data Lineage: Understand and trace how data flows from raw inputs to features and predictions.
# MAGIC   - Data Quality: Enforce data expectations and monitor quality using built-in tools and frameworks (e.g., Delta Live Tables, expectations).
# MAGIC
# MAGIC - Notebook 01b covers:
# MAGIC   - Unity Catalog: Organise and govern datasets with fine-grained access control and discoverability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Data Pipeline 
# MAGIC
# MAGIC In this example, we’ll implement an end-to-end Delta Live Tables (DLT) pipeline to process Flotation and Lab data. We'll follow the medallion architecture to structure the pipeline, though the same approach can be adapted to support other data modeling patterns such as star schema or data vault.
# MAGIC
# MAGIC Using Auto Loader, we’ll incrementally ingest new data, enrich it through a series of transformations, and unify it into a curated dataset — ready to be leveraged for machine learning and advanced analytics.
# MAGIC
# MAGIC ![Data Lineage](/Workspace/Shared/iron_ore_precessing_demo/demo_setup/images/dlt_graph.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥉 Ingest Raw Files into the Bronze Layer
# MAGIC
# MAGIC We'll use Auto Loader to efficiently ingest raw Parquet files from cloud storage into the bronze layer of our pipeline. Auto Loader is designed to handle millions of files at scale, with built-in support for schema inference and schema evolution, making it ideal for dynamic data environments.
# MAGIC
# MAGIC 📚 For a deeper dive into Auto Loader, run: dbdemos.install('auto-loader')
# MAGIC
# MAGIC Let’s now integrate Auto Loader into our pipeline and begin ingesting the raw data arriving in cloud storage.

# COMMAND ----------

from pyspark.sql.functions import *

catalog_name = spark.conf.get("catalog_name")
schema_name = spark.conf.get("schema_name")
volume_name = spark.conf.get("volume_name")

@dlt.table(
    comment="Bronze table for flotation data ingested from cloud storage"
)
def bronze_flotation():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/flotation_data/")
    )

@dlt.table(
    comment="Bronze table for lab data ingested from cloud storage"
)
def bronze_lab_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/lab_data_hourly/")
    )

@dlt.table(
    comment="Bronze table for lab equipment ingested from cloud storage"
)
def bronze_lab_equipment():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/equipment/")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥈 Transform and Clean Data into the Silver Layer
# MAGIC The Silver layer consumes raw data from the Bronze layer, applying transformations to clean, enrich, and contextualise the information. In this step, we’ll aggregate the Plant Data to an hourly grain to support downstream analytics and modeling.
# MAGIC
# MAGIC We're also introducing a data quality [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on the Lab Equipment table to enforce and monitor data quality. This helps ensure that our machine learning models are trained on reliable data, while making it easier to detect and troubleshoot anomalies.

# COMMAND ----------

@dlt.table(
    comment="Silver view for aggregated flotation data"
)
def silver_flotation_data(
):
    return (
        dlt.read("bronze_flotation")
        .groupBy("date")
        .agg(*[avg(col).alias(col) for col in dlt.read("bronze_flotation").columns if col != "date"])
    )

@dlt.table(
    comment="Silver view for infeed lab data"
)
def silver_infeed_lab_data():
    return dlt.read("bronze_lab_data").select("date", "Percent_Iron_Feed", "Percent_Silica_Feed")


@dlt.table(
    comment="Silver view for concentrate lab data"
)
def silver_concentrate_lab_data():
    return dlt.read("bronze_lab_data").select("date", "Percent_Iron_Concentrate", "Percent_Silica_Concentrate", "operator_name")
    

@dlt.table(
    comment="Silver table for lab equipment"
)
@dlt.expect_or_drop("is_active", "is_active IS NOT NULL")
def silver_lab_equipment():
    return (
        dlt.read("bronze_lab_equipment").select("*")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥇 Join Clean Datasets into the Gold Layer
# MAGIC
# MAGIC With our cleaned Silver layer ready, we can now build the Gold layer, where we generate the features needed for our processing prediction model.
# MAGIC In this step, we’ll join the Flotation dataset with Lab data to enrich it with key attributes that support predictive modeling of beneficiation performance, including:
# MAGIC
# MAGIC - 🧪 Infeed Fe and Si content
# MAGIC - 🎯 Concentrate Fe and Si content
# MAGIC
# MAGIC This master dataset will serve as the foundation for training and evaluating our machine learning models.

# COMMAND ----------

@dlt.table(
    comment="Gold table for iron ore processing features"
)
def gold_iron_ore_prediction_dataset():
    silver_infeed = dlt.read("silver_infeed_lab_data")
    silver_concentrate = dlt.read("silver_concentrate_lab_data")
    silver_flotation = dlt.read("silver_flotation_data")
    return (
        silver_infeed
        .join(silver_concentrate, "date")
        .join(silver_flotation, "date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Additional ETL and Data Ingestion Capabilities
# MAGIC
# MAGIC ### 1.2.1 LakeFlow connect
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../_resources/images/LakeFlow1.png" width="800px"/> 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../_resources/images/LakeFlow2.png" width="800px"/> 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../_resources/images/LakeFlow3.png" width="800px"/> 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2.2 AVEVA Connect
# MAGIC
# MAGIC Aveva connect allows Delta Sharing with Databricks for easier PI data ingestion. Additional information can be found here:  https://www.databricks.com/dataaisummit/session/unlocking-industrial-intelligence-aveva-and-agnico-eagle
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../_resources/images/Aveva_connect.png" width="800px"/> 
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../_resources/images/Aveva_connect_2.png" width="800px"/> 
# MAGIC </div>
