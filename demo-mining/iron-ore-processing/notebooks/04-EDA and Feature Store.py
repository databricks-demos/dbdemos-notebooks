# Databricks notebook source
# MAGIC %run ../demo_setup/00.Initial_library_install

# COMMAND ----------

from pyspark.sql.functions import col
from databricks.feature_store import FeatureStoreClient

from pprint import pprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *

fs = FeatureStoreClient()
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. EDA and Feature Store
# MAGIC
# MAGIC In this section, we’ll demonstrate how Databricks Notebooks accelerate exploratory data analysis (EDA) with native visualisations, and how to seamlessly store and manage feature tables using the Databricks Feature Store — including the ability to serve features in real time via the online feature store for low-latency model inference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 EDA
# MAGIC
# MAGIC
# MAGIC - We now load the dataset produced by the ingestion and data engineering pipelines. This will serve as the foundation for exploratory analysis, data cleaning and feature identification.
# MAGIC - We will use the `display()` fuction in Databricks, which is a powerful tool for exploratory data analysis (EDA), allowing users to interactively explore DataFrames through sortable tables, built-in visualisations (e.g., bar charts, histograms, scatter plots), and summary statistics — all without writing additional code. It makes it easy to quickly spot trends, outliers, and data quality issues during early stages of analysis.

# COMMAND ----------

gold_df = spark.sql(f"SELECT * FROM {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.gold_iron_ore_prediction_dataset")
display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Data Cleansing
# MAGIC
# MAGIC We have identified from the charts above, times where the ore feed % values are constant - either due to data quality issues or missed measurements. We remove these rows from the dataset to ensure that the predictions are only using data whilst the plant was live and both feed and concentrate lab data is available.
# MAGIC
# MAGIC

# COMMAND ----------

# Filter out rows between the specified dates
start_date = "2017-05-12"
end_date = "2017-06-14"
df_filtered = gold_df.filter(~(col("date").between(start_date, end_date)))

start_date = "2017-07-23"
end_date = "2017-08-03"
df_filtered = df_filtered.filter(~(col("date").between(start_date, end_date)))

start_date = "2017-08-07"
end_date = "2017-08-14"
df_filtered = df_filtered.filter(~(col("date").between(start_date, end_date)))

df_filtered = df_filtered.filter(col("Percent_Iron_Feed") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 2.2 Feature Store
# MAGIC
# MAGIC The cleaned feature table from Section 2.1 will now be logged to the Databricks Feature Store, where it can be easily reused for training machine learning models, ensuring consistency and traceability across workflows.
# MAGIC
# MAGIC Under the hood, the Databricks Feature Store is powered by Delta Tables, offering both performance and flexibility. You can even define feature tables using SQL syntax — though for this demo, we’ll use the Databricks Feature Store Python client for simplicity.
# MAGIC
# MAGIC As highlighted in Section 1, integrating Delta Tables with Unity Catalog brings powerful enterprise-grade capabilities to your feature tables, including:
# MAGIC
# MAGIC - 🔐 Row-level filtering and column masking for secure data access
# MAGIC - 🕰️ Time travel and table versioning for reproducibility
# MAGIC - 📜 Table history and audit logs for compliance and traceability
# MAGIC - 🔗 End-to-end lineage at both table and column level
# MAGIC
# MAGIC In addition, features can be automatically published to an **online store**, enabling real-time model serving with low-latency access to the latest feature values.
# MAGIC
# MAGIC You can explore your features in the Feature Registry in the side menu : [Link](https://e2-demo-field-eng.cloud.databricks.com/feature-store?o=1444828305810485)
# MAGIC
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/2021/12/feature-store-img-2.png?v=1660758008" style="float: right" width="800px">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's Create a New Feature Table

# COMMAND ----------

feature_table_name = "fs_gold_iop_features"

# Drop the fs table if it was already existing to cleanup the demo state
drop_fs_table(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{feature_table_name}")

# Create feature table using the feature store client
fs.create_table(
    name=f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{feature_table_name}",
    primary_keys=["date"],
    df=df_filtered,
    description="Features for Iron Ore Processing Prediction."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Feature Table to Catalog
# MAGIC
# MAGIC Now that the feature table has been generated, we write this to the catalog as a delta table with a primary key of the date column.

# COMMAND ----------

#set CDF on feature store table
spark.sql(f"ALTER TABLE {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{feature_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Create an online table
spec = OnlineTableSpec(
  primary_key_columns=["date"],
  source_table_full_name=f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{feature_table_name}",
  run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({'triggered': 'true'})
)

online_table_name = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{feature_table_name}_online_table"

# Check if the online table already exists
try:
    w.online_tables.get(name=online_table_name)
    print(f"Table {online_table_name} already exists.")
except:
    online_table = OnlineTable(
      name=online_table_name,  # Fully qualified table name
      spec=spec  # Online table specification
    )
    w.online_tables.create_and_wait(table=online_table)
    print(f"Table {online_table_name} created successfully.")