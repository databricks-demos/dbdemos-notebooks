# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# Create a catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create a schema within the catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

#create a managed volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

# Grant all permissions on the catalog to all account users (crude but will avoid downstream issues)
spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO `account users`")

# COMMAND ----------

def create_mock_plant_equipment():
    data = [
        ("Spectrophotometer", 1, True),
        ("pH Meter", 2, True),
        ("Oven", 3, True),
        ("Remote Arm", 4, False),
        ("Thermometer", 5, None),
    ]
    
    columns = ["name", "id", "is_active"]
    df_equipment = spark.createDataFrame(data, columns)
    
    return df_equipment

# COMMAND ----------

import requests
import zipfile
import io

# Download the zip file
url = "https://www.kaggle.com/api/v1/datasets/download/edumagalhaes/quality-prediction-in-a-mining-process"
response = requests.get(url)
zip_file = zipfile.ZipFile(io.BytesIO(response.content))

# Extract the CSV file to a UC volume
csv_filename = "MiningProcess_Flotation_Plant_Database.csv"
zip_file.extract(csv_filename, f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/")

# COMMAND ----------

# Define the file path
file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/MiningProcess_Flotation_Plant_Database.csv"

# Read the CSV file into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

from pyspark.sql.functions import col, hour, when, concat_ws, lit, avg
from pyspark.sql import functions as F
import random
import pandas as pd

# Rename columns to remove invalid characters
df = df.select([F.col(col).alias(col.replace(" ", "_").replace("%", "Percent")) for col in df.columns])

for col in df.columns:
    if col != "date":
        df = df.withColumn(col, F.regexp_replace(col, r",", ".").cast("double"))

flotation_columns = [
    "date",
    "Starch_Flow",
    "Amina_Flow", 
    "Ore_Pulp_Flow",
    "Ore_Pulp_pH", 
    "Ore_Pulp_Density", 
    "Flotation_Column_01_Air_Flow", 
    "Flotation_Column_02_Air_Flow",
    "Flotation_Column_03_Air_Flow",
    "Flotation_Column_04_Air_Flow",
    "Flotation_Column_05_Air_Flow",
    "Flotation_Column_06_Air_Flow",
    "Flotation_Column_07_Air_Flow",
    "Flotation_Column_01_Level",
    "Flotation_Column_02_Level",
    "Flotation_Column_03_Level",
    "Flotation_Column_04_Level",
    "Flotation_Column_05_Level",
    "Flotation_Column_06_Level",
    "Flotation_Column_07_Level",
]
lab_data_columns = [
    "date",
    "Percent_Iron_Feed",
    "Percent_Silica_Feed",
    "Percent_Iron_Concentrate",
    "Percent_Silica_Concentrate",
]
df_flotation = df.select(*flotation_columns)
df_flotation.write.format("parquet") \
    .mode("overwrite") \
    .save(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/flotation_data/")

df_lab = df.select(*lab_data_columns)

df_lab_hourly = df_lab.groupBy("date").agg(
    avg("Percent_Iron_Feed").alias("Percent_Iron_Feed"),
    avg("Percent_Silica_Feed").alias("Percent_Silica_Feed"),
    avg("Percent_Iron_Concentrate").alias("Percent_Iron_Concentrate"),
    avg("Percent_Silica_Concentrate").alias("Percent_Silica_Concentrate")
)

# Mock shift operators for PII demo

# 1. List of fake operator names
operator_names = [
    "Alice Johnson", "Ben Carter", "Cindy Lee", "David Smith", "Emma Wright",
    "Frank Miller", "Grace Kim", "Henry Jones", "Isla Clarke", "Jack White"
]

# Broadcast the list and create a shift ID based on date and shift
df_with_shift = df_lab_hourly.withColumn("shift_type", when(
    (hour("date") >= 6) & (hour("date") < 18), "day"
).otherwise("night"))

# Create a shift identifier (e.g., "2024-05-13_day")
df_with_shift = df_with_shift.withColumn("shift_id",
    concat_ws("_", F.to_date("date"), F.col("shift_type"))
)

# Get distinct shifts
distinct_shifts = df_with_shift.select("shift_id").distinct().collect()

# Assign a random operator to each shift
shift_operator_map = {row["shift_id"]: random.choice(operator_names) for row in distinct_shifts}

# Convert to a DataFrame for joining
shift_df = spark.createDataFrame(shift_operator_map.items(), ["shift_id", "operator_name"])

# Join operator name back to the main DataFrame
df_with_operator = df_with_shift.join(shift_df, on="shift_id", how="left").drop("shift_id")

df_with_operator.write.format("parquet") \
    .mode("overwrite") \
    .save(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/lab_data_hourly/")

#mock equipment with a None isactive field that we can use to demo expecations
df_equipment = create_mock_plant_equipment()
df_equipment.write.format("parquet") \
    .mode("overwrite") \
    .save(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/equipment/")

display(df_with_operator)
display(df_flotation)


# COMMAND ----------

