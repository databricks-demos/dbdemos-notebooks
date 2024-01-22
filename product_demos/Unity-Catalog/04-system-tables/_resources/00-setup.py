# Databricks notebook source
dbutils.widgets.text('reset_all_data', 'false')

# Change your schema here:
catalog = "main"
schema = "billing_forecast"

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

if reset_all_data:
  print(f'clearing up db {schema}')
  spark.sql(f"DROP DATABASE IF EXISTS `{schema}` CASCADE")

# COMMAND ----------

def use_and_create_db(catalog, schema, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  spark.sql(f"USE CATALOG `{catalog}`")
  spark.sql(f"""create database if not exists `{schema}` """)

assert catalog not in ['hive_metastore', 'spark_catalog']
#If the catalog is defined, we force it to the given value and throw exception if not.
if len(catalog) > 0:
  current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
  if current_catalog != catalog:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs:
      spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
      if catalog == 'dbdemos':
        spark.sql(f"ALTER CATALOG {catalog} OWNER TO `account users`")
  use_and_create_db(catalog, schema)

if catalog == 'dbdemos':
  try:
    spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{schema} TO `account users`")
    spark.sql(f"ALTER SCHEMA {catalog}.{schema} OWNER TO `account users`")
  except Exception as e:
    print("Couldn't grant access to the schema to all users:"+str(e))    

print(f"using catalog.database `{catalog}`.`{schema}`")
spark.sql(f"""USE `{catalog}`.`{schema}`""")    
