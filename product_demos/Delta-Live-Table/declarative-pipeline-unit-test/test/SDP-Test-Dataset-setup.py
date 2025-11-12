# Databricks notebook source
# MAGIC %md 
# MAGIC # Test Datasets setup
# MAGIC
# MAGIC We have 2 files that we'll be using as dataset saved in git within the project (you can open them directly under the `dataset` folder). 
# MAGIC
# MAGIC All we have to do is move these local files to our blob storage so that we can read them within our SDP test pipeline.
# MAGIC
# MAGIC *Note: We could also have used Faker to generate them dynamically.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=LDP-Test-Dataset-setup&demo_name=dlt-unit-test&event=VIEW">

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_sdp_unit_test"
volume_name = "raw_data"

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS `{catalog}`')
spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`')
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

# DBTITLE 1,Move our test resources to DBFS for tests
#We could use repo arbitrary files, but we'll prefer working without files to support workpsace (non repo) deployment too.
#dbutils.fs.rm("/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/customers/users_json", True)
#dbutils.fs.mkdirs("/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/users_json")
#dbutils.fs.mkdirs("/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/spend_csv")

#import shutil
#shutil.copyfile("./dataset/users.json", "/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/users_json/users.json")
#shutil.copyfile("./dataset/spend.csv", "/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/spend_csv/spend.csv")


spend_csv = """id,age,annual_income,spending_core
3,47,858.9,99.4
1,47,861.9,48.1
2,97,486.4,880.8
4,,283.8,117.8
,95,847.5,840.9
invalid_id,1,514.5,284.5"""

dbutils.fs.put('/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/spend_csv/spend.csv', spend_csv, True)
    
users_json = """{"id":1,"email":"joneschristina@example.org","creation_date":"11-28-2021 12:08:46","last_activity_date":"08-20-2021 08:24:44","firstname":"Randall","lastname":"Espinoza","address":"71571 Jennifer Creek - East John, CO 81653","city":"Port Nicholas","last_ip":"22.207.225.77","postcode":"62389"}
{"id":4,"email":"christybautista@example.net","creation_date":"06-30-2022 22:51:30","last_activity_date":"08-22-2021 17:25:06","firstname":"Jose","lastname":"Bell","address":"865 Young Crest - Lake Adriennebury, VA 67749","city":"Brownstad","last_ip":"159.111.101.250","postcode":"52432"}
{"id":0,"email":"amccormick@example.com","creation_date":"10-21-2021 02:37:38","last_activity_date":"07-22-2021 15:06:48","firstname":"Dylan","lastname":"Barber","address":"7995 Ronald Flat Suite 597 - Williefurt, AL 37894","city":"Port Steven","last_ip":"173.88.213.168","postcode":"58368"}
{"id":3,"email":"jenniferbennett@example.org","creation_date":"07-06-2022 12:27:24","last_activity_date":"01-09-2022 15:04:45","firstname":"Phillip","lastname":"Morgan","address":"523 Garza Crossroad - New Maryview, OK 92301","city":"Julieshire","last_ip":"170.233.120.199","postcode":"34528"}
{"id":2,"email":"alexis25@example.org","creation_date":"09-10-2021 02:31:37","last_activity_date":"01-11-2022 20:39:01","firstname":"Gregory","lastname":"Crane","address":"068 Shawn Port - West Jessica, KS 84864","city":"South Tonya","last_ip":"192.220.63.96","postcode":"88033"}
{"email":"davidporter@example.com","creation_date":"05-28-2022 09:54:50","last_activity_date":"12-18-2021 21:48:48","firstname":"Jeremy","lastname":"Knight","address":"06183 Acevedo Bypass - Petermouth, ME 34177","city":"West Brianburgh","last_ip":"53.240.159.208","postcode":"73380"}
{"id":"invalid ID","email":"margaret84@example.com","creation_date":"12-20-2021 19:57:28","last_activity_date":"07-27-2021 09:39:28","firstname":"Angela","lastname":"Adams","address":"098 Daniel Ferry Suite 565 - South Andrea, ND 36326","city":"New Mariafort","last_ip":"7.176.250.65","postcode":"21300"}"""

dbutils.fs.put('/Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/users_json/users.json', users_json, True)

# COMMAND ----------

# In this example, we'll store our rules as a delta table for more flexibility & reusability. 
# While this isn't directly related to Unit test, it can also help for programatical analysis/reporting.

data = [
 # tag/table name      name              constraint
 ("user_bronze_sdp",  "correct_schema", "_rescued_data IS NULL"),
 ("user_silver_sdp",  "valid_id",       "id IS NOT NULL AND id > 0"),
 ("spend_silver_sdp", "valid_id",       "id IS NOT NULL AND id > 0"),
 ("user_gold_sdp",    "valid_age",      "age IS NOT NULL"),
 ("user_gold_sdp",    "valid_income",   "annual_income IS NOT NULL"),
 ("user_gold_sdp",    "valid_score",    "spending_core IS NOT NULL")
]
#Typically only run once, this doesn't have to be part of the SDP pipeline.
spark.createDataFrame(data=data, schema=["tag", "name", "constraint"]).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.expectations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Users dataset
# MAGIC
# MAGIC The ./dataset/users.json dataset contains:
# MAGIC
# MAGIC * 4 "standard users"
# MAGIC * 1 user with Null ID
# MAGIC * 1 user with an ID as a string

# COMMAND ----------

# MAGIC %fs head /Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/users_json/users.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Spend dataset
# MAGIC
# MAGIC The ./dataset/spend.csv dataset contains:
# MAGIC
# MAGIC * 3 "standard spends"
# MAGIC * 1 spend with Null age
# MAGIC * 1 spend with null ID
# MAGIC * 1 spend with incompatible schema (ID as string)

# COMMAND ----------

# MAGIC %fs head /Volumes/main__build/dbdemos_sdp_unit_test/raw_data/test/spend_csv/spend.csv

# COMMAND ----------

# MAGIC %md
# MAGIC That's it, our dataset is ready!
