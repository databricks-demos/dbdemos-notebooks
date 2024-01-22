# Databricks notebook source
# MAGIC %md 
# MAGIC # Test Datasets setup
# MAGIC 
# MAGIC We have 2 files that we'll be using as dataset saved in git within the project (you can open them directly under the `dataset` folder). 
# MAGIC 
# MAGIC All we have to do is move these local files to our blob storage so that we can read them within our DLT test pipeline.
# MAGIC 
# MAGIC *Note: We could also have used Faker to generate them dynamicall.*
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_unit_test%2Fnotebook_dataset&dt=DLT_UNIT_TEST">

# COMMAND ----------

# DBTITLE 1,Move our test resources to DBFS for tests
#We could use repo arbitrary files, but we'll prefer working without files to support workpsace (non repo) deployment too.
#dbutils.fs.rm("/demos/retail/customers/users_json", True)
#dbutils.fs.mkdirs("/demos/retail/customers/test/users_json")
#dbutils.fs.mkdirs("/demos/retail/customers/test/spend_csv")

#import shutil
#shutil.copyfile("./dataset/users.json", "/dbfs/demos/retail/customers/test/users_json/users.json")
#shutil.copyfile("./dataset/spend.csv", "/dbfs/demos/retail/customers/test/spend_csv/spend.csv")


spend_csv = """id,age,annual_income,spending_core
3,47,858.9,99.4
1,47,861.9,48.1
2,97,486.4,880.8
4,,283.8,117.8
,95,847.5,840.9
invalid_id,1,514.5,284.5"""

dbutils.fs.put('/demos/retail/customers/test/spend_csv/spend.csv', spend_csv, True)
    
users_json = """{"id":1,"email":"joneschristina@example.org","creation_date":"11-28-2021 12:08:46","last_activity_date":"08-20-2021 08:24:44","firstname":"Randall","lastname":"Espinoza","address":"71571 Jennifer Creek - East John, CO 81653","city":"Port Nicholas","last_ip":"22.207.225.77","postcode":"62389"}
{"id":4,"email":"christybautista@example.net","creation_date":"06-30-2022 22:51:30","last_activity_date":"08-22-2021 17:25:06","firstname":"Jose","lastname":"Bell","address":"865 Young Crest - Lake Adriennebury, VA 67749","city":"Brownstad","last_ip":"159.111.101.250","postcode":"52432"}
{"id":0,"email":"amccormick@example.com","creation_date":"10-21-2021 02:37:38","last_activity_date":"07-22-2021 15:06:48","firstname":"Dylan","lastname":"Barber","address":"7995 Ronald Flat Suite 597 - Williefurt, AL 37894","city":"Port Steven","last_ip":"173.88.213.168","postcode":"58368"}
{"id":3,"email":"jenniferbennett@example.org","creation_date":"07-06-2022 12:27:24","last_activity_date":"01-09-2022 15:04:45","firstname":"Phillip","lastname":"Morgan","address":"523 Garza Crossroad - New Maryview, OK 92301","city":"Julieshire","last_ip":"170.233.120.199","postcode":"34528"}
{"id":2,"email":"alexis25@example.org","creation_date":"09-10-2021 02:31:37","last_activity_date":"01-11-2022 20:39:01","firstname":"Gregory","lastname":"Crane","address":"068 Shawn Port - West Jessica, KS 84864","city":"South Tonya","last_ip":"192.220.63.96","postcode":"88033"}
{"email":"davidporter@example.com","creation_date":"05-28-2022 09:54:50","last_activity_date":"12-18-2021 21:48:48","firstname":"Jeremy","lastname":"Knight","address":"06183 Acevedo Bypass - Petermouth, ME 34177","city":"West Brianburgh","last_ip":"53.240.159.208","postcode":"73380"}
{"id":"invalid ID","email":"margaret84@example.com","creation_date":"12-20-2021 19:57:28","last_activity_date":"07-27-2021 09:39:28","firstname":"Angela","lastname":"Adams","address":"098 Daniel Ferry Suite 565 - South Andrea, ND 36326","city":"New Mariafort","last_ip":"7.176.250.65","postcode":"21300"}"""

dbutils.fs.put('/demos/retail/customers/test/users_json/users.json', users_json, True)

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

# MAGIC %fs head /demos/retail/customers/test/users_json/users.json

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

# MAGIC %fs head /demos/retail/customers/test/spend_csv/spend.csv

# COMMAND ----------

# MAGIC %md
# MAGIC That's it, our dataset is ready!
