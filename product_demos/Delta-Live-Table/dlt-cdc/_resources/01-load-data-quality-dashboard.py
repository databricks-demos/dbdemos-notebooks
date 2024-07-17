# Databricks notebook source
# MAGIC %md # Data initialization for DLT expectation dashboard
# MAGIC
# MAGIC Run only once to reset the demo dashboard
# MAGIC
# MAGIC Let's load a couple of expectations to be able to build our dbsql dashboard.

# COMMAND ----------

# MAGIC %run ./00-Data_CDC_Generator

# COMMAND ----------

headers = """id,dataset,name,passed_records,failed_records,status_update,dropped_records,output_records,timestamp"""
data = """ef810106-6251-4dae-b282-a99d75b34017,user_silver_dlt,valid_id,40000,400,COMPLETED,0,1000000,2021-10-02T14:05:00.000+0000
5385937c-6c1e-4079-8002-bac0e1b9bc1a,user_gold_dlt,valid_age,40000,600,COMPLETED,600,100000,2021-10-02T14:05:00.000+0000
5385937c-6c1e-4079-8002-bac0e1b9bc1a,user_gold_dlt,valid_score,30000,600,COMPLETED,600,100000,2021-10-02T14:05:00.000+0000
5385937c-6c1e-4079-8002-bac0e1b9bc1a,user_gold_dlt,valid_income,30000,600,COMPLETED,600,100000,2021-10-02T14:05:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,60000,124,COMPLETED,0,1000000,2021-10-03T14:06:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,60000,0,COMPLETED,0,1000000,2021-10-03T14:06:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,50000,400,COMPLETED,0,1000000,2021-10-03T14:06:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,50000,600,COMPLETED,600,100000,2021-10-03T14:06:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,2000,26000,COMPLETED,26000,100000,2021-10-10T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,2000,26000,COMPLETED,26000,100000,2021-10-10T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,2000,26000,COMPLETED,26000,100000,2021-10-10T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,1000,5000,COMPLETED,0,1000000,2021-10-11T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,3000,20000,COMPLETED,0,1000000,2021-10-11T14:07:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,2000,44000,COMPLETED,0,1000000,2021-10-11T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,2000,36000,COMPLETED,36000,100000,2021-10-11T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,2000,36000,COMPLETED,36000,100000,2021-10-11T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,2000,36000,COMPLETED,36000,100000,2021-10-11T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,30000,2000,COMPLETED,0,1000000,2021-10-09T14:09:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,20000,2400,COMPLETED,0,1000000,2021-10-09T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,20000,2600,COMPLETED,2600,100000,2021-10-09T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,20000,2600,COMPLETED,2600,100000,2021-10-09T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,20000,2600,COMPLETED,2600,100000,2021-10-09T14:09:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,1000,5000,COMPLETED,0,1000000,2021-10-10T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,3000,20000,COMPLETED,0,1000000,2021-10-10T14:07:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,2000,24000,COMPLETED,0,1000000,2021-10-10T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,50000,246,COMPLETED,0,1000000,2021-10-05T14:05:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,50000,0,COMPLETED,0,1000000,2021-10-05T14:05:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,40000,500,COMPLETED,0,1000000,2021-10-05T14:05:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,40000,700,COMPLETED,700,100000,2021-10-05T14:05:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,30000,700,COMPLETED,700,100000,2021-10-05T14:05:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,30000,700,COMPLETED,700,100000,2021-10-05T14:05:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,60000,500,COMPLETED,0,1000000,2021-10-06T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,60000,0,COMPLETED,0,1000000,2021-10-06T14:07:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,50000,400,COMPLETED,0,1000000,2021-10-06T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,60000,1600,COMPLETED,1600,100000,2021-10-07T14:02:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,70000,500,COMPLETED,0,1000000,2021-10-08T14:09:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,70000,1000,COMPLETED,0,1000000,2021-10-08T14:09:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,60000,1400,COMPLETED,0,1000000,2021-10-08T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,60000,1600,COMPLETED,1600,100000,2021-10-08T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,60000,1600,COMPLETED,1600,100000,2021-10-08T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,60000,1600,COMPLETED,1600,100000,2021-10-08T14:09:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,30000,500,COMPLETED,0,1000000,2021-10-09T14:09:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,50000,600,COMPLETED,600,100000,2021-10-06T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,40000,600,COMPLETED,600,100000,2021-10-06T14:07:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,40000,600,COMPLETED,600,100000,2021-10-06T14:07:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,70000,500,COMPLETED,0,1000000,2021-10-07T14:02:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,70000,1000,COMPLETED,0,1000000,2021-10-07T14:02:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,60000,1400,COMPLETED,0,1000000,2021-10-07T14:02:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,60000,1600,COMPLETED,1600,100000,2021-10-07T14:02:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,60000,1600,COMPLETED,1600,100000,2021-10-07T14:02:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,40000,600,COMPLETED,600,100000,2021-10-03T14:06:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,40000,600,COMPLETED,600,100000,2021-10-03T14:06:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,spend_silver_dlt,valid_id,50000,120,COMPLETED,0,1000000,2021-10-04T14:08:00.000+0000
c054f1c7-3765-49d6-aa76-debd6e76691c,users_bronze_dlt,correct_schema,50000,0,COMPLETED,0,1000000,2021-10-04T14:08:00.000+0000
d5d76478-ff24-4bca-aede-c69f31b5b35e,user_silver_dlt,valid_id,40000,200,COMPLETED,0,1000000,2021-10-04T14:08:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_age,40000,300,COMPLETED,300,100000,2021-10-04T14:08:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_score,30000,300,COMPLETED,300,100000,2021-10-04T14:08:00.000+0000
4b07c459-f414-492a-9f80-640a741c12c6,user_gold_dlt,valid_income,30000,300,COMPLETED,300,100000,2021-10-04T14:08:00.000+0000
0ddbf700-31af-11ec-93be-00163e375cb1,spend_silver_dlt,valid_id,100000,112,COMPLETED,0,1000000,2021-10-01T14:05:00.000+0000
0f0d7220-31af-11ec-93be-00163e375cb1,users_bronze_dlt,correct_schema,100000,0,COMPLETED,0,1000000,2021-10-01T14:05:00.000+0000
149500f0-31af-11ec-93be-00163e375cb1,user_silver_dlt,valid_id,999999,335,COMPLETED,0,1000000,2021-10-01T14:05:00.000+0000
1b43d1b0-31af-11ec-93be-00163e375cb1,user_gold_dlt,valid_age,100000,1005,COMPLETED,1005,100000,2021-10-01T14:05:00.000+0000
1b43d1b0-31af-11ec-93be-00163e375cb1,user_gold_dlt,valid_score,100000,1005,COMPLETED,1005,100000,2021-10-01T14:05:00.000+0000
1b43d1b0-31af-11ec-93be-00163e375cb1,user_gold_dlt,valid_income,100000,1005,COMPLETED,1005,100000,2021-10-01T14:05:00.000+0000
d1ed9c4c-0fda-4ccc-b57f-aef51fc0a73f  ,spend_silver_dlt,valid_id,50000,124,COMPLETED,0,1000000,2021-10-02T14:05:00.000+0000
d1ed9c4c-0fda-4ccc-b57f-aef51fc0a73f  ,users_bronze_dlt,correct_schema,50000,0,COMPLETED,0,1000000,2021-10-02T14:05:00.000+0000"""

# COMMAND ----------

import pyspark.sql.functions as F
expectations = [d.split(",") for d in data.split("\n")]
spark.createDataFrame(expectations, headers.split(",")) \
  .withColumn("passed_records", F.col("passed_records").cast("int")) \
  .withColumn("output_records", F.col("output_records").cast("int")) \
  .withColumn("timestamp", F.to_timestamp("timestamp")) \
  .withColumn("dropped_records", F.col("dropped_records").cast("int")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dlt_expectations")

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table dlt_expectations set tblproperties (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
