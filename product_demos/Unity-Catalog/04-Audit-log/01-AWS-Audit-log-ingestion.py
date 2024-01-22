# Databricks notebook source
# MAGIC %md
# MAGIC # Audit log for governance and Traceability and Compliance
# MAGIC
# MAGIC Unity catalog collect logs on all your users actions. It's easy to get this data and monitor your data usage, including for your own compliance requirements.
# MAGIC
# MAGIC Here are a couple of examples:
# MAGIC
# MAGIC * How your data is used internally, what are the most critical tables
# MAGIC * Who created, updated or deleted Delta Shares
# MAGIC * Who are your most active users
# MAGIC * Who accessed which tables
# MAGIC * Audit all kind of data access pattern...
# MAGIC
# MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Faudit_log%2Fingestion&dt=FEATURE_UC_AUDIT">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cluster setup for UC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_assigned.png?raw=true" style="float: right"/>
# MAGIC
# MAGIC
# MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
# MAGIC
# MAGIC Go in the compute page, create a new cluster.
# MAGIC
# MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up the audit log
# MAGIC
# MAGIC The first thing to do is enabling the audit log. If you haven't done it, you can check the [setup notebook]($./00-auditlog-activation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing raw data
# MAGIC
# MAGIC **Important:** Audit log delivery must be enabled. See [00-auditlog-activation]($./00-auditlog-activation) for an example.
# MAGIC
# MAGIC In our case, the audit log has been enabled and is being delivered under this cloud storage: `s3a://databricks-field-eng-audit-logs/raw-audit-logs/`
# MAGIC
# MAGIC As you can see, our logs are delivered per workspace. Workspace 0 contains the global audit log, that's where we'll find the Unity Catalog logs to track all the data access. 

# COMMAND ----------

#Replace with your own bucket
display(dbutils.fs.ls("s3a://databricks-field-eng-audit-logs/raw-audit-logs/"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Let's setup a small medaillon pipeline to ingest our Audit log data
# MAGIC
# MAGIC Let's build a small ingestion pipeline to extract these logs and save them as Delta Table to be able to do simpler SQL queries.
# MAGIC
# MAGIC We can then schedule this notebook to run every hour. 
# MAGIC
# MAGIC Note that we could also use a small DLT pipeline to do that, this would prevent us from having to deal with checkpoints and autoloader restart.

# COMMAND ----------

# DBTITLE 1,Note: the demos requires the CREATE CATALOG permission. Ask a metastore admin to run this cell if needed.
# MAGIC %run ./_resources/00-setup $catalog=dbdemos

# COMMAND ----------

# MAGIC %python
# MAGIC print(f"Using catalog {catalog}")
# MAGIC checkpoint_location = "/dbdemos/uc/audit_log"
# MAGIC spark.sql(f'USE CATALOG {catalog}');
# MAGIC spark.sql('CREATE DATABASE IF NOT EXISTS audit_log');

# COMMAND ----------

# DBTITLE 1,Ingest the row JSON from the audit log service as our bronze table
from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json, map_filter

def ingest_audit_log():
  print("starting audit log ingestion...")
  streamDF = (spark.readStream.format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.inferColumnTypes", True)
                    .option("cloudFiles.useIncrementalListing", True)
                    .option("cloudFiles.schemaHints", "requestParams map<string,string>")
                    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
                    .option("cloudFiles.schemaLocation", checkpoint_location+'/bronze/audit_log_schema')
                    .load('s3a://databricks-field-eng-audit-logs/raw-audit-logs'))
  (streamDF.writeStream
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location+'/bronze/checkpoint') 
            .option("mergeSchema", True)
            .partitionBy("workspaceId")
            .trigger(availableNow=True)
            .toTable('audit_log.bronze').awaitTermination())

def ingest_and_restart_on_new_col():
  try: 
    ingest_audit_log()
  except BaseException as e:
    #Adding a new column will trigger an UnknownFieldException. In this case we just restart the stream:
    stack = str(e.stackTrace)
    if 'UnknownFieldException' in stack:
      print(f"new colunm, restarting ingestion: {stack}")
      ingest_and_restart_on_new_col()
    else:
      raise e
#ingest_and_restart_on_new_col()


# COMMAND ----------

# MAGIC %md ## What's tracked by the audit logs?
# MAGIC Audit log is enabled on all the workspaces. 
# MAGIC
# MAGIC Note the workspace with ID 0: that's where our UC audit log information will land, as the Unity Catalog is cross-workspaces:

# COMMAND ----------

# MAGIC %sql select distinct(workspaceId) from audit_log.bronze order by workspaceId

# COMMAND ----------

# DBTITLE 1,Audit log is enabled for all services, including "unityCatalog" which is what we're interested in
# MAGIC %sql select distinct(serviceName) from audit_log.bronze order by serviceName desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's extract all the data from our audit log
# MAGIC To make our analysis simpler, we'll create 1 table per audit log service. 
# MAGIC
# MAGIC We'll then be able to query the separate "unityCatalog" table to run our analysis.

# COMMAND ----------

def get_table_name(service_name):
  return service_name.replace("-","_").lower()

def flatten_table(service_name):
  table_name = get_table_name(service_name)
  flattenedStream = spark.readStream.table("audit_log.bronze")
  print(f"processing service {service_name} (saved as table {table_name})")
  
  (flattenedStream
     .filter(col("serviceName") == service_name)
     .withColumn("requestParams", map_filter("requestParams", lambda k, v: v.isNotNull())) #cleanup null field
     .withColumn("email", col("userIdentity.email"))
     .drop("userIdentity")
     .withColumn("date_time", from_utc_timestamp(from_unixtime(col("timestamp")/1000), "UTC"))
   .writeStream
     .outputMode("append")
     .partitionBy("workspaceId")
     .trigger(availableNow=True)
     .option("mergeSchema", True)
     .option("checkpointLocation", f"{checkpoint_location}/gold/{service_name}/checkpoint")
     .table(f"audit_log.{table_name}").awaitTermination())


# COMMAND ----------

service_name_list = [i['serviceName'] for i in spark.table('audit_log.bronze').select("serviceName").distinct().collect() if i['serviceName'] is not None]
print(f"Service list: {service_name_list}")
#For this demo, let's just ingest the unityCatalog table:
service_name_list = ["unityCatalog"]
with ThreadPoolExecutor(max_workers=3) as executor:
    collections.deque(executor.map(flatten_table, service_name_list))

# COMMAND ----------

# MAGIC %md ## That's it, our UC audit log table is ready for query!

# COMMAND ----------

# MAGIC %sql SELECT * except(accountId, email, sourceIPAddress, requestParams, sessionId, session_id, endpoint_id, signed_uri, user_id) 
# MAGIC         FROM audit_log.unitycatalog limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC We're now ready to run more advanced analysis. Let's open our [next Analysis Notebook]($./02-log-analysis-query)

# COMMAND ----------

# DBTITLE 1,Admin maintenance
#let's make sure everybody can access the tables created in the demo, and enable auto compaction for future run as this is a real pipeline
def maintain_table(service_name):
  table_name = get_table_name(service_name)
  print(f"Maintainance for table {table_name}")
  spark.sql(f"GRANT ALL PRIVILEGES ON TABLE audit_log.{table_name} TO `account users`")
  spark.sql(f"ALTER TABLE audit_log.{table_name} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)")
  spark.sql(f"VACUUM audit_log.{table_name} retain 200 hours")
  
with ThreadPoolExecutor(max_workers=5) as executor:
    collections.deque(executor.map(maintain_table, service_name_list + ['bronze']))
