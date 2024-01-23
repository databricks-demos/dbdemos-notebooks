-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Requesting our Unity Catalog Audit log tables
-- MAGIC Now that our data is saved under our audit log tables, we can easily request them.
-- MAGIC 
-- MAGIC Let's seee a common set of query pattern for log analysis, answering most comomng governance questions.
-- MAGIC 
-- MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Faudit_log%2Fanalysis&dt=FEATURE_UC_AUDIT">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC 
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_assigned.png?raw=true" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC 
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC 
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

USE CATALOG dbdemos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What types of Actions are captured by the Audit Logs?

-- COMMAND ----------

SELECT
  distinct actionName
FROM
  audit_log.unitycatalog
ORDER BY
  actionName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What are the most popular Actions?

-- COMMAND ----------

SELECT
  actionName,
  count(actionName) as actionCount
FROM
  audit_log.unitycatalog
GROUP BY
  actionName
ORDER BY actionCount DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tracking UC Table Query Requests

-- COMMAND ----------

SELECT
  date_time,
  sha1(email),
  actionName,
  requestParams.operation as operation,
  requestParams.is_permissions_enforcing_client as pe_client,
  sha1(requestParams.table_full_name) as table_name,
  response.errorMessage as error
FROM
  audit_log.unitycatalog
WHERE
  actionName in ("generateTemporaryTableCredential")
ORDER BY
  date_time desc


-- COMMAND ----------

SELECT
  sha1(email),
  timestamp,
  requestParams.operation as operation,  
  requestParams.table_full_name as table_name,
  count(actionName) as queries
FROM
  audit_log.unitycatalog
where
  actionName in ("generateTemporaryTableCredential") and requestParams.table_full_name like "demos%"
group by
  1,
  2,
  3,
  4
order by
  2 desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Find out who Created, Updated and Deleted Delta Shares

-- COMMAND ----------

SELECT
  sha1(email),
  date_time,
  actionName,
  sha1(requestParams.name),
  sha1(requestParams.updates),
  requestParams.changes,
  response.result
FROM
  audit_log.unitycatalog
WHERE
  actionName LIKE "%Share" 
  OR actionName = "getActivationUrlInfo"
  OR actionName = "updateSharePermissions"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Which Users are Most Active Overall?

-- COMMAND ----------

SELECT
  sha1(email),
  count(actionName) AS numActions
FROM
  audit_log.unitycatalog
group by
  email
order by
  numActions desc

-- COMMAND ----------

-- MAGIC  %md 
-- MAGIC ### Which Users are the Most Active Sharers?

-- COMMAND ----------

SELECT
  sha1(email),
  count(actionName) AS numActions
FROM
  audit_log.unitycatalog
WHERE
  actionName like '%Share'
group by
  email
order by
  numActions desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tracking Usage of Delta Sharing (external shares) vs Unity Catalog (internal shares)?

-- COMMAND ----------

SELECT
  actionName,
  count(actionName) as numActions
FROM
  audit_log.unitycatalog
where
  actionName Like "%deltaSharing%"
group by
  actionName
order by
  numActions desc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tracking Usage of Delta Sharing (external shares) vs Unity Catalog (internal shares)?

-- COMMAND ----------

SELECT
  actionName,
  count(actionName)
FROM
  audit_log.unitycatalog
where
  actionName not Like "%deltaSharing%"
group by
  actionName
order by
  count(actionName) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tracking Unity Catalog & Delta Sharing Activity by Date 

-- COMMAND ----------

SELECT
  count(actionName),
  to_date(date_time) as date
from
  audit_log.unitycatalog
group by
  to_date(date_time)
order by
  date
