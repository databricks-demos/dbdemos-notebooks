-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Audit Logs with Databricks Unity Catalog System Tables
-- MAGIC
-- MAGIC Databricks tracks all operations across our Lakehouse. This is key for governance and being able to analyze your Lakehouse usage.
-- MAGIC
-- MAGIC This is made possible with the `system.access.audit` table. 
-- MAGIC
-- MAGIC This table contains all informations to answer questions such as:
-- MAGIC
-- MAGIC - All operation executed in your account and workspaces
-- MAGIC - Which operations are executed against your Unity Catalog objects
-- MAGIC - Who is accessing what
-- MAGIC - Who and When a table was deleted
-- MAGIC - Monitor critical table access
-- MAGIC - ...
-- MAGIC
-- MAGIC ## Audit Logs table
-- MAGIC
-- MAGIC The `audit_logs` table has the following columns: 
-- MAGIC - version
-- MAGIC - event_time
-- MAGIC - event_date
-- MAGIC - event_id
-- MAGIC - workspace_id
-- MAGIC - source_ip_address
-- MAGIC - user_agent
-- MAGIC - session_id
-- MAGIC - user_identity
-- MAGIC - service_name 
-- MAGIC - action_name
-- MAGIC - request_id
-- MAGIC - request_params
-- MAGIC - response
-- MAGIC - audit_level
-- MAGIC - account_id
-- MAGIC
-- MAGIC Audit logs give you the ability to see what users are doing within Databricks. They allow you to see the actions users have taken, such as editing or triggering a job and when it occured. 
-- MAGIC
-- MAGIC ## Query example 
-- MAGIC
-- MAGIC The following queries are some example of audit analysis that you can perform.
-- MAGIC
-- MAGIC Make sure you have read access to the system catalog to be able to run the following queries (by default available to admin metastore).
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=02-audit-log&demo_name=04-system-tables&event=VIEW">

-- COMMAND ----------

-- DBTITLE 1,Review audit log table
select * from system.access.audit

-- COMMAND ----------

-- DBTITLE 1,Review all services being tracked as audit log
select distinct(service_name) from system.access.audit

-- COMMAND ----------

-- DBTITLE 1,List Unity Catalog actions
select
  count(*) as action_count,
  action_name
from
  system.access.audit
where
  service_name = 'unityCatalog'
group by
  action_name
order by
  action_count desc;

-- COMMAND ----------

-- DBTITLE 1,List table access
SELECT
  *
FROM
  system.access.audit
WHERE
  service_name = 'unityCatalog'
  AND action_name = "getTable"
LIMIT
  100;

-- COMMAND ----------

-- DBTITLE 1,User permissions being changed(assigned/removed/modified) on the data objects(schemas/tables) across a workspace or group of workspaces
SELECT
  event_time,
  action_name,
  user_identity.email as requester,
  request_params,
  *
FROM
  system.access.audit
WHERE
  action_name IN ('updatePermissions', 'updateSharePermissions')
  AND audit_level = 'ACCOUNT_LEVEL'
  AND service_name = 'unityCatalog'
ORDER BY
  action_name,
  event_time
limit
  1000;

-- COMMAND ----------

-- DBTITLE 1,Any add/update/delete actions to Catalog (find details on item creation/deletion)
SELECT
  event_time,
  action_name,
  user_identity.email as requester,
  request_params,
  *
FROM
  system.access.audit
WHERE
  action_name IN (
    'createCatalog',
    'deleteCatalog',
    'updateCatalog'
  )
  AND audit_level = 'ACCOUNT_LEVEL'
  AND service_name = 'unityCatalog'
ORDER BY
  action_name,
  event_time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Locations 
-- MAGIC External locations are a common way to load data from cloud object storage into Databricks tables. They can also be used for writing data out of Databricks for external processes. We want to ensure that data is being used properly and is not exfiltrating outside our doman. Let's see how many external locations are being created over time and which ones are the most used. 

-- COMMAND ----------

-- created external locations
-- monitor possible data exfilltration
select
  event_time,
  event_date,
  date_format(event_date, 'yyyy-MM') AS year_month,
  user_identity.email as user_email,
  service_name,
  action_name,
  request_id,
  request_params.name as external_location_name,
  request_params.skip_validation,
  request_params.credential_name,
  request_params.url,
  request_params.workspace_id,
  request_params.metastore_id,
  response.status_code
from
  system.access.audit
where
  action_name = 'createExternalLocation'

-- COMMAND ----------

-- most used external locations
select
  event_time,
  event_date,
  date_format(event_date, 'yyyy-MM') AS year_month,
  user_identity.email as user_email,
  service_name,
  action_name,
  request_id,
  request_params,
  request_params.name_arg as external_location_name,
  request_params.workspace_id,
  request_params.metastore_id,
  response.status_code
from
  system.access.audit
where
  action_name = 'getExternalLocation'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users 
-- MAGIC
-- MAGIC An important KPI for any platform team administering Databricks is the number of users that they are supporting. This allows admins to quantify the impact is developer productivity and importance to the organization.  

-- COMMAND ----------

-- DBTITLE 1,Daily and Monthly Active Users
select
  distinct event_date as `Date`,
  date_format(event_date, 'yyyy-MM') AS year_month,
  workspace_id,
  user_identity.email as user_email
from
  system.access.audit

-- COMMAND ----------

-- DBTITLE 1, User Groups being deleted from UC console and at individual workspace level – who deleted(userid/SP), when & where
SELECT
  event_time,
  action_name,
  user_identity.email as requester,
  request_params.targetGroupId as deleted_group_id,
  CASE
    WHEN audit_level = 'ACCOUNT_LEVEL' THEN 'deleted from account console'
    WHEN audit_level = 'WORKSPACE_LEVEL' THEN 'deleted from workspace'
  END AS scenario,
  *
FROM
  system.access.audit
WHERE
  action_name = 'removeGroup' --AND request_params.targetGroupId IS NOT NULL
  AND audit_level IN ('ACCOUNT_LEVEL', 'WORKSPACE_LEVEL')
  AND service_name = 'accounts'
ORDER BY
  audit_level,
  event_time;

-- COMMAND ----------

-- DBTITLE 1,List users added as metastore admin
-- Metastore admin assigned for UC - when and who(userid) changed
SELECT
  event_time,
  action_name,
  user_identity.email AS requester,
  request_params.`owner` AS new_metastore_owner_admin,
  *
FROM
  system.access.audit
WHERE
  action_name = 'updateMetastore'
  AND request_params.`owner` IS NOT NULL
  AND audit_level = 'ACCOUNT_LEVEL'
  AND service_name = 'unityCatalog'
ORDER BY
  event_time
LIMIT
  100;
