-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Databricks Unity Catalog - Table ACL
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/uc-base-0.png?raw=true" style="float: right" width="500px"/> 
-- MAGIC
-- MAGIC The main feature of Unity Catalog is to provide you an easy way to setup Table ACL (Access Control Level), but also build Dynamic Views based on each individual permission.
-- MAGIC
-- MAGIC Typically, Analysts will only have access to customers from their country and won't be able to read GDPR/Sensitive informations (like email, firstname etc.)
-- MAGIC
-- MAGIC A typical workflow in the Lakehouse architecture is the following:
-- MAGIC
-- MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
-- MAGIC * Data Scientists can read the final tables and update their features tables
-- MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
-- MAGIC * Data is masked/anonymized dynamically based on each user access level
-- MAGIC
-- MAGIC With Unity Catalog, your tables, users and groups are defined at the account level, cross workspaces. Ideal to deploy and operate a Lakehouse Platform across all your teams.
-- MAGIC
-- MAGIC Let's see how this can be done with the Unity Catalog
-- MAGIC
-- MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Ftable_acl%2Facl&dt=FEATURE_UC_TABLE_ACL">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_assigned.png?raw=true" style="float: right" width="500px"/>
-- MAGIC
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC
-- MAGIC Under "Access mode", select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "dbdemos", "Catalog")

-- COMMAND ----------

-- DBTITLE 1,Initialize the demo dataset
-- MAGIC %run ./_resources/00-setup $catalog=$catalog

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Creating the CATALOG
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/uc-base-1.png?raw=true" style="float: right" width="800px"/> 
-- MAGIC
-- MAGIC The first step is to create a new catalog.
-- MAGIC
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC
-- MAGIC To access one table, you can specify the full path: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`
-- MAGIC
-- MAGIC Note that the tables created before Unity Catalog are saved under the catalog named `hive_metastore`. Unity Catalog features are not available for this catalog.
-- MAGIC
-- MAGIC Note that Unity Catalog comes in addition to your existing data, not hard change required!

-- COMMAND ----------

-- The demo will create and use the catalog defined:
CREATE CATALOG IF NOT EXISTS ${catalog};
-- Make it default for future usage (we won't have to specify it)
USE CATALOG ${catalog};

-- COMMAND ----------

-- the catalog has been created for your user and is defined as default. All shares will be created inside.
-- make sure you run the 00-setup cell above to init the catalog to your user. 
SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating the SCHEMA
-- MAGIC Next, we need to create the SCHEMA (or DATABASE).
-- MAGIC
-- MAGIC Unity catalog provide the standard GRANT SQL syntax. We'll use it to GRANT CREATE and USAGE on our SCHEMA to all the users for this demo.
-- MAGIC
-- MAGIC They'll be able to create extra table into this schema.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS uc_acl;
USE uc_acl;

-- COMMAND ----------

-- DBTITLE 1,Let's make sure that all users can use the uc_acl schema for our demo:
GRANT CREATE, USAGE ON CATALOG `${catalog}` TO `account users`;
GRANT CREATE, USAGE ON SCHEMA uc_acl TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating our table
-- MAGIC
-- MAGIC We're all set! We can use standard SQL to create our tables.
-- MAGIC
-- MAGIC We'll use a customers dataset, loading data about users (id, email etc...)
-- MAGIC
-- MAGIC Because we want our demo to be available for all, we'll grant full privilege to the table to all USERS.
-- MAGIC
-- MAGIC Note that the table owner is the current user. Owners have full permissions.<br/>
-- MAGIC If you want to change the owner you can set it as following: ```ALTER TABLE <catalog>.uc_acl.customers OWNER TO `account users`;```

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS uc_acl.customers (
  id STRING,
  creation_date STRING,
  firstname STRING,
  lastname STRING,
  country STRING,
  email STRING,
  address STRING,
  gender DOUBLE,
  age_group DOUBLE); ; 
ALTER TABLE uc_acl.customers OWNER TO `account users`; -- for the demo only, allow all users to edit the table - don't do that in production!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Our customer data was filled for us!
-- MAGIC
-- MAGIC The initialization cell already filled the table for us with fake data for the demo, let's review it's content.

-- COMMAND ----------

SELECT * FROM  uc_acl.customers

-- COMMAND ----------

-- MAGIC %md ## Granting users or group access
-- MAGIC
-- MAGIC Let's now use Unity Catalog to GRANT permission on the table.
-- MAGIC
-- MAGIC Unity catalog let you GRANT standard SQL permission to your objects, using the Unity Catalog users or group:
-- MAGIC
-- MAGIC ### Creating groups
-- MAGIC
-- MAGIC Databricks groups can be created at the account level using the Account Admin UI, or the SCIM API. Here, we created the `dataengineers` group for this demo.
-- MAGIC
-- MAGIC *Note on workspace-level groups: you can also create groups at a workspace level, however, we recommend managing permissions with UC at an account level.*

-- COMMAND ----------

-- Let's grant all users a SELECT
GRANT SELECT ON TABLE uc_acl.customers TO `account users`;

-- We'll grant an extra MODIFY to our Data Engineer
-- Note: make sure you created the dataengineers group first as an account admin!
GRANT SELECT, MODIFY ON TABLE uc_acl.customers TO `dataengineers`;

-- COMMAND ----------

SHOW GRANTS ON TABLE uc_acl.customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC Unity Catalog gives you Table ACL permissions, leveraging users, group and table across multiple workspaces.
-- MAGIC
-- MAGIC But UC not only gives you control over Tables. You can do more advanced permission and data access pattern such as dynamic masking at the row level.
-- MAGIC
-- MAGIC ### Next: Fine Grain Access control
-- MAGIC
-- MAGIC Databricks Unity Catalog provides built-in capabilities to add dynamic masking on columns or rows.
-- MAGIC
-- MAGIC Let's see how this can be done in the [01-Row-Column-access-control notebook ]($./01-Row-Column-access-control).
