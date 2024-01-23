-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sharing data between organization using Databricks
-- MAGIC 
-- MAGIC With Databricks Unity Catalog and Delta Sharing, sharing data within organization is much easier.
-- MAGIC 
-- MAGIC We often reference this as Sharing Data from Databricks to Databricks (D2D).
-- MAGIC 
-- MAGIC All you need to do is to provide your metastore id to the organization sharing the data, and they'll be able to grant you access directly, without having to worry about credential file & security.
-- MAGIC 
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_sharing%2Fnotebook_d2d%2Faccess&dt=FEATURE_DELTA_SHARING">

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Step 1: Receiver needs to share its metastore ID
-- MAGIC 
-- MAGIC To get access to your provider data, you need to send him your metastore ID. This can be retrived very easily.
-- MAGIC 
-- MAGIC As a **Receiver**, send your metastore ID to the provider

-- COMMAND ----------

SELECT current_metastore();

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Step 2: Providers creates the recipient using the receiver metastore id
-- MAGIC 
-- MAGIC The data provider can now easily create a recipient using this metastore id:
-- MAGIC 
-- MAGIC As a **Receiver**, send your metastore ID to the provider

-- COMMAND ----------

-- DBTITLE 1,Full steps to create a share & recipient using the metastore id
-- Start with the share creation
CREATE SHARE IF NOT EXISTS my_share COMMENT 'My share containing data for other organization';
-- For the demo we'll grant ownership to all users. Typical deployments wouls have admin groups or similar.
ALTER SHARE my_share OWNER TO `account users`;
-- Add our tables (as many as you want, see previous notebook for more details)
-- Note that we turn on Change Data Feed on the table and share
ALTER TABLE dbdemos_sharing_airlinedata.lookupcodes SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER SHARE my_share ADD TABLE dbdemos_sharing_airlinedata.lookupcodes WITH CHANGE DATA FEED;

-- Create the recipient using the metastore id shared by the receiver (see previous cell)
CREATE RECIPIENT IF NOT EXISTS databricks_to_databricks_demo USING ID 'aws:us-west-2:<the_reciever_recipient>' COMMENT 'Recipient for my external customer using Databricks';
-- Grant select access to the share
GRANT SELECT ON SHARE my_share TO RECIPIENT databricks_to_databricks_demo;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Step 3: accept and mount the share as a receiver
-- MAGIC 
-- MAGIC As a receiver, we can now see the data listed as provider. It'll appear as `PROVIDER`.

-- COMMAND ----------

-- DBTITLE 1,List all your providers. The one created previously (databricks_to_databricks) will appear
SHOW PROVIDERS;

-- COMMAND ----------

-- DBTITLE 1,List your provider details
DESC PROVIDER `databricks_to_databricks_demo`

-- COMMAND ----------

-- DBTITLE 1,List the shares details from this provider (what is being shared with me)
SHOW SHARES IN PROVIDER `databricks_to_databricks_demo`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To make the data available to all your organization, all you now need to do as Metastore Admin is to add a new catalog using this share.
-- MAGIC 
-- MAGIC You'll then be able to GRANT permission as you'd do with any other table, and start querying the data directly:

-- COMMAND ----------

-- DBTITLE 1,Create a catalog from the providers share
CREATE CATALOG IF NOT EXISTS USING SHARE `databricks_to_databricks_demo`.my_share;

-- COMMAND ----------

-- DBTITLE 1,Thats it! Our data is now ready to be used:
SELECT * FROM  `databricks_to_databricks_demo`.my_share.lookupcodes

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Subscribing to Change Data Feed
-- MAGIC If your data is being updated or deleted, you'll likely want to share the increment so that external organization can access them.
-- MAGIC 
-- MAGIC A typical use-case is GDPR deletion: you want to make sure other organization also capture this information so that they can DELETE the data downstream.
-- MAGIC 
-- MAGIC To do so, you can simply use Delta Lake `table_changes()` capability on top of your share (see [the documentation](https://docs.databricks.com/delta/delta-change-data-feed.html) for more details): 
-- MAGIC 
-- MAGIC Note that as a provider, you need to turn on CDF at the table level before:
-- MAGIC 
-- MAGIC `ALTER TABLE dbdemos_sharing_airlinedata.lookupcodes SET TBLPROPERTIES (delta.enableChangeDataFeed = true);`<br/>
-- MAGIC `ALTER SHARE my_share ADD TABLE dbdemos_sharing_airlinedata.lookupcodes WITH CHANGE DATA FEED;`

-- COMMAND ----------

SELECT * FROM table_changes('databricks_to_databricks_demo.my_share.lookupcodes', 2, 4)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC # Conclusion
-- MAGIC To recap, Delta Sharing is a cloud and platform agnostic solution to share your data with external consumer. 
-- MAGIC 
-- MAGIC It's simple (pure SQL), open (can be used on any system) and scalable.
-- MAGIC 
-- MAGIC All recipients can access your data, using Databricks or any other system on any Cloud.
-- MAGIC 
-- MAGIC Delta Sharing enable critical use cases around Data Sharing and Data Marketplace. 
-- MAGIC 
-- MAGIC When combined with Databricks Unity catalog, it's the perfect too to accelerate your Datamesh deployment and improve your data governance.
-- MAGIC 
-- MAGIC [Back to Overview]($./01-Delta-Sharing-presentation)
