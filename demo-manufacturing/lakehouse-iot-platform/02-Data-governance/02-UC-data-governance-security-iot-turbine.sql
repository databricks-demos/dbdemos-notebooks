-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Ensuring Governance and security for our IOT platform
-- MAGIC
-- MAGIC Data governance and security is hard when it comes to a complete Data Platform. SQL GRANT on tables isn't enough and security must be enforced for multiple data assets (dashboards, Models, files etc).
-- MAGIC
-- MAGIC To reduce risks and driving innovation, Emily's team needs to:
-- MAGIC
-- MAGIC - Unify all data assets (Tables, Files, ML models, Features, Dashboards, Queries)
-- MAGIC - Onboard data with multiple teams
-- MAGIC - Share & monetize assets with external Organizations
-- MAGIC
-- MAGIC <style>
-- MAGIC .box{
-- MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
-- MAGIC .badge {
-- MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
-- MAGIC .badge_b { 
-- MAGIC   height: 35px}
-- MAGIC </style>
-- MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
-- MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
-- MAGIC   <div style="width:200px; float: left; text-align: center">
-- MAGIC     <div class="box" style="">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team A</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">
-- MAGIC         <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/alice.png" style="" width="60px"> <br/>
-- MAGIC         Data Analysts<br/>
-- MAGIC         <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/marc.png" style="" width="60px"> <br/>
-- MAGIC         Data Scientists<br/>
-- MAGIC         <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/john.png" style="" width="60px"> <br/>
-- MAGIC         Data Engineers
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team B</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">...</div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on queries, dashboards</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on tables, columns, rows</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on features, ML models, endpoints, notebooks…</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on files, jobs</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   <div class="box" style="width:550px; float: left">
-- MAGIC     <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/emily.png" style="float: left; margin-right: 10px;" width="80px"> 
-- MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />Governance and Security</div>
-- MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
-- MAGIC       <ul style="line-height: 2px;">
-- MAGIC         <li>Central catalog - all data assets</li>
-- MAGIC         <li>Data exploration & discovery to unlock new use-cases</li>
-- MAGIC         <li>Permissions cross-teams</li>
-- MAGIC         <li>Reduce risk with audit logs</li>
-- MAGIC         <li>Measure impact with lineage</li>
-- MAGIC       </ul>
-- MAGIC       + Monetize & Share data with external organization (Delta Sharing)
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   
-- MAGIC </div>
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=02-UC-data-governance-security-iot-turbine&demo_name=lakehouse-patient-readmission&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Implementing a global data governance and security with Unity Catalog
-- MAGIC
-- MAGIC <img style="float: right; margin-top: 30px" width="500px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/team_flow_emily.png" />
-- MAGIC
-- MAGIC Let's see how the Lakehouse can solve this challenge leveraging Unity Catalog.
-- MAGIC
-- MAGIC Our Data has been saved as Delta Table by our Data Engineering team.  The next step is to secure this data while allowing cross team to access it. <br>
-- MAGIC A typical setup would be the following:
-- MAGIC
-- MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
-- MAGIC * Data Scientists can read the final tables and update their features tables
-- MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
-- MAGIC * Data is masked/anonymized dynamically based on each user access level
-- MAGIC
-- MAGIC This is made possible by Unity Catalog. When tables are saved in the Unity Catalog, they can be made accessible to the entire organization, cross-workpsaces and cross users.
-- MAGIC
-- MAGIC Unity Catalog is key for data governance, including creating data products or organazing teams around datamesh. It brings among other:
-- MAGIC
-- MAGIC * Fined grained ACL
-- MAGIC * Audit log
-- MAGIC * Data lineage
-- MAGIC * Data exploration & discovery
-- MAGIC * Sharing data with external organization (Delta Sharing)
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Flakehouse_iot_turbine%2Fuc&dt=LAKEHOUSE_MANUFACTURING_TURBINE">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right; margin-left: 10px"/>
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled & Unity Catalog enabled at the account level (see [documentation](https://docs.databricks.com/data-governance/unity-catalog/get-started.html))
-- MAGIC
-- MAGIC In the compute page, make sure you select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)
-- MAGIC
-- MAGIC If you're using the cluster created by `dbdemos`, you're all good.

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Exploring our Iot Platform database
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC
-- MAGIC Let's review the data created.
-- MAGIC
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC
-- MAGIC All unity catalog is available with SQL (`CREATE CATALOG IF NOT EXISTS my_catalog` ...)
-- MAGIC
-- MAGIC To access one table, you can specify the full path: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`

-- COMMAND ----------

-- the catalog has been created for your user and is defined as default. 
-- make sure you run the 00-setup cell above to init the catalog to your user. 
SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Let's review the tables we created under our schema
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
-- MAGIC
-- MAGIC Unity Catalog provides a comprehensive Data Explorer that you can access on the left menu.
-- MAGIC
-- MAGIC **Open it and navigate under `dbdemos`.`lakehouse_iot` to review the tables created.**
-- MAGIC
-- MAGIC You'll find all the options for your data management and governance: review the tables created, add new one, share them with Delta Sharing...
-- MAGIC
-- MAGIC **You'll also be able to explore data and GRANT permissions to your users directly using the UI**
-- MAGIC
-- MAGIC
-- MAGIC ### Discoverability 
-- MAGIC
-- MAGIC In addition, Unity catalog also provides explorability and discoverability. 
-- MAGIC
-- MAGIC Anyone having access to the tables will be able to search it and analyze its main usage. <br>
-- MAGIC You can use the Search menu (⌘ + P) to navigate in your data assets (tables, notebooks, queries...)

-- COMMAND ----------

-- DBTITLE 1,As you can see, our tables are available under our catalog.
SHOW TABLES;

-- COMMAND ----------

-- DBTITLE 1,Granting access to Analysts & Data Engineers:
-- Let's grant our ANALYSTS a SELECT permission:
-- Note: make sure you created an analysts and dataengineers group first.
GRANT SELECT ON TABLE main_build.dbdemos_iot_platform.turbine TO `analysts`;
GRANT SELECT ON TABLE main_build.dbdemos_iot_platform.sensor_bronze TO `analysts`;
GRANT SELECT ON TABLE main_build.dbdemos_iot_platform.sensor_hourly TO `analysts`;
GRANT SELECT ON TABLE main_build.dbdemos_iot_platform.historical_turbine_status TO `analysts`;

-- We'll grant an extra MODIFY to our Data Engineer
GRANT SELECT, MODIFY ON SCHEMA main_build.dbdemos_iot_platform TO `dataengineers`;

-- COMMAND ----------

-- DBTITLE 1,Review grant
SHOW GRANT ON turbine

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dynamically filtering data base on current user, row and column-level filtering
-- MAGIC
-- MAGIC Let's see how Unity Catalog can be used to filter data and return different results based on who is querying it.
-- MAGIC
-- MAGIC Let's pretend we're based in Chicago, and we want the `parts` table to only return the parts available in the Chicago location as this is where we operate.
-- MAGIC
-- MAGIC We'll add a new table doing a matching between users and the parts locations *(Note: this could also be done with groups)*.
-- MAGIC
-- MAGIC You'll be based in Chicago, John in Honolulu and Lea in Denvers:

-- COMMAND ----------

-- create the table matchying the users and the country/location
CREATE OR REPLACE TABLE parts_users_country_permission (email STRING, country STRING);

INSERT INTO parts_users_country_permission (email, country)
  VALUES 
    (current_user(), 'America/Chicago'),
    ('john@mycompany.com', 'America/Honolulu'),
    ('lea@mycompany.com', 'America/Denver');

-- COMMAND ----------

-- DBTITLE 1,Let's create and try our new protected view
CREATE OR REPLACE VIEW parts_secured AS
SELECT
  CASE 
    WHEN is_account_group_member('iot_admin') THEN EAN  -- allow admin to see all
    ELSE '***' -- filter other users, they won't be able to see the EAN
  END as EAN,
  p.* EXCEPT (EAN)
FROM parts p 
INNER JOIN parts_users_country_permission u -- Get the country/location permission table
  ON p.stock_location = u.country 
  AND (u.email = current_user() OR is_account_group_member('iot_admin')); --Filter based on the current user, admin also have all permission


-- Let's test our secured view. We'll only see the 'America/Chicago' parts, and the EAN will be filtered.
SELECT * FROM parts_secured;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Sharing data with external organization
-- MAGIC
-- MAGIC We've seen how to GRANT access to our tables internally (to any entity within your Databricks account)
-- MAGIC
-- MAGIC However, this might not be enough. You'll have to share this data with external organizations (for data monetization, partners etc). 
-- MAGIC
-- MAGIC The Lakehouse provides this capability while being agnostic about your partners data stack / cloud.
-- MAGIC
-- MAGIC This is powered by [Delta Sharing](https://www.databricks.com/en/product/delta-sharing), an open protocol creating open standard and data normalization across industries.
-- MAGIC
-- MAGIC For a full example on Delta Sharing, run: `dbdemos.intall('delta-sharing-airlines')`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Going further with Data governance & security
-- MAGIC
-- MAGIC By bringing all your data assets together, Unity Catalog let you build a complete and simple governance to help you scale your teams.
-- MAGIC
-- MAGIC Unity Catalog can be leveraged from simple GRANT to building a complete datamesh organization.
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
-- MAGIC
-- MAGIC ### Fine-grained ACL
-- MAGIC
-- MAGIC Need more advanced control? You can chose to dynamically change your table output based on the user permissions: `dbdemos.intall('uc-01-acl')`
-- MAGIC
-- MAGIC ### Secure external location (S3/ADLS/GCS)
-- MAGIC
-- MAGIC Unity Catatalog let you secure your managed table but also your external locations:  `dbdemos.intall('uc-02-external-location')`
-- MAGIC
-- MAGIC ### Lineage 
-- MAGIC
-- MAGIC UC automatically captures table dependencies and let you track how your data is used, including at a row level: `dbdemos.intall('uc-03-data-lineage')`
-- MAGIC
-- MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
-- MAGIC
-- MAGIC
-- MAGIC ### Audit log
-- MAGIC
-- MAGIC UC captures all events. Need to know who is accessing which data? Query your audit log:  `dbdemos.intall('uc-04-audit-log')`
-- MAGIC
-- MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
-- MAGIC
-- MAGIC ### Upgrading to UC
-- MAGIC
-- MAGIC Already using Databricks without UC? Upgrading your tables to benefit from Unity Catalog is simple:  `dbdemos.intall('uc-05-upgrade')`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: Start building analysis with Databricks SQL
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse and secured, let's see how our Data Analyst team can start leveraging them to run BI workloads
-- MAGIC
-- MAGIC Jump to the [BI / Data warehousing notebook]($../03-BI-data-warehousing/03-BI-Datawarehousing-iot-turbine) or [Go back to the introduction]($../00-IOT-wind-turbine-introduction-lakehouse)
