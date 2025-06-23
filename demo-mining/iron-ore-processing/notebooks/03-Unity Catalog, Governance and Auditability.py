# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("schema_name", schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Unity Catalog

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC Unity Catalog provides a unified governance layer in Databricks to centrally manage data access, lineage, and security across all data assets — including tables, files, models, and notebooks — with fine-grained, role-based controls.
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../demo_setup/images/Unity_Catalog.png" width="800px"/> 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1.3. 🔐 Now Lets Secure the Datasets with Unity Catalog
# MAGIC
# MAGIC In complex environments, ensuring robust data governance and security across the entire data platform is critical. Traditional approaches — like using SQL GRANT statements on individual tables — fall short. Governance must extend beyond just tables to include files, models, dashboards, features, and queries.
# MAGIC
# MAGIC To both minimise risk and enable data-driven innovation, the team must:
# MAGIC
# MAGIC - 🔄 Unify all data assets — including tables, files, ML models, features, dashboards, and queries — under a single governance layer
# MAGIC - 👥 Onboard and collaborate across multiple teams, from operations to data science
# MAGIC - 🌐 Securely share selected data and insights with external partners or organisations, while maintaining fine-grained access controls
# MAGIC
# MAGIC Unity Catalog enables this unified governance approach across the platform — critical in regulated and high-value environments.
# MAGIC
# MAGIC <style>
# MAGIC .box{
# MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
# MAGIC   <div style="width:200px; float: left; text-align: center">
# MAGIC     <div class="box" style="">
# MAGIC       <div style="font-size: 26px;">
# MAGIC         <strong>Team A</strong>
# MAGIC       </div>
# MAGIC       <div style="font-size: 13px">
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/da.png" style="" width="60px"> <br/>
# MAGIC         Data Analysts<br/>
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="" width="60px"> <br/>
# MAGIC         Data Scientists<br/>
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="" width="60px"> <br/>
# MAGIC         Data Engineers
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
# MAGIC       <div style="font-size: 26px;">
# MAGIC         <strong>Team B</strong>
# MAGIC       </div>
# MAGIC       <div style="font-size: 13px">...</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on queries, dashboards</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on tables, columns, rows</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on features, ML models, endpoints, notebooks…</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on files, jobs</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC   </div>
# MAGIC   
# MAGIC   <div class="box" style="width:550px; float: left">
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/gov.png" style="float: left; margin-right: 10px;" width="80px"> 
# MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />Governance and Security</div>
# MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
# MAGIC       <ul style="line-height: 2px;">
# MAGIC         <li>Central catalog - all data assets</li>
# MAGIC         <li>Data exploration & discovery to unlock new use-cases</li>
# MAGIC         <li>Permissions cross-teams</li>
# MAGIC         <li>Reduce risk with audit logs</li>
# MAGIC         <li>Measure impact with lineage</li>
# MAGIC       </ul>
# MAGIC       + Share data with external organization (Delta Sharing)
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2F02-Data-governance%2F02-UC-data-governance-security-churn&demo_name=lakehouse-retail-c360&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-retail-c360%2F02-Data-governance%2F02-UC-data-governance-security-churn&version=1">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Exploring our Iron Ore Processing Catalog
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
# MAGIC
# MAGIC 🧭 Exploring Our Iron Ore Processing Catalog
# MAGIC
# MAGIC Now that we’ve processed the data, let’s take a look at how it’s organised in Unity Catalog — the unified governance layer for all data assets in Databricks.
# MAGIC
# MAGIC Unity Catalog follows a three-tiered structure:
# MAGIC
# MAGIC - 🗂️ CATALOG – The top-level container, typically used to separate environments or domains (e.g., iron_ore_demo)
# MAGIC - 📁 SCHEMA (or DATABASE) – A logical grouping of related tables within a catalog (e.g., raw, silver, gold)
# MAGIC - 📊 TABLE – The actual datasets used in queries and pipelines (e.g., flotation_data, lab_results)
# MAGIC
# MAGIC You can manage all of this directly with SQL — for example:
# MAGIC
# MAGIC `CREATE CATALOG IF NOT EXISTS my_catalog ...`
# MAGIC
# MAGIC This structure makes it easy to organise, secure, and discover datasets across the processing lifecycle.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG identifier(:catalog_name);
# MAGIC USE SCHEMA identifier(:schema_name);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Let's review the tables we created under our schema
# MAGIC
# MAGIC Unity Catalog Link: [Catalog Explorer](https://e2-demo-field-eng.cloud.databricks.com/explore/data/mining_iron_ore_processing_demo_catalog?o=1444828305810485&activeTab=overview)
# MAGIC
# MAGIC Unity Catalog provides a comprehensive Data Explorer that you can access on the left menu.
# MAGIC
# MAGIC You'll find all your tables, and can use it to access and administrate your tables.
# MAGIC
# MAGIC They'll be able to create extra table into this schema.
# MAGIC
# MAGIC #### Discoverability 
# MAGIC
# MAGIC In addition, Unity catalog also provides explorability and discoverability. 
# MAGIC
# MAGIC Anyone having access to the tables will be able to search it and analyze its main usage. <br>
# MAGIC You can use the Search menu (⌘ + P) to navigate in your data assets (tables, notebooks, queries...)
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA identifier(:schema_name);
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🛡️ Data Classification, PII Scanning and Anomaly Detection
# MAGIC Unity Catalog support Automatic and wide ranging PII scanning, using a combination of GenAI and ML models to identify and classify both sensitive data, as well as anomalies in Data.
# MAGIC
# MAGIC ##### Data Classification Dashboard
# MAGIC [Dashboard for PII Scanning](https://e2-demo-field-eng.cloud.databricks.com/dashboardsv3/01eff84e38ce1766b4962cb3835a53e5/published/pages/classification-overview?o=1444828305810485&f_classification-overview%7Ecatalog-selector=mining_iron_ore_processing_demo_catalog)
# MAGIC
# MAGIC ![](https://docs.databricks.com/aws/en/assets/images/data-classification-dashboard-overview-be8575d1deec9cba00a5663139cbd016.png)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 🛡️ PII Masking, Row-Level Security & Column-Level Filtering with Unity Catalog
# MAGIC
# MAGIC The `gold_iron_ore_prediction_dataset` table contains personally identifiable information (PII), such as operator names. In the cells below, we’ll demonstrate how to protect sensitive data using column- and row-level masking techniques.

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_iron_ore_prediction_dataset"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_iop_features_protected AS SELECT * FROM gold_iron_ore_prediction_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- hls_admin group will have access to all data, all other users will see a masked information.
# MAGIC CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
# MAGIC    RETURN IF(is_account_group_member('really_important_group'), column_value, "****");
# MAGIC    
# MAGIC -- Mask all PII information
# MAGIC ALTER TABLE gold_iop_features_protected ALTER COLUMN operator_name SET MASK simple_mask;
# MAGIC
# MAGIC -- Apply row filter based on the country
# MAGIC CREATE OR REPLACE FUNCTION date_filter(date_param TIMESTAMP) 
# MAGIC RETURN 
# MAGIC   date_param > "2017-09-09T20:00:00.000+00:00";                   -- Filter rows for records with a certain date
# MAGIC
# MAGIC ALTER TABLE gold_iop_features_protected SET ROW FILTER date_filter ON (date);
# MAGIC
# MAGIC SELECT * FROM gold_iop_features_protected

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🕵️ Auditing Your Features with Time Travel
# MAGIC With our feature table now secured, we can take advantage of powerful capabilities provided by Delta Tables and Unity Catalog, such as table versioning and time travel.
# MAGIC
# MAGIC In this step, we’ll create a new table to demonstrate how you can:
# MAGIC - 🔍 Audit and review changes to a feature set over time
# MAGIC - ♻️ Restore a table to a previous version if needed — a critical feature for debugging, governance, and compliance
# MAGIC
# MAGIC These capabilities make it easy to track feature evolution, support reproducibility, and maintain trust in your machine learning pipelines.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_iop_features_version_demo AS SELECT * FROM gold_iron_ore_prediction_dataset;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now lets delete some data by "accident"
# MAGIC
# MAGIC Dont worry, we can recover it with the power of Time Travel and Table versions!

# COMMAND ----------

# MAGIC %sql
# MAGIC --Uh oh... I've deleted everything from my table!
# MAGIC DELETE FROM gold_iop_features_version_demo;
# MAGIC
# MAGIC --Lets take a look at what happened to the table - Notice the delete operation
# MAGIC DESCRIBE HISTORY gold_iop_features_version_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Restore to a Previous Version of the Table
# MAGIC
# MAGIC Luckily we can undo my mistake by restoring the table to its version before the delete Operation. 
# MAGIC
# MAGIC We can also restore back to a point in time as well!
# MAGIC
# MAGIC `RESTORE TABLE gold_iop_features_version_demo TO TIMESTAMP AS OF '2025-05-01T06:00:00';`

# COMMAND ----------

# MAGIC %sql
# MAGIC --Restoring is as easy as going back to a previous version
# MAGIC RESTORE TABLE gold_iop_features_version_demo TO VERSION AS OF 0;
# MAGIC
# MAGIC --Huzzah! It is restored!
# MAGIC SELECT * FROM gold_iop_features_version_demo

# COMMAND ----------

# MAGIC %md
# MAGIC #### Going further with Data Governance & Security
# MAGIC
# MAGIC By bringing all your data assets together, Unity Catalog let you build a complete and simple governance to help you scale your teams.
# MAGIC
# MAGIC Unity Catalog can be leveraged from simple GRANT to building a complete datamesh organization.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
# MAGIC
# MAGIC ##### Fine-grained ACL: row/column level access
# MAGIC
# MAGIC Need more advanced control? You can chose to dynamically change your table output based on the user permissions: `dbdemos.intall('uc-01-acl')`
# MAGIC
# MAGIC ##### Secure external location (S3/ADLS/GCS)
# MAGIC
# MAGIC Unity Catatalog let you secure your managed table but also your external locations:  `dbdemos.intall('uc-02-external-location')`
# MAGIC
# MAGIC ##### Lineage 
# MAGIC
# MAGIC UC automatically captures table dependencies and let you track how your data is used, including at a row level: `dbdemos.intall('uc-03-data-lineage')`
# MAGIC
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC
# MAGIC
# MAGIC ##### Audit log
# MAGIC
# MAGIC UC captures all events. Need to know who is accessing which data? Query your audit log:  `dbdemos.intall('uc-04-audit-log')`
# MAGIC
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC
# MAGIC
# MAGIC ##### Sharing data with external organization
# MAGIC
# MAGIC Sharing your data outside of your Databricks users is simple with Delta Sharing, and doesn't require your data consumers to use Databricks:  `dbdemos.intall('delta-sharing-airlines')`

# COMMAND ----------

