-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unify Governance and security for all users and all data
-- MAGIC
-- MAGIC Data governance and security is hard when it comes to a complete Data Platform. SQL GRANT on tables isn't enough and security must be enforced for multiple data assets (dashboards, Models, files etc).
-- MAGIC
-- MAGIC To reduce risks and driving innovation, Emily's team needs to:
-- MAGIC
-- MAGIC - Unify all data assets (Tables, Files, ML models, Features, Dashboards, Queries)
-- MAGIC - Onboard data with multiple teams
-- MAGIC - Share & monetize assets with external Organizations
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=02-Data-Governance-credit-decisioning&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Implementing a global data governance and security with Unity Catalog
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
-- MAGIC * Fined grained ACL,
-- MAGIC * Audit log,
-- MAGIC * Data lineage,
-- MAGIC * Data exploration & discovery,
-- MAGIC * Sharing data with external organization (Delta Sharing),
-- MAGIC * (*coming soon*) Attribute-based access control. 

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup $reset_all_data=false

-- COMMAND ----------

--CREATE CATALOG IF NOT EXISTS dbdemos;
--USE CATALOG dbdemos;
SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- DBTITLE 1,As you can see, our tables are available under our catalog.
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 1. Access control
-- MAGIC
-- MAGIC In the Lakehouse, you can use simple SQL GRANT and REVOKE statements to create granular (on data and even schema and catalog levels) access control irrespective of the data source or format.

-- COMMAND ----------

-- Let's grant our ANALYSTS a SELECT permission:
-- Note: make sure you created an analysts and dataengineers group first.
GRANT SELECT ON TABLE main__build.dbdemos_fsi_credit_decisioning.credit_bureau_gold TO `analysts`;
GRANT SELECT ON TABLE main__build.dbdemos_fsi_credit_decisioning.customer_gold TO `analysts`;
GRANT SELECT ON TABLE main__build.dbdemos_fsi_credit_decisioning.fund_trans_gold TO `analysts`;

-- We'll grant an extra MODIFY to our Data Engineer
GRANT SELECT, MODIFY ON SCHEMA main__build.dbdemos_fsi_credit_decisioning TO `dataengineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 2. PII data masking, row and column-level filtering
-- MAGIC
-- MAGIC In the cells below we will demonstrate how to handle sensitive data through column and row masking.

-- COMMAND ----------

CREATE OR REPLACE VIEW  customer_gold_secured AS
SELECT
  c.* EXCEPT (first_name),
  CASE
    WHEN is_member('data_scientists')
    THEN base64(aes_encrypt(c.first_name, 'YOUR_SECRET_FROM_MANAGER')) -- save secret in Databricks manager and load it with secret('<YOUR_SCOPE> ', '<YOUR_SECRET_NAME>')
    ELSE c.first_name
  END AS first_name
FROM
  customer_gold AS c;

-- COMMAND ----------

-- CREATE GROUP data_scientists;
ALTER GROUP `data_scientists` ADD USER `quentin.ambard@databricks.com`;

SELECT
  current_user() as user,
  is_member("data_scientists") as user_is_data_scientists ;

-- COMMAND ----------

SELECT cust_id, first_name FROM customer_gold_secured;

-- COMMAND ----------

ALTER GROUP `data_scientists` DROP USER `quentin.ambard@databricks.com`;

-- COMMAND ----------

-- MAGIC %python time.sleep(60) #make sure the change is visible with sql alter group

-- COMMAND ----------

SELECT cust_id, first_name FROM customer_gold_secured;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC As we can observe from the cells above, the ```first_name``` column is masked whenever the current user requesting the data is part of the ```data-science-users``` group, and not masked if other type of users queries the data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 3. (Data and assets) Lineage
-- MAGIC
-- MAGIC Lineage is critical for understanding compliance, audit, observability, but also discoverability of data.
-- MAGIC
-- MAGIC These are three very common schenarios, where full data lineage becomes incredibly important:
-- MAGIC 1. **Explainability** - we need to have the means of tracing features used in machine learning to the raw data that created those features,
-- MAGIC 2. Tracing **missing values** in a dashboard or ML model to the origin,
-- MAGIC 3. **Finding specific data** - organizations have hundreds and even thousands of data tables and sources. Finiding the table or column that contains specific information can be daunting without a proper discoverability tools.
-- MAGIC
-- MAGIC In the image below, you can see every possible data (both ingested and created internally) in the same lineage graph, irrespective of the data type (stream vs batch), file type (csv, json, xml), language (SQL, python), or tool used (DLT, SQL query, Databricks Feature Store, or a python Notebook).
-- MAGIC
-- MAGIC **Note**: To explore the whole lineage, open navigate to the Data Explorer, and find the ```customer_gold``` table inside your catalog and database.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/UC.png" />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 4. Secure data sharing
-- MAGIC
-- MAGIC Once our data is ready, we can easily share it leveraging Delta Sharing, an open protocol to share your data assets with any customer or partnair.
-- MAGIC
-- MAGIC For more details on Delta Sharing, run `dbdemos.install('delta-sharing-airlines')`

-- COMMAND ----------

-- DBTITLE 1,Create a Delta Sharing Share
CREATE SHARE IF NOT EXISTS dbdemos_credit_decisioning_customer 
  COMMENT 'Sharing the Customer Gold table from the Credit Decisioning Demo.';
 
-- For the demo we'll grant ownership to all users. Typical deployments wouls have admin groups or similar.
ALTER SHARE dbdemos_credit_decisioning_customer OWNER TO `account users`;

-- Simply add the tables you want to share to your SHARE:
-- ALTER SHARE dbdemos_credit_decisioning_customer  ADD TABLE dbdemos.fsi_credit_decisioning.credit_bureau_gold ;

-- COMMAND ----------

DESCRIBE SHARE dbdemos_credit_decisioning_customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: leverage your data to better serve your customers and reduce credit default risk
-- MAGIC
-- MAGIC Our data is now ingested, secured, and our Data Scientist can access it.
-- MAGIC
-- MAGIC Let's get the maximum value out of the data we ingested: open the [Feature Engineering notebook]($../03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning) and start creating features for our machine learning models 
-- MAGIC
-- MAGIC Go back to the [Introduction]($../00-Credit-Decisioning).
