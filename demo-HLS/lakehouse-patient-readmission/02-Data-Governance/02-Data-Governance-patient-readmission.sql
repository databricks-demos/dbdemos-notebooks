-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Unify Governance and security for all users and all data
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-flow-2.png" style="float: right; margin-left: 10px; margin-top:10px" width="650px" />
-- MAGIC
-- MAGIC Our dataset contains sensitive information on our patients and admission.
-- MAGIC
-- MAGIC It's critical to be able to add a security and privacy layer on top of our data, but also on all the other data assets that will use this data (notebooks dashboards, Models, files etc).
-- MAGIC
-- MAGIC This is made easy with Databricks Unity Catalog.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=02-Data-Governance-patient-readmission&demo_name=lakehouse-patient-readmission&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Implementing a global data governance and security with Unity Catalog
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
-- MAGIC * Fined grained ACL, Row and Column Masking 
-- MAGIC * Audit log,
-- MAGIC * Data lineage,
-- MAGIC * Data exploration & discovery,
-- MAGIC * Sharing data with external organization (Delta Sharing),
-- MAGIC * (*coming soon*) Attribute-based access control. 

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup $reset_all_data=false

-- COMMAND ----------

SELECT CURRENT_CATALOG(), CURRENT_DATABASE();

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
GRANT SELECT ON TABLE drug_exposure TO `analysts`;
GRANT SELECT ON TABLE condition_occurrence TO `analysts`;
GRANT SELECT ON TABLE patients TO `analysts`;

-- We'll grant an extra MODIFY to our Data Engineer
-- GRANT SELECT, MODIFY ON SCHEMA dbdemos_hls_readmission TO `dataengineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 2. PII data masking, row and column-level filtering
-- MAGIC
-- MAGIC In the cells below we will demonstrate how to handle sensitive data through column and row masking.

-- COMMAND ----------

CREATE OR REPLACE TABLE protected_patients AS SELECT * FROM patients;

-- COMMAND ----------

-- hls_admin group will have access to all data, all other users will see a masked information.
CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
   RETURN IF(is_account_group_member('hls_admin'), column_value, "****");
   
-- ALTER FUNCTION simple_mask OWNER TO `account users`; -- grant access to all user to the function for the demo - don't do it in production

-- Mask all PII information
ALTER TABLE protected_patients ALTER COLUMN FIRST SET MASK simple_mask;
ALTER TABLE protected_patients ALTER COLUMN LAST SET MASK simple_mask;
ALTER TABLE protected_patients ALTER COLUMN PASSPORT SET MASK simple_mask;
ALTER TABLE protected_patients ALTER COLUMN DRIVERS SET MASK simple_mask;
ALTER TABLE protected_patients ALTER COLUMN SSN SET MASK simple_mask;
ALTER TABLE protected_patients ALTER COLUMN ADDRESS SET MASK simple_mask;

SELECT * FROM protected_patients

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
-- MAGIC
-- MAGIC ### 4. Secure data sharing
-- MAGIC
-- MAGIC Once our data is ready, we can easily share it leveraging Delta Sharing, an open protocol to share your data assets with any customer or partnair.
-- MAGIC
-- MAGIC For more details on Delta Sharing, run `dbdemos.install('delta-sharing-airlines')`

-- COMMAND ----------

-- DBTITLE 1,Create a Delta Sharing Share
CREATE SHARE IF NOT EXISTS dbdemos_patient_readmission_visits 
  COMMENT 'Sharing the Customer Gold table from the Credit Decisioning Demo.';
 
-- For the demo we'll grant ownership to all users. Typical deployments wouls have admin groups or similar.
ALTER SHARE dbdemos_patient_readmission_visits OWNER TO `account users`;

-- Simply add the tables you want to share to your SHARE:
-- ALTER SHARE dbdemos_patient_readmission_visits  ADD TABLE patients ;

-- COMMAND ----------

DESCRIBE SHARE dbdemos_patient_readmission_visits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: Start Data Analysis on top of our existing dataset
-- MAGIC
-- MAGIC Our data is now ingested, secured, and our Data Scientist can access it.
-- MAGIC
-- MAGIC
-- MAGIC Let's get the maximum value out of the data we ingested: open the [Data Science and Analysis notebook]($../03-Data-Analysis-BI-Warehousing/03-Data-Analysis-BI-Warehousing-patient-readmission) and start building our patient cohorts.
-- MAGIC
-- MAGIC Go back to the [Introduction]($../00-patient-readmission-introduction).
