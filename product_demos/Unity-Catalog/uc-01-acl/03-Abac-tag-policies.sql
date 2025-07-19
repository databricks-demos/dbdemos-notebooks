-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## ABAC
-- MAGIC
-- MAGIC
-- MAGIC ABAC is a data governance model that provides flexible, scalable, and centralized access control across Databricks. ABAC complements Unity Catalog's existing privilege model by allowing policies to be defined based on governed tags, which are applied to data assets. This simplifies governance and strengthens security posture.
-- MAGIC <br style="clear: both"/>
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rlscls_intro.png?raw=true" width="200" style="float: right; margin-top: 20; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC ## Policies 
-- MAGIC
-- MAGIC Policies are created and managed at three hierarchical levels within Unity Catalog:
-- MAGIC Catalog level: Apply broad policies affecting all contained schemas and tables.
-- MAGIC Schema level: Apply policies specific to a schema and its tables.
-- MAGIC Table level: Apply fine-grained policies directly on individual tables.
-- MAGIC
-- MAGIC
-- MAGIC Two types of ABAC policies are supported:
-- MAGIC
-- MAGIC Row filter policies restrict access to individual rows in a table based on their content. A filter UDF evaluates whether each row should be visible to a user. These policies are useful when access depends on data characteristics.
-- MAGIC
-- MAGIC
-- MAGIC Column mask policies control what values users see in specific columns. A masking UDF can return the actual value or a redacted version, based on governed tags.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup

-- COMMAND ----------

SELECT current_user(), is_account_group_member('ANALYST_USA_DBDEMO');

-- COMMAND ----------

USE CATALOG main;
use schema dbdemos_uc_01_acl;

SELECT CURRENT_CATALOG(),current_schema();

-- COMMAND ----------

-- MAGIC %sql CREATE OR REPLACE TABLE 
-- MAGIC   customers (
-- MAGIC     `name` STRING, 
-- MAGIC     ssn STRING ,
-- MAGIC     region String);

-- COMMAND ----------

INSERT INTO
  customers
values
  ("Jane Doe", "111-11-1111", "eu"),
  ("Joane Doe", "123-12-1234", "us"),
  ("Joe Doe", "222-33-4444","apac");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Row Filter Policies
-- MAGIC  Row filters allow you to apply a filter to a table so that queries return only rows that meet the filter criteria. Row filter policies are implemented using a user-defined function (UDF) written in SQL. Python and Scala UDFs are also supported, but only when they are wrapped in SQL UDFs.
-- MAGIC
-- MAGIC Row Filter policies extend this functionality by referencing a specific tag, and is then enforced anywhere the tag is applied.  The syntax for building a row filter policy is as follows
-- MAGIC

-- COMMAND ----------

--CREATE UDF - Required as the Policy will call this UDF at  runtime
CREATE or REPLACE FUNCTION non_eu_region (geo_region STRING) 
RETURNS BOOLEAN
  RETURN geo_region <> 'eu';


-- COMMAND ----------

-- CREATE POLICY - Calls the UDF created in the first step.  
CREATE or REPLACE POLICY hide_eu_customers 
ON SCHEMA main.dbdemos_uc_01_acl
COMMENT 'Hide european customers from sensitive tables'
ROW FILTER non_eu_region
TO ANALYST_USA_DBDEMO
FOR TABLES
MATCH COLUMNS 
  hasTag('geo_region') AS region
USING COLUMNS(region);



-- COMMAND ----------

-- TAG A TABLE - tag a table in the prod.customers schema so the 
--  policy will be applied
ALTER  TABLE main.dbdemos_uc_01_acl.customers
ALTER COLUMN region
SET TAGS('geo_region');

-- COMMAND ----------

--GRANT USER ACCESS - Grant a user SELECT access to a table 
GRANT SELECT ON TABLE main.dbdemos_uc_01_acl.customers TO ANALYST_USA_DBDEMO

-- COMMAND ----------

select * from main.dbdemos_uc_01_acl.customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Column Mask Policies
-- MAGIC Column masks let you apply a masking function to a table column. The masking function evaluates at query runtime, substituting each reference of the target column with the results of the masking function. For most use cases, column masks determine whether to return the original column value or redact it based on the identity of the invoking user. Column masks are expressions written as SQL UDFs or as Python or Scala UDFs that are wrapped in SQL UDFs.
-- MAGIC
-- MAGIC Column Masking Policies extend this functionality by referencing a specific tag and is then enforced anywhere the tag is applied.
-- MAGIC

-- COMMAND ----------

-- CREATE UDF - Required as the Policy will call this UDF at //  
--  runtime
CREATE or REPLACE FUNCTION mask_SSN (ssn STRING) 
RETURN '***-**-****';


-- COMMAND ----------

-- UPDATE POLICY - Calls the UDF created in the first step.  
CREATE or REPLACE POLICY mask_ssn 
ON SCHEMA main.dbdemos_uc_01_acl
COMMENT 'mask ssn'
COLUMN MASK mask_SSN
TO ANALYST_USA_DBDEMO
FOR TABLES
MATCH COLUMNS 
  hasTagValue('pii','ssn') AS ssn
ON COLUMN ssn;

-- COMMAND ----------

-- TAG A TABLE - tag a table in the prod.customers schema so the 
--  policy will be applied
ALTER  TABLE main.dbdemos_uc_01_acl.customers
ALTER COLUMN ssn
SET TAGS('pii'='ssn');

-- COMMAND ----------

select * from main.dbdemos_uc_01_acl.customers

-- COMMAND ----------

DROP POLICY hide_eu_customers ON Schema main.dbdemos_uc_01_acl;

DROP POLICY mask_ssn ON Schema main.dbdemos_uc_01_acl;


-- COMMAND ----------


