-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dynamic views: Securing data at the row level using Databricks Unity Catalog
-- MAGIC
-- MAGIC **Note: using dynamic view was the solution before Row level and column-level masking with SQL FUNCTIONS**
-- MAGIC
-- MAGIC **We recommend using the previous [01-Row-Column-access-control]($./01-Row-Column-access-control) notebook over adding dynamic views when possible.**
-- MAGIC
-- MAGIC As seen in the previous notebook, Unity Catalog let you grant table ACL using standard SQL GRANT on all the objects (CATALOG, SCHEMA, TABLE)
-- MAGIC
-- MAGIC But this alone isn't enough. UC let you create more advanced access pattern to dynamically filter your data based on who query it.
-- MAGIC
-- MAGIC This is usefull to mask sensitive PII information, or restrict access to a subset of data without having to create and maintain multiple tables.
-- MAGIC
-- MAGIC *Note that Unity Catalog will provide more advanced data masking capabilities in the future, this demo covers what can be done now.*
-- MAGIC
-- MAGIC *Note: This is currently only supported with shared cluster (python/SQL). Single node requires access to the underlying view*
-- MAGIC
-- MAGIC See the [documentation](https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions) for more details.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=02-[legacy]-UC-Dynamic-view&demo_name=uc-01-acl&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_shared.png?raw=true" width="600" style="float: right"/>
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC
-- MAGIC 1. Go in the compute page, create a new cluster
-- MAGIC
-- MAGIC 2. Under "Access mode", select "Shared"

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup

-- COMMAND ----------

-- MAGIC %md ## Current user and is member (group)
-- MAGIC
-- MAGIC Databricks has 2 functions: `current_user()` and `is_account_group_member()`.
-- MAGIC
-- MAGIC Theses functions can be used to dynamically get the user running the query and knowing if the user is member of a give group.

-- COMMAND ----------

-- The demo will create and use the catalog defined:
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS uc_acl;
-- Make it default for future usage (we won't have to specify it)
USE CATALOG main;
USE SCHEMA uc_acl;

-- COMMAND ----------

-- DBTITLE 1,Getting the current user
SELECT current_user();

-- COMMAND ----------

-- DBTITLE 1,Am I member of the ANALYST_USA and ANALYST_FR group defined at the account level?
-- Note: The account should have been setup by adding all users to the ANALYST_USA group
SELECT is_account_group_member('account users'), is_account_group_member('ANALYST_USA'), is_account_group_member('ANALYST_FR');

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Dynamic Views: Restricting data to a subset based on a field
-- MAGIC
-- MAGIC We'll be using the previous customers table. Let's review it's content first.
-- MAGIC
-- MAGIC *Note: Make sure you run the [previous notebook]($00-UC-Table-ACL) first*

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC As you can see, this table has a `country`field. We want to be able to restrict the table access based in this country.
-- MAGIC
-- MAGIC Data Analyst and Data Scientists in USA can only access the local Dataset, same for the FR team.
-- MAGIC
-- MAGIC ### Using groups
-- MAGIC One option to do that would be to create groups in the Unity Catalog. We can name the groups as the concatenation of `CONCAT("ANALYST_", country)`:
-- MAGIC * `ANALYST_FR`
-- MAGIC * `ANALYST_USA`. 
-- MAGIC * `ANALYST_SPAIN`. 
-- MAGIC
-- MAGIC You can then add a view with `CASE ... WHEN` statement based on your groups to define when the data can be accessed.
-- MAGIC
-- MAGIC See the [documentation](https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions) for more details on that.
-- MAGIC
-- MAGIC But what makes the `is_member()` function powerful is that you can combine it with a column. Let's see how we can use it to dynamically check access based on the row.
-- MAGIC  
-- MAGIC We'll create a field named `group_name` as the concatenation of ANALYST and the country, and then for each value check if the current user is a member of this group:

-- COMMAND ----------

-- as ANALYST from the USA (ANALYST_USA group), each USA row are now at "true"
SELECT is_account_group_member(group_name), * FROM (
  SELECT CONCAT("ANALYST_", country) AS group_name, country, id, firstname FROM customers)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC As you can see, we are not admin on any of these group.
-- MAGIC We can create a view securiting this data and only grant our analyst access to this view: 

-- COMMAND ----------

CREATE OR REPLACE VIEW customer_dynamic_view  AS (
  SELECT * FROM customers as customers WHERE is_account_group_member(CONCAT("ANALYST_", country))
);
-- Then grant select access on the view only
GRANT SELECT ON VIEW customer_dynamic_view TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Because we're not part of any group, we won't have access to the data. Users being in the `ANALYST_FR` group will have a filter to access only the FR country.
-- MAGIC
-- MAGIC All we have to do now is add our users to the groups to be able to have access

-- COMMAND ----------

-- We should be part of the ANALYST_USA group. As result, we now have a row-level filter applied in our secured view and we only see the USA country:
select * from customer_dynamic_view

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Dynamic Views & data masking
-- MAGIC
-- MAGIC The country example was a first level of row-level security implementation. We can implement more advances features using the same pattern.
-- MAGIC
-- MAGIC Let's see how Dynamic views can also be used to add data masking. For this example we'll be using the `current_user()` functions.
-- MAGIC
-- MAGIC Let's create a table with all our current analyst permission including a GDPR permission flag: `analyst_permissions`.
-- MAGIC
-- MAGIC This table has 3 field:
-- MAGIC
-- MAGIC * `analyst_email`: to identify the analyst (we could work with groups instead)
-- MAGIC * `country_filter`: we'll filter the dataset based on this value
-- MAGIC * `gdpr_filter`: if true, we'll filter the PII information from the table. If not set the user can see all the information
-- MAGIC
-- MAGIC *Of course this could be implemented with the previous `is_account_group_member()` function instead of individual users information being saved in a permission tale.*
-- MAGIC
-- MAGIC Let's query this table and check our current user permissions. As you can see I don't have GDPR filter enabled and a filter on FR is applied for me in the permission table we created.

-- COMMAND ----------

select * from analyst_permissions where analyst_email = current_user()

-- COMMAND ----------

-- DBTITLE 1,Let's create the secure view to filter PII information and country based on the analyst permission
CREATE OR REPLACE VIEW customer_dynamic_view_gdpr AS (
  SELECT 
  id ,
  creation_date,
  country,
  gender,
  age_group,
  CASE WHEN country.gdpr_filter=1 THEN sha1(firstname) ELSE firstname END AS firstname,
  CASE WHEN country.gdpr_filter=1 THEN sha1(lastname)  ELSE lastname  END AS lastname,
  CASE WHEN country.gdpr_filter=1 THEN sha1(email)     ELSE email     END AS email
  FROM 
    customers as customers INNER JOIN 
    analyst_permissions country  ON country_filter=country
  WHERE 
    country.analyst_email=current_user() 
);
-- Then grant select access on the view only
GRANT SELECT ON VIEW customer_dynamic_view_gdpr TO `account users`;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying the secured view
-- MAGIC Let's now query the view. Because I've a filter on `COUNTRY=FR`and `gdpr_filter=0`, I'll see all the FR customers information. 

-- COMMAND ----------

-- MAGIC %sql select * from customer_dynamic_view_gdpr 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's now change my permission. We'll enable the `gdpr_filter` flag and change our `country_filter` to USA.
-- MAGIC
-- MAGIC As you can see, requesting the same secured view now returns all the USA customers, and PII information has been obfuscated:

-- COMMAND ----------

UPDATE analyst_permissions SET country_filter='USA', gdpr_filter=1 where analyst_email=current_user();

select * from customer_dynamic_view_gdpr ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC As we've seen, data masking and filtering can be implemented at a row level using groups, users and even extra table that you can use to manage more advanced permissions.
-- MAGIC
-- MAGIC You're now ready to deploy the Lakehouse for your entire organisation, securing data based on your own governance, ensuring PII regulation and governance.
