-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # 🛡️ Row and column level access control with Databricks Unity Catalog
-- MAGIC
-- MAGIC In this demo, you'll learn how to harness the power of Unity Catalog to secure your data at a more granular level using its *row-level* and *column-level* access control capabilities.
-- MAGIC
-- MAGIC
-- MAGIC ## Row level access control 
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC Row-level security allows you to automatically hide a subset of your rows based on who is attempting to query it, without having to maintain any seperate copies of your data.
-- MAGIC
-- MAGIC A typical use-case would be to filter out rows based on your country or Business Unit : you only see the data (financial transactions, orders, customer information...) pertaining to your region, thus preventing you from having access to the entire dataset.
-- MAGIC
-- MAGIC 💡 While this filter can be applied at the user / principal level, it is recommended to implement access policies using groups instead.
-- MAGIC <br style="clear: both"/>
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_cls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-right: 20; margin-left: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC ## Column Level access control 
-- MAGIC
-- MAGIC Similarly, column-level access control helps you mask or anonymise the data that is in certain columns of your table, depending on the user or service principal that is trying to access it. This is typically used to mask or remove sensitive PII informations from your end users (email, SSN...).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## I. Prepare the demo

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### I.1 Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_shared.png?raw=true" width="500px" style="float: right"/>
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the shared security mode enabled.
-- MAGIC
-- MAGIC 1. Go in the compute page, create a new cluster
-- MAGIC
-- MAGIC 2. **Under "Access mode", select "Shared"**
-- MAGIC
-- MAGIC Just like dynamic views, RL and CL acces control are only supported on Shared clusters for now.

-- COMMAND ----------

-- DBTITLE 1,Make sure we use our catalog and schema previously created
USE CATALOG dbdemos;
USE SCHEMA uc_acl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### I.2 Review our customers table
-- MAGIC We created the table in the previous notebook (make sure you run it before this one).
-- MAGIC
-- MAGIC The table was created without restriction, all users can access all the rows

-- COMMAND ----------

-- DBTITLE 1,Note that all rows are visible (all countries)
select * from customers

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## II. Row-level access control
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rls.png?raw=true" width="200" style="float: right; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC As mentioned earlier, row-level security allows you to automatically hide rows in your table from users, based on their identity or group assignment.
-- MAGIC
-- MAGIC In this part of the demo, we will show you how you can enforce a policy where an analyst can only access data related to customers in their country.

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC To capture the current user and check their membership to a particular group, Databricks provides you with 2 built-in functions: 
-- MAGIC - `current_user()` 
-- MAGIC - and `is_account_group_member()` respectively.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Getting the current user
SELECT current_user();

-- COMMAND ----------

-- DBTITLE 1,Am I member of the groups defined at the account level?
-- Note: The account should have been setup by adding all users to the ANALYST_USA group
SELECT is_account_group_member('account users'), is_account_group_member('bu_admin'), is_account_group_member('fr_analysts');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### II.1. Define the access rule
-- MAGIC
-- MAGIC To declare an access control rule, you will need to create a SQL function that returns a **boolean**.
-- MAGIC Unity Catalog will then hide the rows if the function returns `False`.
-- MAGIC
-- MAGIC Inside your SQL function, you can define different conditions and implement complex logic to create this boolean return value. (e.g :  `IF(condition)-THEN(view)-ELSE`)
-- MAGIC
-- MAGIC Here, we will apply the following logic :
-- MAGIC
-- MAGIC 1. if the user is a `bu_admin` group member, then they can access data from all countries. (we will use `is_account_group_member('group_name')` we saw earlier)
-- MAGIC 2. if the user is not a `bu_admin` group member, we'll restrict access to only the rows pertaining to regions `US` and `Canada` as our default regions. All other customers will be hidden!
-- MAGIC
-- MAGIC Note that columns within whatever table that this function will be applied on, can also be referred to inside the function's conditions. You can do so by using parameters.

-- COMMAND ----------

-- DBTITLE 1,Create the SQL function
CREATE OR REPLACE FUNCTION region_filter(region_param STRING) 
RETURN 
  is_account_group_member('bu_admin') or -- admin can access all regions
  region_param like "%US%" or region_param = "CANADA";  -- everybody can access regions containing US or CANADA

ALTER FUNCTION region_filter OWNER TO `account users`; -- grant access to all user to the function for the demo - don't do it in production

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### II.2. Apply the access rule
-- MAGIC
-- MAGIC With our rule function declared, all that's left to do is apply it on a table and see it in action!
-- MAGIC A simple `SET ROW FILTER` followed by a call to the function is all it takes.
-- MAGIC
-- MAGIC **Note: if this is failing, make sure you're using a Shared Cluster!**

-- COMMAND ----------

-- country will be the column send as parameter to our SQL function (country_param)
ALTER TABLE customers SET ROW FILTER region_filter ON (country);

-- COMMAND ----------

-- DBTITLE 1,🥷 Checking... Rows have been hidden. Only customers in US/Canada are visible!
select * from customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC More advanced filter can be done. As example, we can imagine we have a few regional admin user groups defined as : `regional_admin_USA`, `regional_admin_FR` and we want to use them to filter on a country value. 
-- MAGIC
-- MAGIC This can easily be done by checking the group based on the region value :

-- COMMAND ----------

CREATE OR REPLACE FUNCTION region_filter_dynamic(country_param STRING) 
RETURN 
  is_account_group_member('bu_admin') or -- admin can access all regions
  is_account_group_member(CONCAT('regional_admin_', country_param)); --regional admin can also access if the region matches the regional admin group.
  
ALTER FUNCTION region_filter_dynamic OWNER TO `account users`; -- grant access to all user to the function for the demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## III. Manage permissions to access rules
-- MAGIC Just like any other object in Unity Catalog, SQL functions you create to implement your row-level (RL) ord column-level (CL) access control, are also governable and securable with a set of permissions.
-- MAGIC
-- MAGIC With that in mind, let's allow other users to use the RL access rule `region_filter_dynamic` we've created so they too can apply it on their own tables.

-- COMMAND ----------

GRANT EXECUTE ON FUNCTION region_filter_dynamic TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IV. Column-level access control

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### IV.1. Define the access rule
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_cls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC Declaring a rule to implement column-level access control is very similar to what we did earlier for our row-level access control rule.
-- MAGIC
-- MAGIC In this example, we'll create a SQL function with the following `IF-THEN-ELSE` logic:
-- MAGIC
-- MAGIC - if the current user is member of the group `bu_admin`, then return the column value as-is (here `ssn`), 
-- MAGIC - if not, mask it completely with a constant string (here `****`)
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
   RETURN IF(is_account_group_member('bu_admin'), column_value, "****");
   
ALTER FUNCTION simple_mask OWNER TO `account users`; -- grant access to all user to the function for the demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IV.2. Apply the access rule
-- MAGIC
-- MAGIC To change things a bit, instead of applying a rule on an existing table, we'll demonstrte here how we can apply a rule upon the creation of a table.
-- MAGIC
-- MAGIC Note: In this demo we have only one column mask function to apply. In real life, you may want to apply different column masks on different columns within the same table.

-- COMMAND ----------

-- DBTITLE 1,We can apply rules during the creation of the table too!
-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS patient_ssn (
-- MAGIC   `name` STRING,
-- MAGIC    ssn STRING MASK simple_mask);
-- MAGIC    
-- MAGIC ALTER TABLE patient_ssn OWNER TO `account users`; -- for the demo only

-- COMMAND ----------

--Populating our newly-created table using the INSERT INTO command that insert some rows in it*/
INSERT INTO patient_ssn values 
("Jane Doe", "1111111"), 
("Joe Doe", "222333");

-- COMMAND ----------

-- DBTITLE 1,🥷 Checking... SSN has been anonymised for me!
select * from patient_ssn

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## V. Combine RL and CL access control
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rlscls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC Let's go back to our customer table on which we've already applied the row-level access control, and alter it to add another layer of security by applying the column-level access rule we've created in the previous step.
-- MAGIC
-- MAGIC As we apply it, let's make it target address column!

-- COMMAND ----------

-- DBTITLE 1,2- Apply function using built-in MASK
ALTER TABLE customers ALTER COLUMN address SET MASK simple_mask;

-- COMMAND ----------

-- DBTITLE 1,🥷 Checking... Addresses have been masked for me!
SELECT * FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VI. Change the definition of the access control rules
-- MAGIC If the business ever decides to change a rule's conditions or the way they want the data to be returned in response to these conditions, it is easy to adapt with Unity Catalog.
-- MAGIC
-- MAGIC Since the function is the central element, all you need to do is update it and the effects will automatically be reflected on all the tables that it has been attached to.
-- MAGIC
-- MAGIC In this example, we'll rewrite our `simple_mask` column mask function and change the way we anonymse data from the rather simplistic `****`, to using the built-in sql `MASK` function ([see documentation](https://docs.databricks.com/sql/language-manual/functions/mask.html))

-- COMMAND ----------

-- DBTITLE 1,1- Update existing function
-- you can also create custom mask transformations, such as concat(substr(maskable_param, 0, 2), "..."))
CREATE OR REPLACE FUNCTION simple_mask(maskable_param STRING)
   RETURN IF(is_account_group_member('bu_admin'), maskable_param, MASK(maskable_param, '*', '*'));
   
ALTER FUNCTION simple_mask OWNER TO `account users`; -- grant access to all user to the function for the demo  

-- COMMAND ----------

-- DBTITLE 1,🥷 Checking... masking method has changed!
select * from customers

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ## VII. Dynamic access rules with lookup data
-- MAGIC
-- MAGIC So we've seen how through functions, Unity Catalog give us the flexibility to overwrite the definition of an access rule but also combine multiple rules on a single table to ultimately implement complex multi-dimensional access control on our data.
-- MAGIC
-- MAGIC Let's take a step further by adding an intermediate table describing a permission model. We'll use this table to lookup pre-defined mappings of users to their corresponsing data, on which we'll base the bahavior of our access control function.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VII.1. Create the mapping data
-- MAGIC First, let's create this mapping table.
-- MAGIC
-- MAGIC In an organization where we have a user group for each supported language, we went ahead and mapped in this table each of these groups to their corresponding countries.
-- MAGIC
-- MAGIC The members of the `en_analysts` are thus mapped to data of the `USA` or `CANADA`, the members of the `sp_analysts` are mapped to data for `SPAIN`, `MEXICO` or `ARGENTINA`, and so on.
-- MAGIC
-- MAGIC In our case, we belong to neither so we'll fall back on the all users condition (`account users`)
-- MAGIC
-- MAGIC Note : You can easily create and your user to one of these groups using the account console if you have the permissions to do so.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Create the table containing the mapping between groups and countries
CREATE TABLE IF NOT EXISTS map_country_group (
  identity_group STRING,
  countries ARRAY<STRING>
);
ALTER TABLE map_country_group OWNER TO `account users`; -- for the demo only, allow all users to edit the table - don't do that in production!

INSERT OVERWRITE map_country_group (identity_group, countries) VALUES
  ('fr_analysts', Array("FR", "BELGIUM","CANADA","SWITZERLAND")),
  ('sp_anlysts',  Array("SPAIN","MEXICO","ARGENTINA")),
  ('us_analysts', Array("USA","CANADA"));

SELECT * FROM map_country_group;

-- COMMAND ----------

-- DBTITLE 1,Testing our table against our user and a FR country
-- This should returns only fr_analyst as this is the only group our current user is assign to.
SELECT * FROM map_country_group 
  WHERE is_account_group_member(identity_group) AND array_contains(countries, 'FR')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VII.2. Define the access rule with lookup data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Let's go ahead now and update our row filter function to call this new table and follow the mapping indicated in it :

-- COMMAND ----------

CREATE OR REPLACE FUNCTION region_filter(region_param STRING)
 RETURN 
 is_account_group_member('bu_admin') or -- the current user is super admin, we can see everything
 exists (
  -- current user is in a group and the group array contains the region. You could also do it with more advanced control, joins etc.
  -- Spark optimizer will execute that as an efficient JOIN between the map_country_group table and your main table - you can check the query execution in Spark SQL UI for more details.  
   SELECT 1 FROM map_country_group WHERE is_account_group_member(identity_group) AND array_contains(countries, region_param)
 );

-- COMMAND ----------

-- DBTITLE 1,Checking... all are French-speaking customers!
-- the current user is french and only see countries in the fr_analyst group: ["FR", "BELGIUM", "CANADA", "SWITZERLAND"]
select * from customers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Note that the same strategy can be applied at column level! You can add more advanced filters, getting metadata from other tables containing more advanced permission model based on your requirements!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VIII. Dissociate a rule from a table
-- MAGIC This dissociation of the rule from the objects you apply it to, also allows you to stop applying it on the table of your choice at any time, all without :
-- MAGIC - impacting the other tables this rule is attached to
-- MAGIC - discontinuing the other rules that are also applied to your table
-- MAGIC
-- MAGIC In this example, we'll remove the address column masking we added to the `customers` in the previous section, and see how :
-- MAGIC - everything continues to work as expected on the Social Security Number in the `patient_ssn` table on which it's also applied
-- MAGIC - the row filter continues to take effect on the `customers` table

-- COMMAND ----------

-- DBTITLE 1,1- Removing the column mask
ALTER TABLE customers ALTER COLUMN address DROP MASK;

-- COMMAND ----------

-- DBTITLE 1,💡 Checking... customer addresses are visible again!
select * from customers

-- COMMAND ----------

-- DBTITLE 1,🏖️ ... and nothing impacted the other table
select * from patient_ssn

-- COMMAND ----------

-- DBTITLE 1,2- Removing the row filter
-- MAGIC %md
-- MAGIC Let's go ahead and deactivate the row filter as well!

-- COMMAND ----------

ALTER TABLE customers DROP ROW FILTER;

-- COMMAND ----------

-- DBTITLE 1,Checking... all countries are visible again!
select * from customers

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## IX. Conclusion
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rlscls_intro.png?raw=true" width="200" style="float: right; margin-top: 20; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC In this demo, we saw how in a few and simple lines of code we are able to implement powerful access controls using Unity Catalog's row-level and column-level acess control capabilities. We :
-- MAGIC - explained the logic,
-- MAGIC - navigated the lifecycle of the different components (e.g: redefining a rule, dissociating a rule from a table),
-- MAGIC - and ventured in more complex use cases (e.g: combining multiple rules, dynamic access rules using lookup data) 
-- MAGIC
-- MAGIC ... so you feel ready to take full control of managing your data security policies! 💪
-- MAGIC
