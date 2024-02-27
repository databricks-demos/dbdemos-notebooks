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
-- MAGIC ## First things first...
-- MAGIC
-- MAGIC ### Confirm you have the correct `group membership`
-- MAGIC
-- MAGIC To see the desired results for this demo, this notebook assumes that the user 
-- MAGIC -  __is__ a member of groups `ANALYST_USA` and `region_admin_SPAIN`
-- MAGIC -  is __not__ a member of groups `bu_admin` and `fr_analysts`
-- MAGIC
-- MAGIC If you are not a member of `region_admin_SPAIN` you should be able to add yourself via workspace admin console:
-- MAGIC
-- MAGIC __Workspace settings / Identity and access / Groups__
-- MAGIC
-- MAGIC (more on how to confirm proper group membership, below)

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
-- MAGIC 2. Under **"Access mode"**, select **"Shared"**
-- MAGIC
-- MAGIC Just like dynamic views, Row Level and Column Level access control are only supported on Shared clusters for now.

-- COMMAND ----------

-- MAGIC %md ## Designate the same catalog/schema used in the "00-UC-Table-ACL" demo notebook
-- MAGIC > ##...as that's where the sample data was generated.

-- COMMAND ----------

USE CATALOG dbdemos_ogden; -- change to 'dbdemos' prior to publishing
USE SCHEMA uc_acl;

-- COMMAND ----------

-- DBTITLE 1,Testing.  Remove before publishing
select current_catalog(), current_database()

-- COMMAND ----------

-- MAGIC %md ## Ensuring no filters or masks are currently applied to the table:

-- COMMAND ----------

-- Removing any row-filters or masks on the 'address' column that may have been added in previous runs of the demo

ALTER TABLE customers DROP ROW FILTER;
ALTER TABLE customers ALTER COLUMN address DROP MASK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### I.2 Review our 'customers' table
-- MAGIC We created the table in the previous notebook (make sure you run it before this one).
-- MAGIC
-- MAGIC The table was created without restriction, all users can access all the rows

-- COMMAND ----------

-- Note that all rows are visible (all countries) and the 'address' column has no masking:

SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
FROM customers;

-- COMMAND ----------

-- Confirming that we can see all countries (FR, USA, SPAIN) prior to setting up row-filters:

SELECT DISTINCT(country) FROM customers;

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

-- get the current user (for informational purposes)

SELECT current_user();

-- COMMAND ----------

-- MAGIC %md ## NOTICE:  important to confirm proper group membership, below:

-- COMMAND ----------

-- Am I member of the groups defined at the account level?

SELECT 
  is_account_group_member('account users') is_account_user,            -- should return True
  is_account_group_member('ANALYST_USA') is_analyst_usa,               -- should return True
  is_account_group_member('region_admin_SPAIN') is_region_admin_spain, -- should return True (may need to add yourself)
  is_account_group_member('bu_admin') is_bu_admin,                     -- should return False
  is_account_group_member('fr_analysts') is_fr_analyst;                -- should return False

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC To see the intended results, the above query should return:  
-- MAGIC -  `true, true, true, false, false`
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### II.1. Define the access rule
-- MAGIC
-- MAGIC To declare an access control rule, you will need to create a SQL function that returns a **boolean**.
-- MAGIC Unity Catalog will then hide the row if the function returns `False`.
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

-- MAGIC %md ## Create a SQL function for a simple row-filter:

-- COMMAND ----------

CREATE OR REPLACE FUNCTION region_filter(region_param STRING) 
RETURN 
  is_account_group_member('bu_admin') or                -- bu_admin can access all regions
  region_param like "%US%" or region_param = "CANADA";  -- non bu_admin's can access regions containing US or CANADA (no CANADA in our table, but you get the idea)

-- Grant access to all users to the function for the demo by making all account users owners.  Note: Don't do this in production!
ALTER FUNCTION region_filter OWNER TO `account users`; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### II.2. Apply the access rule
-- MAGIC
-- MAGIC With our rule function declared, all that's left to do is apply it on a table and see it in action!
-- MAGIC A simple `SET ROW FILTER` followed by a call to the function is all it takes.
-- MAGIC
-- MAGIC **Note: if this is failing, make sure you're using a Shared Cluster!**

-- COMMAND ----------

-- apply access rule to customers table...
-- country will be the column sent as parameter to our SQL function (region_param)

ALTER TABLE customers SET ROW FILTER region_filter ON (country);

-- COMMAND ----------

-- MAGIC %md ## 🥷 Confirm only customers in USA are visible:

-- COMMAND ----------

SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
FROM customers;

-- COMMAND ----------

-- We should see only USA and Canada here, unless the user is a member of bu_admin:

SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Now let's drop the current filter, and demonstrate more dynamic versions...

-- COMMAND ----------

ALTER TABLE customers DROP ROW FILTER;

-- Confirming that we can once again see all countries:
SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### More advanced filters can be done. 
-- MAGIC As example, we can imagine we have a few regional admin user groups defined as : `region_admin_USA`, `region_admin_SPAIN`, etc... and we want to use these groups to *dynamically* filter on a country value. 
-- MAGIC
-- MAGIC This can easily be done by checking the group based on the region value.

-- COMMAND ----------

-- DBTITLE 1,Create an advanced access rule
CREATE OR REPLACE FUNCTION region_filter_dynamic(country_param STRING) 
RETURN 
  is_account_group_member('bu_admin') or -- bu_admin can access all regions
  is_account_group_member(CONCAT('region_admin_', country_param)); --regional admins can access only if the region (country column) matches the regional admin group suffix.
  
-- instead of making full owners of our function, we can use GRANT EXECUTE below instead
-- ALTER FUNCTION region_filter_dynamic OWNER TO `account users`; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## III. Manage permissions to access rules
-- MAGIC Just like any other object in Unity Catalog, SQL functions you create to implement your row-level (RL) or column-level (CL) access control, are also governable and securable with a set of permissions.
-- MAGIC
-- MAGIC With that in mind, let's allow other users to use the RL access rule `region_filter_dynamic` we've created so they too can apply it on their own tables.

-- COMMAND ----------

-- grant execute permissions on the function:

GRANT EXECUTE ON FUNCTION region_filter_dynamic TO `account users`;

-- COMMAND ----------

-- apply access rule to the customers table:

ALTER TABLE customers SET ROW FILTER region_filter_dynamic ON (country);

-- COMMAND ----------

-- Since our current user is a member of region_admin_SPAIN, now they see only SPAIN from our query:

SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Making filters dynamic
-- MAGIC For the above dynamic row filter, if we have various groups of the form `region_admin_{country}`, we could add and remove users from those groups, and their row access would change dynamically without needing to modify the function that implements the dynamic row filter.

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

-- create a SQL function for a simple column mask:

CREATE OR REPLACE FUNCTION simple_mask(column_value STRING)
RETURN 
  IF(is_account_group_member('bu_admin'), column_value, "****");
   
-- Grant access to all users to the function for the demo by making all account users owners.  Note: Don't do this in production!
ALTER FUNCTION simple_mask OWNER TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IV.2. Apply the access rule
-- MAGIC
-- MAGIC To change things a bit, instead of applying a rule on an existing table, we'll demonstrte here how we can apply a rule upon the creation of a new table.
-- MAGIC
-- MAGIC Note: In this demo we have only one column mask function to apply. In real life, you may want to apply different column masks on different columns within the same table.

-- COMMAND ----------

-- Just as we can ALTER a table with new access controls, we can also apply rules during the CREATE OR REPLACE TABLE:

CREATE OR REPLACE TABLE 
  patient_ssn (
    `name` STRING, 
    ssn STRING MASK simple_mask);

-- for the demo only
ALTER TABLE
  patient_ssn OWNER TO `account users`;

-- COMMAND ----------

-- Populating our newly-created table using the INSERT INTO command to insert some rows:

INSERT INTO
  patient_ssn
values
  ("Jane Doe", "111-11-1111"),
  ("Joe Doe", "222-33-4444");

-- COMMAND ----------

-- MAGIC %md ## We can see in our SELECT results that SSN has been masked:

-- COMMAND ----------

SELECT * FROM patient_ssn;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## V. Combine RL and CL access control
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rlscls.png?raw=true" width="200" style="float: right; margin-top: 20; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC Let's go back to our customer table on which we've already applied the row-level access control, and alter it to add another layer of security by applying the column-level access rule we've created in the previous step.
-- MAGIC
-- MAGIC As we apply it, let's make it's target the 'address' column!

-- COMMAND ----------

-- applying our simple masking function to the 'address' column in the 'customers' table:

ALTER TABLE
  customers
ALTER COLUMN
  address
SET
  MASK simple_mask;

-- COMMAND ----------

-- DBTITLE 1,🥷 Confirm Addresses have been masked
-- confirming 'address' columns has been masked:

SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## VI. Change the definition of the access control rules
-- MAGIC If the business ever decides to change a rule's conditions or the way they want the data to be returned in response to these conditions, it is easy to adapt with Unity Catalog.
-- MAGIC
-- MAGIC Since the function is the central element, all you need to do is update it and the effects will automatically be reflected on all the tables that it has been attached to.
-- MAGIC
-- MAGIC In this example, we'll rewrite our `simple_mask` column mask function and change the way we anonymse data from the rather simplistic `****`, to using the built-in sql `MASK` function ([see documentation](https://docs.databricks.com/sql/language-manual/functions/mask.html))

-- COMMAND ----------

-- Updating the existing simple_mask function to provide more advanced masking options via the built-in SQL MASK function:

CREATE OR REPLACE FUNCTION simple_mask(maskable_param STRING)
   RETURN 
      IF(is_account_group_member('bu_admin'), maskable_param, MASK(maskable_param, '*', '*'));
      -- You can also create custom mask transformations, such as concat(substr(maskable_param, 0, 2), "..."))
   
 -- grant access to all user to the function for the demo  
ALTER FUNCTION simple_mask OWNER TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Notice: we do not need to ALTER the table with the updated function for it to take effect.

-- COMMAND ----------

-- confirming the masking method has changed:

SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
FROM customers;

-- COMMAND ----------

-- confirming the masking method has also changed for the patient_ssn table:

SELECT * FROM patient_ssn;

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
-- MAGIC - The members of the `ANALYST_USA` are thus mapped to data for `USA` or `CANADA`, 
-- MAGIC - the members of the `region_admin_SPAIN` are mapped to data for `SPAIN`, `MEXICO` or `ARGENTINA`,
-- MAGIC - etc
-- MAGIC
-- MAGIC In our case, our user belongs to `ANALYST_USA` and `region_admin_SPAIN`...as well as (`account users`).
-- MAGIC
-- MAGIC Note : You can easily create and assign your user to one of these groups using the account console if you have the permissions to do so.
-- MAGIC

-- COMMAND ----------

-- Let's first remind ourselves what the current row-filter function (applied to our table) allows us to see:
-- We can only see SPAIN, based on the current 'region_filter_dynamic' function:

SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md ## Creating a mapping table that contains information about what countries each user group can see:

-- COMMAND ----------

-- Creating a table containing the mapping between groups and countries

CREATE TABLE IF NOT EXISTS 
  map_country_group (
    identity_group STRING,
    countries ARRAY<STRING>
);

-- for the demo only, allow all users to edit the table - don't do this in production!
ALTER TABLE map_country_group OWNER TO `account users`; 

INSERT OVERWRITE 
  map_country_group (identity_group, countries) VALUES
    ('fr_analysts', Array("FR", "BELGIUM","CANADA","SWITZERLAND")),
    ('region_admin_SPAIN',  Array("SPAIN","MEXICO","ARGENTINA")),
    ('ANALYST_USA', Array("USA","CANADA"));

SELECT * FROM map_country_group;

-- COMMAND ----------

-- MAGIC %md ## Querying the mapping table to see of which groups our current user is a member:

-- COMMAND ----------

-- Query the map_country_group table to see how current user is mapped.
-- This should return the rows for ANALYST_USA and region_admin_SPAIN since our user is assigned to those groups:

SELECT * FROM map_country_group 
  WHERE is_account_group_member(identity_group) ; 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Previously we manually specified country values in the filter function definition, and then dynamically matched the country based on membership to a region_admin_{country} membership.
-- MAGIC
-- MAGIC Now we will use a table lookup approach in our filter function definition.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VII.2. Define the access rule with lookup data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's now update our dynamic row filter function to call this new table-lookup approach.
-- MAGIC
-- MAGIC - If the current user is in the group `bu_admin`, they will be able to see all rows
-- MAGIC - If the user is in another group which has a row in the `map_country_group` table, allow access to rows for the corresponding countries
-- MAGIC
-- MAGIC You could also do it with more advanced control such as joins etc.  
-- MAGIC
-- MAGIC Spark optimizer will execute that as an efficient JOIN between the map_country_group table and your main table. You can check the query execution in Spark SQL UI for more details.

-- COMMAND ----------

-- Create a SQL function for row-level filter based on mapping table lookup:

CREATE OR REPLACE FUNCTION region_filter_dynamic(region_param STRING)
  RETURN 
    is_account_group_member('bu_admin') or
    exists (  
      SELECT 1 
      FROM map_country_group 
      WHERE is_account_group_member(identity_group) AND array_contains(countries, region_param)
    );

GRANT EXECUTE ON FUNCTION region_filter_dynamic TO `account users`;

-- COMMAND ----------

-- MAGIC %md ## Modifying our dynamic filter function to use the table lookup approach...
-- MAGIC > ## now we should see both USA and SPAIN:

-- COMMAND ----------

SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The current user 
-- MAGIC -  can see countries mapped to the ANALYST_USA group: ["USA", "CANADA"], 
-- MAGIC -  but is also a regional admin for SPAIN, which our table lookup results in visibility to ["SPAIN", "MEXICO", "ARGENTINA"]

-- COMMAND ----------

-- confirm only USA and SPAIN data are now visible for our current user:

SELECT DISTINCT(country) FROM customers;

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

-- DBTITLE 1,Remove the column mask
-- removing the column mask on 'address' from the 'customers' table:

ALTER TABLE customers ALTER COLUMN address DROP MASK;

-- COMMAND ----------

-- now we can see that customer addresses are visible again:

SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
FROM customers;

-- COMMAND ----------

-- and confirming that our mask is unaffected on the patient_ssn table:

SELECT * FROM patient_ssn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's go ahead and deactivate the row filter from `customers` as well!

-- COMMAND ----------

-- dropping the row filter:

ALTER TABLE customers DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md ## Confirming that all countries are visible again:

-- COMMAND ----------

SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md ## Removing the mask from the patient_ssn table:

-- COMMAND ----------

ALTER TABLE patient_ssn ALTER COLUMN ssn DROP MASK;

-- COMMAND ----------

-- Confirm ssn's are no longer masked:

SELECT * FROM patient_ssn;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## IX. Conclusion
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rlscls_intro.png?raw=true" width="200" style="float: right; margin-top: 20; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC In this demo, we saw how through a few simple lines of code we are able to implement powerful access controls using Unity Catalog's row-level and column-level access control capabilities. We :
-- MAGIC - explained the logic,
-- MAGIC - navigated the lifecycle of the different components (e.g: redefining a rule, dissociating a rule from a table),
-- MAGIC - and ventured in more complex use cases (e.g: combining multiple rules, dynamic access rules using lookup data) 
-- MAGIC
-- MAGIC ... so you feel ready to take full control of managing your data security policies! 💪
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A note on Delta Sharing...
-- MAGIC Delta Sharing does not work with SQL-function-based row-level security or column masks (i.e. the methods above).  
-- MAGIC To create row and column level security within a Delta Share, you'd need to use dynamic views (please see the [documentation](https://docs.databricks.com/en/data-sharing/create-share.html#add-dynamic-views-to-a-share-to-filter-rows-and-columns)).
