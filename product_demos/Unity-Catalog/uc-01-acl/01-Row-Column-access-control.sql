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
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=01-Row-Column-access-control&demo_name=uc-01-acl&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1. Prepare the demo

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1.1 Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_shared.png?raw=true" width="500px" style="float: right"/>
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the shared security mode enabled.
-- MAGIC
-- MAGIC 1. Go in the compute page, create a new cluster
-- MAGIC 2. Under **"Access mode"**, select **"Shared"**
-- MAGIC
-- MAGIC Just like dynamic views, Row Level and Column Level access control are only supported on Shared clusters for now (will soon be available everywhere).

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 1.2 This demo uses groups to showcase fine grained access control.
-- MAGIC
-- MAGIC To see the desired results for this demo, this notebook assumes that the user 
-- MAGIC -  __is__ a member of groups `ANALYST_USA` and `region_admin_SPAIN`
-- MAGIC -  is __not__ a member of groups `bu_admin` and `fr_analysts`
-- MAGIC
-- MAGIC If you are not a member of these groups, add yourself (or ask an admin) via workspace admin console:
-- MAGIC
-- MAGIC __Workspace settings / Identity and access / Groups__

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup

-- COMMAND ----------

-- If this request is failing, you're missing some groups. Make sure you are part of ANALYST_USA and not bu_admin!
SELECT 
  assert_true(is_account_group_member('account users'),      'You must be part of account users for this demo'),
  assert_true(is_account_group_member('ANALYST_USA'),        'You must be part of ANALYST_USA for this demo'),
  assert_true(not is_account_group_member('bu_admin'),       'You must NOT be part of bu_admin for this demo');

-- Cleanup any row-filters or masks that may have been added in previous runs of the demo
ALTER TABLE customers DROP ROW FILTER;
ALTER TABLE customers ALTER COLUMN address DROP MASK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.3 Review our 'customers' table
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
-- MAGIC ##  2. Row-level access control
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/acls/table_uc_rls.png?raw=true" width="200" style="float: right; margin-left: 20; margin-right: 20" alt="databricks-demos"/>
-- MAGIC
-- MAGIC As mentioned earlier, row-level security allows you to automatically hide rows in your table from users, based on their identity or group assignment.
-- MAGIC
-- MAGIC In this part of the demo, we will show you how you can enforce a policy where an analyst can only access data related to customers in their country.
-- MAGIC
-- MAGIC
-- MAGIC To capture the current user and check their membership to a particular group, Databricks provides you with 2 built-in functions: 
-- MAGIC - `current_user()` 
-- MAGIC - and `is_account_group_member()` respectively.
-- MAGIC

-- COMMAND ----------

-- get the current user (for informational purposes)
SELECT current_user(), is_account_group_member('account users');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1. Define the access rule
-- MAGIC
-- MAGIC To declare an access control rule, you will need to create a SQL function that returns a **boolean**.
-- MAGIC Unity Catalog will then hide the row if the function returns `False`.
-- MAGIC
-- MAGIC Inside your SQL function, you can define different conditions and implement complex logic to create this boolean return value. (e.g :  `IF(condition)-THEN(view)-ELSE`)
-- MAGIC
-- MAGIC Here, we will apply the following logic :
-- MAGIC
-- MAGIC 1. if the user is a `bu_admin` group member, then they can access data from all countries. (we will use `is_account_group_member('group_name')` we saw earlier)
-- MAGIC 2. if the user is not a `bu_admin` group member, we'll restrict access to only the rows pertaining to regions `US` as our default regions. All other customers will be hidden!
-- MAGIC
-- MAGIC Note that columns within whatever table that this function will be applied on, can also be referred to inside the function's conditions. You can do so by using parameters.

-- COMMAND ----------

-- DBTITLE 1,Create a SQL function for a simple row-filter:
CREATE OR REPLACE FUNCTION region_filter(region_param STRING) 
RETURN 
  is_account_group_member('bu_admin') or  -- bu_admin can access all regions
  region_param like "US%";                -- non bu_admin's can only access regions containing US

-- Grant access to all users to the function for the demo by making all account users owners.  Note: Don't do this in production!
GRANT ALL PRIVILEGES ON FUNCTION region_filter TO `account users`; 

-- Let's try our filter. As expected, we can access USA but not SPAIN.
SELECT region_filter('USA'), region_filter('SPAIN')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2. Apply the access rule
-- MAGIC
-- MAGIC With our rule function declared, all that's left to do is apply it on a table and see it in action!
-- MAGIC A simple `SET ROW FILTER` followed by a call to the function is all it takes.
-- MAGIC
-- MAGIC **Note: if this is failing, make sure you're using a Shared Cluster!**

-- COMMAND ----------

-- Apply access rule to customers table.
-- country will be the column sent as parameter to our SQL function (region_param)
ALTER TABLE customers SET ROW FILTER region_filter ON (country);

-- COMMAND ----------

-- DBTITLE 1,Confirm only customers in USA are visible:
SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
  FROM customers;

-- COMMAND ----------

-- We should see only USA and Canada here, unless the user is a member of bu_admin:
SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### This is working as expected! 
-- MAGIC
-- MAGIC We secured our table, and dynamically filter the results to only keep rows with country=USA.
-- MAGIC
-- MAGIC Let's drop the current filter, and demonstrate a more dynamic version.

-- COMMAND ----------

ALTER TABLE customers DROP ROW FILTER;
-- Confirming that we can once again see all countries:
SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.3 More advanced dynamic filters. 
-- MAGIC Let's imagine we have a few regional user groups defined as : `ANALYST_USA`, `ANALYST_SPAIN`, etc... and we want to use these groups to *dynamically* filter on a country value. 
-- MAGIC
-- MAGIC This can easily be done by checking the group based on the region value.

-- COMMAND ----------

-- DBTITLE 1,Create an advanced access rule
CREATE OR REPLACE FUNCTION region_filter_dynamic(country_param STRING) 
RETURN 
  is_account_group_member('bu_admin') or                           -- bu_admin can access all regions
  is_account_group_member(CONCAT('ANALYST_', country_param)); --regional admins can access only if the region (country column) matches the regional admin group suffix.
  
GRANT ALL PRIVILEGES ON FUNCTION region_filter_dynamic TO `account users`; --only for demo, don't do that in prod as everybody could change the function

-- apply the new access rule to the customers table:
ALTER TABLE customers SET ROW FILTER region_filter_dynamic ON (country);

SELECT region_filter_dynamic('USA'), region_filter_dynamic('SPAIN')

-- COMMAND ----------

-- DBTITLE 1,Check the rule. We can only access USA
-- Since our current user is a member of ANALYST_USA, now they see only USA from our query:
SELECT DISTINCT(country) FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Column-level access control

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3.1. Define the access rule (masking PII data)
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
   
GRANT ALL PRIVILEGES ON FUNCTION simple_mask TO `account users`; --only for demo, don't do that in prod as everybody could change the function

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2. Apply the access rule
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

GRANT ALL PRIVILEGES ON TABLE patient_ssn TO `account users`; --only for demo, don't do that in prod as everybody could change the function

-- COMMAND ----------

-- Populating our newly-created table using the INSERT INTO command to insert some rows:
INSERT INTO
  patient_ssn
values
  ("Jane Doe", "111-11-1111"),
  ("Joe Doe", "222-33-4444");

-- COMMAND ----------

-- MAGIC %md #### We can see in our SELECT results that SSN has been masked:

-- COMMAND ----------

SELECT * FROM patient_ssn;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3. Combine RL and CL access control
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
-- MAGIC ## 4. Change the definition of the access control rules
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
-- MAGIC ## 5. Dynamic access rules with lookup data
-- MAGIC
-- MAGIC So we've seen how through functions, Unity Catalog give us the flexibility to overwrite the definition of an access rule but also combine multiple rules on a single table to ultimately implement complex multi-dimensional access control on our data.
-- MAGIC
-- MAGIC Let's take a step further by adding an intermediate table describing a permission model. We'll use this table to lookup pre-defined mappings of users to their corresponsing data, on which we'll base the bahavior of our access control function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.1. Create the mapping data
-- MAGIC First, let's create a mapping table. This is just a simple example, you can implement more advanced table based on your requirements
-- MAGIC
-- MAGIC In an organization where we have a user group for each supported language, we went ahead and mapped in this table each of these groups to their corresponding countries.
-- MAGIC
-- MAGIC - The members of the `ANALYST_USA` are thus mapped to data for `USA` or `CANADA`, 
-- MAGIC - the members of the `ANALYST_SPAIN` are mapped to data for `SPAIN`, `MEXICO` or `ARGENTINA`,
-- MAGIC - etc
-- MAGIC
-- MAGIC In our case, our user belongs to `ANALYST_USA`.
-- MAGIC
-- MAGIC Note : You can easily create and assign your user to one of these groups using the account console if you have the permissions to do so.

-- COMMAND ----------

-- Creating a table containing the mapping between groups and countries

CREATE TABLE IF NOT EXISTS 
  map_country_group (
    identity_group STRING,
    countries ARRAY<STRING>
);

GRANT ALL PRIVILEGES ON TABLE map_country_group TO `account users`; --only for demo, don't do that in prod as everybody could change the groups

INSERT OVERWRITE 
  map_country_group (identity_group, countries) VALUES
    ('ANALYST_FR', Array("FR", "BELGIUM","CANADA","SWITZERLAND")),
    ('ANALYST_SPAIN',  Array("SPAIN","MEXICO","ARGENTINA")),
    ('ANALYST_USA', Array("USA","CANADA"));

SELECT * FROM map_country_group;

-- COMMAND ----------

-- DBTITLE 1,Query the mapping table to see of which groups our current user is a member
-- Query the map_country_group table to see how current user is mapped.
-- This should return the rows for ANALYST_USA since our user is assigned to these groups:
SELECT * FROM map_country_group 
  WHERE is_account_group_member(identity_group) ; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5.3. Define the access rule with lookup data
-- MAGIC
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

GRANT ALL PRIVILEGES ON FUNCTION region_filter_dynamic TO `account users`;

-- COMMAND ----------

-- DBTITLE 1,Let's review the results, we should only see USA and CANADA
SELECT id, creation_date, country, address, firstname, lastname, email, gender, age_group, canal, last_activity_date, churn 
  FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As expected, the current user can only see countries mapped to the ANALYST_USA group: ["USA", "CANADA"].
-- MAGIC
-- MAGIC Note that the same strategy can be applied at column level! You can add more advanced filters, getting metadata from other tables containing more advanced permission model based on your requirements!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Dissociate a rule from a table
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

-- MAGIC %md
-- MAGIC Let's go ahead and deactivate the row filter from `customers` as well!

-- COMMAND ----------

-- dropping the row filter:
ALTER TABLE customers DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md Removing the mask from the patient_ssn table:

-- COMMAND ----------

ALTER TABLE patient_ssn ALTER COLUMN ssn DROP MASK;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 7. Conclusion
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
-- MAGIC ### A note on Delta Sharing...
-- MAGIC As of now, Delta Sharing table does not work with SQL-function-based row-level security or column masks (i.e. the methods above).  
-- MAGIC To create row and column level security within a Delta Share, you'd need to use dynamic views (please see the [documentation](https://docs.databricks.com/en/data-sharing/create-share.html#add-dynamic-views-to-a-share-to-filter-rows-and-columns)).
