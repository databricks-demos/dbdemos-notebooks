-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Unity Catalog : Support for Identity Columns, Primary + Foreign Key Constraints
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/UC-FK-PK-crop.png" style="float:right; margin:10px 0px 0px 10px" width="700"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC To simplify SQL operations and support migrations from on-prem and alternative warehouse, Databricks Lakehouse now give customers convenient ways to build Entity Relationship Diagrams that are simple to maintain and evolve.
-- MAGIC 
-- MAGIC These features offer:
-- MAGIC - The ability to automatically generate auto-incrementing identify columns. Just insert data and the engine will automatically increment the ID.
-- MAGIC - Support for defining primary key
-- MAGIC - Support for defining foreign key constraints
-- MAGIC 
-- MAGIC Note that as of now, Primary Key and Foreign Key are informational only and then won’t be enforced. 
-- MAGIC 
-- MAGIC <br /><br /><br />
-- MAGIC ## Use case
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/UC-star-schema.png" style="float:right; margin:10px 0px 0px 10px" width="700"/>
-- MAGIC 
-- MAGIC Defining PK & FK helps the BI analyst to understand the entity relationships and how to join tables. It also offers more information to BI tools who can leverage this to perform further optimisation.
-- MAGIC 
-- MAGIC We'll define the following star schema:
-- MAGIC * dim_store
-- MAGIC * dim_product
-- MAGIC * dim_customer
-- MAGIC 
-- MAGIC And the fact table containing our sales information pointing to our dimension tables:
-- MAGIC 
-- MAGIC * fact_sales
-- MAGIC 
-- MAGIC Requirements:
-- MAGIC - PK/FK requires Unity Catalog enabled (Hive Metastore is not supported for FK/PK)
-- MAGIC - DBR 11.1
-- MAGIC 
-- MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Fpk_fk%2Facl&dt=FEATURE_UC_PK_KF">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC 
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC 
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

-- COMMAND ----------

-- DBTITLE 1,Environment Setup. You need the permission to create a catalog and schema in your Unity Catalog metastore.
-- MAGIC %run ./_resources/00-setup

-- COMMAND ----------

-- MAGIC %md ## 1/ Create a Dimension & Fact Tables In Unity Catalog
-- MAGIC 
-- MAGIC The first step is to create a Delta Tables in Unity Catalog (see [documentation](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)).
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support (we could have done it in python too):
-- MAGIC 
-- MAGIC 
-- MAGIC * Use the `CREATE TABLE` command
-- MAGIC * Add generated identity column with `GENERATED ALWAYS AS IDENTITY`
-- MAGIC * Define PK with `PRIMARY KEY`
-- MAGIC * Define Foreign Keys with `FOREIGN KEY REFERENCES`

-- COMMAND ----------

--STORE DIMENSION
CREATE OR REPLACE  TABLE dim_store(
  store_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  store_name STRING,
  address STRING
);

--PRODUCT DIMENSION
CREATE OR REPLACE  TABLE dim_product(
  product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku STRING,
  description STRING,
  category STRING
);

--CUSTOMER DIMENSION
CREATE OR REPLACE  TABLE dim_customer(
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 10) PRIMARY KEY,
  customer_name STRING,
  customer_profile STRING,
  address STRING
);

CREATE OR REPLACE TABLE fact_sales(
  sales_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_id BIGINT NOT NULL CONSTRAINT dim_product_fk FOREIGN KEY REFERENCES dim_product,
  store_id BIGINT NOT NULL CONSTRAINT dim_store_fk FOREIGN KEY REFERENCES dim_store,
  customer_id BIGINT NOT NULL CONSTRAINT dim_customer_fk FOREIGN KEY REFERENCES dim_customer,
  price_sold DOUBLE,
  units_sold INT,
  dollar_cost DOUBLE
);


-- COMMAND ----------

-- MAGIC %md ## 2/ Let's look at the table definition for DIM_CUSTOMER
-- MAGIC 
-- MAGIC The first step is to run DESCRIBE TABLE EXTENDED
-- MAGIC 
-- MAGIC Constraints are shown at the bottom of the results:
-- MAGIC 
-- MAGIC 
-- MAGIC | col_name       | data_type                | 
-- MAGIC |----------------|--------------------------|
-- MAGIC | #  Constraints |                          |
-- MAGIC | dim_customer_pk    | PRIMARY KEY (`customer_id`) |

-- COMMAND ----------

DESCRIBE TABLE EXTENDED dim_customer;

-- COMMAND ----------

-- MAGIC %md ## 3/ Let's add some data to the Dimension Tables
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 
-- MAGIC * Use the `INSERT INTO` command to insert some rows in the table
-- MAGIC * Note that we don't specify the values for IDs as they'll be generated by the engine with auto-increment

-- COMMAND ----------

INSERT INTO
  dim_store (store_name, address)
VALUES
  ('City Store', '1 Main Rd, Whoville');
  
INSERT INTO
  dim_product (sku, description, category)
VALUES
  ('1000001', 'High Tops', 'Ladies Shoes'),
  ('7000003', 'Printed T', 'Ladies Fashion Tops');
  
INSERT INTO
  dim_customer (customer_name, customer_profile, address)
VALUES
  ('Al', 'Al profile', 'Databricks - Queensland Australia'),
  ('Quentin', 'REDACTED_PROFILE', 'Databricks - Paris France');

-- COMMAND ----------

-- DBTITLE 1,As you can see the ids (GENERATED ALWAYS) are automatically generated with increment:
SELECT * FROM dim_product;

-- COMMAND ----------

-- MAGIC %md ## 4/ Let's add some data to the Fact Tables
-- MAGIC 
-- MAGIC We want to do that in SQL, to show multi-language support:
-- MAGIC 1. Use the `INSERT INTO` command to insert some rows in the table

-- COMMAND ----------

INSERT INTO
  fact_sales (product_id, store_id, customer_id, price_sold, units_sold, dollar_cost)
VALUES
  (1, 1, 0, 100.99, 2, 2.99),
  (2, 1, 0, 10.99, 2, 2.99),
  (1, 1, 0, 100.99, 2, 2.99),
  (1, 1, 10, 100.99, 2, 2.99),
  (2, 1, 10, 10.99, 2, 2.99);

-- COMMAND ----------

-- MAGIC %md ### Query the tables joining data
-- MAGIC 
-- MAGIC We can now imply query the tables to retrieve our data based on the FK:

-- COMMAND ----------

SELECT * FROM fact_sales
  INNER JOIN dim_product  USING (product_id)
  INNER JOIN dim_customer USING (customer_id)
  INNER JOIN dim_store    USING (store_id)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 5/ Primary Key and Foreign Key in Data Explorer
-- MAGIC 
-- MAGIC <br />
-- MAGIC 
-- MAGIC <img src="https://github.com/althrussell/databricks-demo/raw/main/product-demos/pkfk/images/data_explorer.gif" style="float:right; margin-left:100px" width="700"/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## 6/ Primary Key and Foreign Key in DBSQL - Code Completion
-- MAGIC 
-- MAGIC <br />
-- MAGIC 
-- MAGIC <img src="https://github.com/althrussell/databricks-demo/raw/main/product-demos/pkfk/images/code_completion.gif" style="float:center; margin-left:100px" width="700"/>

-- COMMAND ----------

-- DBTITLE 1,Clean Up
-- MAGIC %python spark.sql(f"DROP DATABASE {catalog}.{database} CASCADE")

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Summary
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/UC-FK-PK.png" style="float:right; margin-left:10px" width="900"/>
-- MAGIC 
-- MAGIC As you have seen Primary Keys and Foreign Keys help the BI analyst to understand the entity relationships and how to join tables and even better having code completion do the joins for you.  
-- MAGIC 
-- MAGIC The best Datawarehouse is a Lakehouse!
-- MAGIC 
-- MAGIC Next Steps:
-- MAGIC - Try DBSQL query & dashboard editors
-- MAGIC - Plug your BI tools (Tableau, PowerBI ...) to query these tables directly!
