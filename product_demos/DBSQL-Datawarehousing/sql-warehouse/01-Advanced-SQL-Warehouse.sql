-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # SQL Warehousing with Databricks
-- MAGIC
-- MAGIC Databricks SQL provides access to a complete set of features, making SQL workload easy and fully compatible with your existing legacy DW!
-- MAGIC
-- MAGIC In this notebook, you'll find some example with more advanced SQL features, such as loops and stored procedures!

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1/ Defining variables 
-- MAGIC
-- MAGIC Databricks SQL let you define your own variable, making it easy to parametrize your sql script.
-- MAGIC
-- MAGIC Variable typically include schema name, or any other string to execute SQL. Here is a basic example:

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE myvar INT DEFAULT 17;
SELECT myvar;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2/ Let's execute multiple statements 
-- MAGIC
-- MAGIC You can now use multi-statements in a script, to delete a record from fact_sales and insert a new record, all in one go:

-- COMMAND ----------

-- Add and delete records from fact tables multi-statement example 

BEGIN
  DELETE FROM fact_sales WHERE sales_id = 1;
  INSERT INTO fact_sales (product_id, store_id, customer_id, price_sold, units_sold, dollar_cost) VALUES (1, 1, 0, 100.99, 2, 2.99);
  SELECT * FROM fact_sales;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2/ Stored procedures with DBSQL
-- MAGIC
-- MAGIC In this example, we will create a stored procedure to calculate our revenue. This is a basic example, much more complete procedure can be created:
-- MAGIC
-- MAGIC *Disclaimer: Stored procedures might not be available yet in your workspace - ask your account team - make sure you use the latest DBR version*

-- COMMAND ----------

-- Stored procedures run only on specific DBR versions and may need to be enabled  
CREATE OR REPLACE PROCEDURE total_revenue(IN price_sold DOUBLE, IN units_sold INT, OUT revenue DOUBLE)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  SET revenue = price_sold * units_sold;
END;

-- COMMAND ----------

DECLARE revenue DOUBLE DEFAULT 0;
CALL total_revenue(100.99, 3, revenue);
SELECT revenue;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3/ Recursive loop example 

-- COMMAND ----------

WITH RECURSIVE revene_facts AS (
  SELECT 0 AS price_sold, 0 as units_sold , 0 AS revenue
  UNION ALL 
  SELECT  r.price_sold,
          r.units_sold,
          r.price_sold * r.units_sold as revenue
  FROM fact_sales AS r
)

SELECT * FROM revene_facts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4/ For loop example 

-- COMMAND ----------

-- For loop statement 
-- sum all units_sold from a given table
BEGIN
  DECLARE total_units_sold INT DEFAULT 0;
  sumNumbers: FOR row AS SELECT units_sold FROM fact_sales DO
    SET total_units_sold = total_units_sold + row.units_sold;
  END FOR sumNumbers;
  VALUES (total_units_sold);
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5/ While loop example 

-- COMMAND ----------


-- sum up all odd numbers from 1 through 10
  BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: WHILE num < 10 DO
      SET num = num + 1;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    END WHILE sumNumbers;
    VALUES (sum);
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC That's it, you're now ready to migrate your existing SQL script to Databricks!
