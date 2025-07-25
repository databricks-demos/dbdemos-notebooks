-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Creating and Editing Metric Views in Databricks
-- MAGIC This notebook provides a step-by-step guide on creating a metric view using the Databricks Assistant and subsequently editing it using the Databricks UI.
-- MAGIC
-- MAGIC ## Contents
-- MAGIC - Introduction to Metric Views
-- MAGIC - Creating a Metric View with Databricks Assistant
-- MAGIC - Editing Metric Views via the UI
-- MAGIC  
-- MAGIC
-- MAGIC ## What are Metric Views?
-- MAGIC
-- MAGIC Metric views in Unity Catalog simplify access to key business KPIs, empowering teams across your organization to make data-driven decisions with confidence.
-- MAGIC
-- MAGIC By abstracting away the underlying data complexity, metric views ensure consistent definitions and calculations, so users can focus on insights, not infrastructure. Whether you're in analytics, finance, or operations, accessing trusted metrics has never been easier.
-- MAGIC
-- MAGIC Leveraging the Assistant and UI streamlines the process, making it accessible for both technical and non-technical users.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Metric View with Databricks Assistant
-- MAGIC
-- MAGIC For this demo, we are going to use the `samples.tpch` catalog that is provided to all Databricks customers.
-- MAGIC
-- MAGIC 1. Click Catalog
-- MAGIC 2. Locate the `samples` catalog
-- MAGIC 3. Click `tpch` schema
-- MAGIC 4. Open the `orders` table
-- MAGIC
-- MAGIC 5. Click on the Databricks assistant (top right corner) to get started -- UC metrics is fully integrated into the data intelligence platform, this means we can use the databricks assistant to bootstrap a UC metrics definition based on existing object (tables and views) in the Unity catalog.
-- MAGIC 6. In the chat window, ask the assistant to provide the definition of a basic / starter UC metrics view. Put the message `"give me the uc metric views definition for this table"`
-- MAGIC 7. The databricks assistant is able to provide the metric YAML definition. Click on `Open metric view editor` at the end of the message.
-- MAGIC 8. This will open a metric view editor with the prepopulated assistant-generated YAML.
-- MAGIC 9. Select a catalog and schema that you have write permissions to, and enter a metric name (like `orders_mteric_view`)
-- MAGIC 10. Click on the Create button.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The YAML might look something like this:
-- MAGIC
-- MAGIC ~~~
-- MAGIC version: 0.1
-- MAGIC
-- MAGIC source: samples.tpch.orders
-- MAGIC
-- MAGIC dimensions:
-- MAGIC   - name: Order Date
-- MAGIC     expr: o_orderdate
-- MAGIC
-- MAGIC   - name: Order Status Readable
-- MAGIC     expr: >
-- MAGIC       case 
-- MAGIC         when o_orderstatus = 'O' then 'Open' 
-- MAGIC         when o_orderstatus = 'P' then 'Processing' 
-- MAGIC         when o_orderstatus = 'F' then 'Fulfilled' 
-- MAGIC       end
-- MAGIC
-- MAGIC measures:
-- MAGIC   - name: Total Price per customer
-- MAGIC     expr: SUM(o_totalprice) / count(distinct(o_custkey))
-- MAGIC ~~~
-- MAGIC
-- MAGIC * **Measures** are aggregations that do not have a predefined aggregation level. Every measure must represent an aggregated result.
-- MAGIC
-- MAGIC * **Dimensions** are categories (like country, product, or date) used to break down your data. They can use any valid SQL formula.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore the Metric View in the Catalog Explorer
-- MAGIC - Check Measures and Dimensions
-- MAGIC - [Optional] Add a description to the metric view
-- MAGIC - [Optional] Add comments/tags 
-- MAGIC - Click the Permissions tab (UC privileges)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Edit the Metric View
-- MAGIC - Click Edit. This will take you to the YAML edit page.
-- MAGIC
-- MAGIC - Let's enhance the UC metric view to include the following:
-- MAGIC 1.  **Filters**: Filters are optional conditions, like the WHERE clause in SQL. Let’s add a filter to the UC metrics view. This filter will apply to all queries inside that view, working like a global filter.
-- MAGIC ~~~
-- MAGIC filter: o_orderdate > '1992-01-01'
-- MAGIC ~~~
-- MAGIC 2. **Join**: A join is a way to connect two tables so you can combine related information. For example, we join the orders table with the customer table to get order details along with customer info.
-- MAGIC ~~~
-- MAGIC joins:
-- MAGIC  - name: customer
-- MAGIC    source: samples.tpch.customer
-- MAGIC    on: source.o_custkey = customer.c_custkey
-- MAGIC ~~~
-- MAGIC 3. Dimension with joined data: A dimension has two things: `name: the column’s nickname` and `expr: the actual column or a formula`. In the `expr` part, we use columns from the joined table and refer to them by the alias given in the "joins" section. For example, customer.c_mktsegment.
-- MAGIC ~~~
-- MAGIC  - name: Market Segment
-- MAGIC    expr: customer.c_mktsegment
-- MAGIC ~~~
-- MAGIC 4. Dimension with SQL function: We can use functions in the `expr` of dimensions. In this example, we have used the `"date_trunc"` and `"split"` functions.
-- MAGIC ~~~
-- MAGIC  - name: Order Month
-- MAGIC    expr: date_trunc('month',source.o_orderdate)
-- MAGIC
-- MAGIC  - name: Order Priority
-- MAGIC    expr: split(o_orderpriority, '-')[1] 
-- MAGIC ~~~
-- MAGIC 5. Measure with SQL function: A measure has the following components: `Name: The alias of the column` and `Expr: The aggregate SQL expression`. 
-- MAGIC ~~~
-- MAGIC  - name: Total Revenue
-- MAGIC    expr: SUM(o_totalprice)
-- MAGIC ~~~
-- MAGIC 6. Measure with filter: A measure-level filter that applies only to queries on this measure can be added. In this example, `"Number of Fulfilled Orders"` is a value that has a filter applied to it. This filter only affects this specific value and doesn’t change or hide any other data or numbers.
-- MAGIC ~~~
-- MAGIC - name: Number of Fulfilled orders
-- MAGIC   expr: count(*)filter(where `Order Status Readable` = 'Fulfilled')
-- MAGIC ~~~
-- MAGIC 7. Measure with window function: Window measures allow you to define metrics that use windowed, cumulative, or semi-additive aggregations. To configure a window function, you need to specify the following attributes: `order (required): The dimension used to determine the ordering of the window`. This must be a dimension exposed by the metric view. `semiadditive (required): Defines how the window measure should be summarized when the order field is not part of the query's group by clause`. You can choose to return either the first or last value in the sequence to enable semiadditive behavior. `range (required): Indicates the extent of the window`. In this example, we define a window function that displays the amount from the previous month. `Order Month` is used as the dimension to determine the ordering. The `range` is set to `trailing 1 month`. If Order Month is not included in the group by fields, we ensure that the last value is shown.
-- MAGIC ~~~
-- MAGIC - name: Total Amount for previous month
-- MAGIC    expr: sum(o_totalprice)
-- MAGIC    window:
-- MAGIC      - order: Order Month
-- MAGIC        range: trailing 1 month
-- MAGIC        semiadditive: last
-- MAGIC
-- MAGIC - name: Total Amount for current month
-- MAGIC    expr: sum(o_totalprice)
-- MAGIC    window:
-- MAGIC      - order: Order Month
-- MAGIC        range: current
-- MAGIC        semiadditive: last
-- MAGIC ~~~
-- MAGIC 8. Measure with other measures: Here’s a simple example of making a new measure by using two existing ones. We create a measure that finds the difference between those two. Remember, you have to use the MEASURE function to refer to the existing measures.
-- MAGIC ~~~
-- MAGIC  - name : Change in amount
-- MAGIC    expr: MEASURE(`Total Amount for current month`) -  MEASURE(`Total Amount for previous month`)
-- MAGIC ~~~

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The YAML will now look like this:
-- MAGIC ~~~
-- MAGIC version: 0.1
-- MAGIC
-- MAGIC source: samples.tpch.orders
-- MAGIC
-- MAGIC filter: o_orderdate > '1992-01-01'
-- MAGIC
-- MAGIC joins:
-- MAGIC  - name: customer
-- MAGIC    source: samples.tpch.customer
-- MAGIC    on: source.o_custkey = customer.c_custkey
-- MAGIC
-- MAGIC dimensions:
-- MAGIC   - name: Order Date
-- MAGIC     expr: o_orderdate
-- MAGIC
-- MAGIC   - name: Market Segment
-- MAGIC     expr: customer.c_mktsegment
-- MAGIC
-- MAGIC   - name: Order Status Readable
-- MAGIC     expr: >
-- MAGIC       case 
-- MAGIC         when o_orderstatus = 'O' then 'Open' 
-- MAGIC         when o_orderstatus = 'P' then 'Processing' 
-- MAGIC         when o_orderstatus = 'F' then 'Fulfilled' 
-- MAGIC       end
-- MAGIC
-- MAGIC   - name: Order Month
-- MAGIC     expr: date_trunc('month',source.o_orderdate)
-- MAGIC
-- MAGIC   - name: Order Priority
-- MAGIC     expr: split(o_orderpriority, '-')[1] 
-- MAGIC
-- MAGIC measures:
-- MAGIC   - name: Total Price per customer
-- MAGIC     expr: SUM(o_totalprice) / count(distinct(o_custkey))
-- MAGIC   
-- MAGIC   - name: Total Revenue
-- MAGIC     expr: SUM(o_totalprice)
-- MAGIC
-- MAGIC   - name: Number of Fulfilled orders
-- MAGIC     expr: count(*)filter(where `Order Status Readable` = 'Fulfilled')
-- MAGIC
-- MAGIC   - name: Total Amount for previous month
-- MAGIC     expr: sum(o_totalprice)
-- MAGIC     window:
-- MAGIC      - order: Order Month
-- MAGIC        range: trailing 1 month
-- MAGIC        semiadditive: last
-- MAGIC
-- MAGIC   - name: Total Amount for current month
-- MAGIC     expr: sum(o_totalprice)
-- MAGIC     window:
-- MAGIC      - order: Order Month
-- MAGIC        range: current
-- MAGIC        semiadditive: last
-- MAGIC   
-- MAGIC   - name : Change in amount
-- MAGIC     expr: MEASURE(`Total Amount for current month`) -  MEASURE(`Total Amount for previous month`)
-- MAGIC ~~~
-- MAGIC - Save this.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Certify the Metric View
-- MAGIC - Metric views can be certified for increased trust and reliability 
-- MAGIC - Go to the Mteric View Overview page
-- MAGIC - Set a certification (Edit icon below the Metric View name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Advantages of using Metric View

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'main';
CREATE WIDGET TEXT schema DEFAULT 'default';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Example 1: Business Language Transformation
-- MAGIC - Before: Gold Tables with Repetitive CASE Statements
-- MAGIC - After: Metric Views with Built-in Business Translations

-- COMMAND ----------

-- WITHOUT METRIC VIEWS: Complex query with repetitive translations

SELECT 
  -- Repetitive translation of codes to business terms
  CASE 
    WHEN o_orderstatus = 'O' THEN 'Open'
    WHEN o_orderstatus = 'P' THEN 'Processing'
    WHEN o_orderstatus = 'F' THEN 'Fulfilled'
    ELSE 'Unknown'
  END AS `Order Status Readable`,
  -- Definition of a Total Price per customer
  SUM(o_totalprice) / count(distinct(o_custkey)) AS `Total Price per customer`
FROM samples.tpch.orders
GROUP BY ALL;

-- 1. Every analyst has to remember how to translate o_orderstatus
-- 2. Different analysts might translate them differently, causing confusion
-- 3. These translations need to be repeated in every query
-- 4. Definitions of Total Price per customer might vary from one analysis to another

-- COMMAND ----------

-- WITH METRIC VIEWS: Clean, business-friendly query

SELECT
  `Order Status Readable`,
  MEASURE(`Total Price per customer`)
FROM
  IDENTIFIER(:catalog || '.' || :schema || '.orders_metric_view')
GROUP BY
  ALL;

-- 1. No more CASE statements for device_type or cust_segment
-- 2. Business-friendly dimension names that everyone understands
-- 3. The translations happen ONCE in the Metric View definition, not in every query
-- 4. Different teams will all use the same translations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Example 2: Time-Based Analysis
-- MAGIC - Before: Complex Window Functions Required
-- MAGIC - After: Metric Views with Built-in Time Windows

-- COMMAND ----------

-- WITHOUT METRIC VIEWS: Complex window functions required

SELECT
  date_trunc('month', o_orderdate) AS `Order Month`,
  LAG(SUM(o_totalprice)) OVER (
      ORDER BY date_trunc('month', o_orderdate)
    ) AS previous_month_total_amount
FROM
  samples.tpch.orders
WHERE
  date_trunc('month', o_orderdate) > '1998-01-01T00:00:00.000+00:00'
GROUP BY
  date_trunc('month', o_orderdate)
ORDER BY
  `Order Month`;

-- 1. Window functions like this require SQL expertise
-- 2. Business users typically don't know how to write these
-- 3. Each analyst might implement this differently

-- COMMAND ----------

-- WITH METRIC VIEWS: Clean, business-friendly query

SELECT
  `Order Month`,
  MEASURE(`Total Amount for previous month`)
FROM
  IDENTIFIER(:catalog || '.' || :schema || '.orders_metric_view')
WHERE `Order Month` > '1998-01-01T00:00:00.000+00:00'
GROUP BY ALL
ORDER BY `Order Month`;

-- 1. Just selecting the measure is sufficient
-- 2. Business-friendly dimension names that everyone understands