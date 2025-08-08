-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create a Metric View with SQL
-- MAGIC In the last demo, we created a metric view from the catalog explorer using the AI assistant. In this demo, we will create a metric view using SQL. The YAML can be provided in the SQL statement itself.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "main") ## Enter the catalog name in which the metric view will be created
-- MAGIC dbutils.widgets.text("schema", "default") ## Enter the schema name in which the metric view will be created
-- MAGIC dbutils.widgets.text("metric_view_name", "orders_metric_view_sql") ## Enter the metric view name that will be created

-- COMMAND ----------


-- Create a Metric View with business-friendly dimensions and measures
CREATE VIEW IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema || '.' || :metric_view_name)
WITH METRICS
LANGUAGE YAML
COMMENT 'Metric view demonstration'
AS $$
version: 0.1

source: samples.tpch.orders

filter: o_orderdate > '1992-01-01'

joins:
 - name: customer
   source: samples.tpch.customer
   on: source.o_custkey = customer.c_custkey

dimensions:
  - name: Order Date
    expr: o_orderdate

  - name: Market Segment
    expr: customer.c_mktsegment

  - name: Order Status Readable
    expr: >
      case 
        when o_orderstatus = 'O' then 'Open' 
        when o_orderstatus = 'P' then 'Processing' 
        when o_orderstatus = 'F' then 'Fulfilled' 
      end

  - name: Order Month
    expr: date_trunc('month',source.o_orderdate)

  - name: Order Priority
    expr: split(o_orderpriority, '-')[1] 

measures:
  - name: Total Price per customer
    expr: SUM(o_totalprice) / count(distinct(o_custkey))
  
  - name: Total Revenue
    expr: SUM(o_totalprice)

  - name: Number of Fulfilled orders
    expr: count(*)filter(where `Order Status Readable` = 'Fulfilled')

  - name: Total Amount for previous month
    expr: sum(o_totalprice)
    window:
     - order: Order Month
       range: trailing 1 month
       semiadditive: last

  - name: Total Amount for current month
    expr: sum(o_totalprice)
    window:
     - order: Order Month
       range: current
       semiadditive: last
  
  - name : Change in amount
    expr: MEASURE(`Total Amount for current month`) -  MEASURE(`Total Amount for previous month`)
$$;