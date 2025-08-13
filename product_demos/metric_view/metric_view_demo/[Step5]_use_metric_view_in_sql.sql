-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Write a Query using a Metric View
-- MAGIC
-- MAGIC For this demo, we will write a query using a metric view.
-- MAGIC
-- MAGIC 1. Go to the SQL editor.
-- MAGIC 4. [Optional] Use a SQL block in a notebook.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'main';
CREATE WIDGET TEXT schema DEFAULT 'default';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## List all views in the schema

-- COMMAND ----------

show views from identifier(:catalog || '.' || :schema)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Describe a metric view
-- MAGIC Let's look at the metric view created in Step 1.

-- COMMAND ----------

describe table identifier(:catalog || '.' || :schema || '.orders_metric_view');

-- COMMAND ----------

describe table extended identifier(:catalog || '.' || :schema || '.orders_metric_view');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write a query using the metric view

-- COMMAND ----------

SELECT
  `Order Month`,
  `Order Priority`,
  `Market Segment`,
  MEASURE(`Total Revenue`) as `Total Price`,
  MEASURE(`Total Amount for current month`) as `Total Amount for current month`,
  MEASURE(`Total Amount for previous month`) as `Total Amount for previous month`
FROM
  identifier(:catalog || '.' || :schema || '.orders_metric_view')
WHERE
  `Order Month` between '1992-05-01'
  and '1992-07-01'
GROUP BY
  `Order Month`,
  `Order Priority`,
  `Market Segment`
ORDER BY
  `Order Month`,
  `Order Priority`,
  `Market Segment`;