-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create a Genie space with Metric View
-- MAGIC
-- MAGIC For this demo, we will create a Genie Space with the metric view created in Step 1.
-- MAGIC
-- MAGIC 1. Go to the metric view Overview in Catalog Explorer.
-- MAGIC 2. Click the `Create` button in the top right corner.
-- MAGIC 3. Click `Genie Space`.
-- MAGIC 4. The Metric View will be auto-selected. Click `Create`.
-- MAGIC 5. [Optonal] Instead of creating the Genie Space from the metric view Overview page, you can go to `Genie` on the left menu and create a Genie Space after selecting the Metric View that was previously created.
-- MAGIC 6. Start interacting with Genie. Ask questions in natural language and Genie will give back results with the generated query and results from the metric view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Some sample questions
-- MAGIC
-- MAGIC - Start by clicking on `Explain the data set`
-- MAGIC - Which market segments are generating the most revenue?
-- MAGIC - How is monthly order revenue trending?
-- MAGIC - Which order priorities result in higher revenue?
-- MAGIC - What is the impact of order status on revenue?
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Limitation:** Currently, in a Genie Space, you cannot select both a metric view and a regular table. Support for this is on the roadmap.

-- COMMAND ----------

pie chart to visualize total revenue by order status
visualize how revenue changes with month