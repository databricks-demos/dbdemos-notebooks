-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #What are Metric Views?
-- MAGIC
-- MAGIC Metric views in Unity Catalog simplify access to key business KPIs, empowering teams across your organization to make data-driven decisions confidently.
-- MAGIC
-- MAGIC By abstracting away the underlying data complexity, metric views ensure consistent definitions and calculations so that users can focus on insights, not infrastructure. Whether you're in analytics, finance, or operations, accessing trusted metrics has never been easier.

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup $reset_all_data=false

-- COMMAND ----------

select CURRENT_CATALOG() as catalog, CURRENT_SCHEMA() as schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a Metric View with Databricks Assistant
-- MAGIC
-- MAGIC Leveraging the Assistant and UI streamlines the process, making it accessible for technical and non-technical users.
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/create_mv_assistant.gif" style="float: right" width="1200px"/>
-- MAGIC
-- MAGIC Open the source table and ask the assistant to "give me the metric views definition for this table"`. You can even open the assistant generated YAML in a metric view editor.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The YAML might look something like this:
-- MAGIC
-- MAGIC ~~~
-- MAGIC version: 0.1
-- MAGIC
-- MAGIC source: main.dbdemos_retail_c360.churn_users
-- MAGIC
-- MAGIC dimensions:
-- MAGIC   - name: Age Group
-- MAGIC     expr: age_group
-- MAGIC
-- MAGIC measures:
-- MAGIC   - name: Total Users
-- MAGIC     expr: COUNT(user_id)
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
-- MAGIC - Check the lineage tab (lineage graph)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Edit the Metric View
-- MAGIC - Click Edit. This will take you to the YAML edit page. Let's enhance the UC metric view to look like this [YAML]($churn_users_mteric_view.yaml).
-- MAGIC
-- MAGIC - Explanation of the fields:
-- MAGIC 1.  **Filters**: Filters are optional conditions, like the WHERE clause in SQL. Let’s add a filter to the UC metrics view. This filter will apply to all queries inside that view, working like a global filter.
-- MAGIC ~~~
-- MAGIC filter: last_activity_date >= '2020-01-01'
-- MAGIC ~~~
-- MAGIC 2. **Join**: A join is a way to connect two tables to combine related information. For example, we join the orders table to get order details and user info.
-- MAGIC ~~~
-- MAGIC joins:
-- MAGIC   - name: churn_orders
-- MAGIC     source: main.dbdemos_retail_c360.churn_orders
-- MAGIC     using: ["user_id"]
-- MAGIC ~~~
-- MAGIC 3. Dimension: A dimension has two things: `name: the column’s nickname` and `expr: the actual column or a formula`.
-- MAGIC ~~~
-- MAGIC  - name: Age Group
-- MAGIC    expr: age_group
-- MAGIC ~~~
-- MAGIC 4. Dimension with SQL function: We can use functions in the `expr` of dimensions.
-- MAGIC ~~~
-- MAGIC  - name: Last Activity Date
-- MAGIC    expr: date_trunc('day', last_activity_date)
-- MAGIC ~~~
-- MAGIC 5. Measure with SQL function: A measure has the following components: `Name: The alias of the column` and `Expr: The aggregate SQL expression`.
-- MAGIC ~~~
-- MAGIC  - name: Total Users
-- MAGIC    expr: COUNT(DISTINCT user_id)
-- MAGIC ~~~
-- MAGIC 6. Measure with filter: A measure-level filter that applies only to queries on this measure can be added. This filter only affects this specific measure and doesn’t change or hide any other data or numbers.
-- MAGIC ~~~
-- MAGIC  - name: Churned Users
-- MAGIC    expr: COUNT(DISTINCT user_id) FILTER (WHERE churn = 1)
-- MAGIC ~~~
-- MAGIC 7. Measure with window function: Window measures allow you to define metrics that use windowed, cumulative, or semi-additive aggregations. To configure a window function, you need to specify the following attributes: `order (required): The dimension used to determine the ordering of the window`. This must be a dimension exposed by the metric view. `semiadditive (required): Defines how the window measure should be summarized when the order field is not part of the query's group by clause`. You can choose to return either the first or last value in the sequence to enable semiadditive behavior. `range (required): Indicates the extent of the window`.
-- MAGIC ~~~
-- MAGIC  - name: Trailing 30-Day Active Users
-- MAGIC    expr: COUNT(DISTINCT user_id)
-- MAGIC    window:
-- MAGIC       - order: Last Activity Date
-- MAGIC         range: trailing 30 day
-- MAGIC         semiadditive: last
-- MAGIC ~~~
-- MAGIC 8. Measure with other measures: Here’s a simple example of making a new measure by using two existing ones. Remember, you have to use the MEASURE function to refer to the existing measures.
-- MAGIC ~~~
-- MAGIC  - name: Churn Rate Percentage
-- MAGIC    expr: (MEASURE(`Churned Users`) / MEASURE(`Total Users`)) * 100
-- MAGIC ~~~

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a Metric View with SQL
-- MAGIC We created a metric view from the catalog explorer using the AI assistant in the last steps. Now, we will create a metric view using SQL. The YAML can be provided in the SQL statement itself.

-- COMMAND ----------


-- Create a Metric View with business-friendly dimensions and measures
CREATE VIEW IF NOT EXISTS churn_users_metric_view
WITH METRICS
LANGUAGE YAML
COMMENT 'Metric view demonstration'
AS $$
version: 0.1
source: main.dbdemos_retail_c360.churn_users

filter: last_activity_date >= '2020-01-01'

joins:
  - name: churn_orders
    source: main.dbdemos_retail_c360.churn_orders
    using: ["user_id"]

dimensions:
  - name: Age Group
    expr: age_group

  - name: Canal
    expr: canal

  - name: Country
    expr: country

  - name: Order Creation Date
    expr: churn_orders.creation_date

  - name: Last Activity Date
    expr: date_trunc('day', last_activity_date)

  - name: Gender
    expr: CASE
            WHEN gender = 0 THEN 'Female'
            WHEN gender = 1 THEN 'Male'
            ELSE 'Other'
          END

measures:
  - name: Total Users
    expr: COUNT(DISTINCT user_id)

  - name: Average Age Group
    expr: AVG(age_group)

  - name: Active Users
    expr: COUNT(DISTINCT user_id) FILTER (WHERE last_activity_date > CURRENT_DATE - INTERVAL 3 YEARS)

  - name: Churned Users
    expr: COUNT(DISTINCT user_id) FILTER (WHERE churn = 1)

  - name: Churn Rate
    expr: COUNT(DISTINCT user_id) FILTER (WHERE churn = 1) / COUNT(DISTINCT user_id)

  - name: Churn Rate Percentage
    expr: (MEASURE(`Churned Users`) / MEASURE(`Total Users`)) * 100

  - name: Total Order Amount
    expr: SUM(churn_orders.amount)

  - name: Trailing 30-Day Active Users
    expr: COUNT(DISTINCT user_id)
    window:
      - order: Last Activity Date
        range: trailing 30 day
        semiadditive: last
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Certify the Metric View
-- MAGIC - Metric views can be certified for increased trust and reliability
-- MAGIC - Go to the Mteric View Overview page
-- MAGIC - Set a certification (Edit icon below the Metric View name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a Genie space with Metric View
-- MAGIC
-- MAGIC Now, we will create a Genie Space with the metric view.
-- MAGIC
-- MAGIC 1. Go to the metric view Overview in Catalog Explorer.
-- MAGIC 2. Click the `Create` button in the top right corner.
-- MAGIC 3. Click `Genie Space`.
-- MAGIC 4. The Metric View will be auto-selected. Click `Create`.
-- MAGIC 5. [Optional] Instead of creating the Genie Space from the metric view Overview page, you can go to `Genie` on the left menu and create a Genie Space after selecting the Metric View that was previously created.
-- MAGIC 6. Start interacting with Genie. Ask questions in natural language and Genie will give back results with the generated query and results from the metric view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Some sample questions
-- MAGIC
-- MAGIC - Start by clicking on `Explain the data set`
-- MAGIC - Show Average Age Group by Gender
-- MAGIC - What is the active user distribution by canal?
-- MAGIC - What is the churn rate by age group?
-- MAGIC - What is the total order amount by gender?
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create a Dashboard with Metric View
-- MAGIC
-- MAGIC We will now create a Dashboard with the metric view created in the previous steps.
-- MAGIC
-- MAGIC 1. Go to the metric view Overview in Catalog Explorer.
-- MAGIC 2. Click the `Create` button in the top right corner.
-- MAGIC 3. Click `Dashboard`.
-- MAGIC 4. [Optional] Instead of creating the Dashboard from the metric view Overview page, you can go to `Dashboards` on the left menu and create one. Go to the `Data` tab and select the Metric View that was previously created.
-- MAGIC 5. Start creating visualizations in the dashboard.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use the Dashboard AI assistant to create a visualization
-- MAGIC 1. Click on the icon to `Add a visualization`.
-- MAGIC 2. Place the box anywhere on the page.
-- MAGIC 3. In the textbox to `Ask the assistant to create a chart...` give the following input
-- MAGIC ~~~
-- MAGIC Active Users by Age Group
-- MAGIC ~~~
-- MAGIC 4. Hit the Enter button or click on `Submit`.
-- MAGIC 5. The assistant will create a visualization. Check the widget values in the right pane and click `Accept.`
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manually create a visualization
-- MAGIC 1. Click the icon to `Add a visualization`.
-- MAGIC 2. Place the box anywhere on the page.
-- MAGIC 3. On the right pane, under `Visualization`, select `Pie`.
-- MAGIC 4. Select `Churn Rate` for `Angle`. Notice that MEASURE is automatically added.
-- MAGIC 5. Under `Color`, select `Canal`.
-- MAGIC 6. This will display the pie chart for Churn Rate by Canal.
-- MAGIC 7. [Optional] Check `Title` under widget and add `Churn Rate by Canal` as title in the visualization.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise
-- MAGIC
-- MAGIC - Add some more visualizations in the dashboard
-- MAGIC - Add filters to the dashboard
-- MAGIC - Add a name to the dashboard
-- MAGIC - Save the dashboard
-- MAGIC - Publish the dashboard

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Use SQL to query the metric view
-- MAGIC
-- MAGIC Next, we will see how to interact with a metric view using SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## List all views in the schema
-- MAGIC Let's see how to find the metric views programmatically.

-- COMMAND ----------

SHOW VIEWS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Note:** isMetric is true for metric views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Describe a metric view
-- MAGIC Let's look at the metric view.

-- COMMAND ----------

describe table extended churn_users_metric_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Note:** Type is METRIC_VIEW and 'View Text' contains the entire YAML.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write a query using the metric view
-- MAGIC
-- MAGIC ### Exercise
-- MAGIC
-- MAGIC - Write a query to get Total Users per Age Group from `churn_users_metric_view`.
-- MAGIC - Some examples below show how to query a metric view using SQL.
-- MAGIC
-- MAGIC > **Hint:** Remember to use the MEASURE keyword.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Advantages of using Metric View

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Example 1: Business Language Transformation
-- MAGIC ✅ No more **complicated logic** for defined dimensions.  
-- MAGIC ✅ **Business-friendly dimension** names that everyone understands.  
-- MAGIC ✅ The **translations happen once** in the Metric View definition, not in every query.  
-- MAGIC

-- COMMAND ----------

SELECT
  `Gender`,
  MEASURE(`Active Users`)
FROM
  churn_users_metric_view
GROUP BY
  ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Example 2: Time-Based Analysis
-- MAGIC ✅ Complicated **Window functions** are no longer needed; just selecting the measure is sufficient.  
-- MAGIC ✅ No need for analysts to **remember and repeat** complex logic.  
-- MAGIC ✅ Different teams will all use the **same translations**.  

-- COMMAND ----------

SELECT
  `Last Activity Date`,
  MEASURE(`Trailing 30-Day Active Users`)
FROM
  churn_users_metric_view
GROUP BY ALL
ORDER BY `Last Activity Date` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: Start building analysis with Databricks SQL
-- MAGIC
-- MAGIC Now that we are ready for data analysis, let's see how our Data Analyst team can leverage them to run BI workloads.
-- MAGIC
-- MAGIC Jump to the [BI / Data warehousing notebook]($../03-AI-BI-data-warehousing/03.1-AI-BI-Datawarehousing) or [Go back to the introduction]($../00-churn-introduction-lakehouse)