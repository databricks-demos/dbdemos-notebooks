-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create a Dashboard with Metric View
-- MAGIC
-- MAGIC For this demo, we will create a Dashboard with the metric view created in Step 1.
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
-- MAGIC visualize how revenue changes with month
-- MAGIC ~~~
-- MAGIC 4. Hit the Enter button or click on `Submit`.
-- MAGIC 5. The assistant will create a visualization. Check the widget values in the right pane. Click on `Accept`.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manually create a visualization
-- MAGIC 1. Click the icon to `Add a visualization`.
-- MAGIC 2. Place the box anywhere on the page.
-- MAGIC 3. On the right pane, under `Visualization`, select `Pie`.
-- MAGIC 4. Select `Total Revenue` for `Angle`.
-- MAGIC 5. Under `Color`, select `Order Status Redeable`.
-- MAGIC 6. This will display the pie chart for revenue with order status.
-- MAGIC 7. [Optional] Check `Title` under widget and add `Total Revenue by Order Status` as title in the visualization.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercise
-- MAGIC
-- MAGIC - Add some more visualizations in the dashboard
-- MAGIC - Add filters to the dashboard
-- MAGIC - Add a name to the dashboard
-- MAGIC - Save the dashboard
-- MAGIC - Publish the dashboard

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additonal: Create a custom calculation
-- MAGIC Custom calculations let you define dynamic metrics and transformations without modifying dataset queries. These can be saved as new Metric Views in Unity Catalog
-- MAGIC 1. Go to the `Data` tab and click `Add data source`.
-- MAGIC 2. Select `samples.tpch.lineitem` and click `Confirm`.
-- MAGIC 3. Click on `Custom Calculation`.
-- MAGIC 3. Put `Total selling price` under `Name`.
-- MAGIC 4. Add `SUM(l_extendedprice) - SUM(l_discount)` for `Expression`.
-- MAGIC 5. Click `Create`.
-- MAGIC 6. Create another custom calculation called `Line status readable` and Expression
-- MAGIC ~~~
-- MAGIC       case 
-- MAGIC         when l_linestatus = 'O' then 'Open' 
-- MAGIC         when l_linestatus = 'P' then 'Processing' 
-- MAGIC         when l_linestatus = 'F' then 'Fulfilled' 
-- MAGIC       end
-- MAGIC ~~~
-- MAGIC 6. Go to the visualizations tab and add a new visualization.
-- MAGIC 7. Select `lineitem` for `Dataset`.
-- MAGIC 8. Create a bar chart with `MONTHLY(l_shipdate)` as X-axis and `MEASURE(Total selling price)` as Y-axis.
-- MAGIC 9. Create another bar chart visualization with `Line status readable` in the X-axis and `MEASURE(Total selling price)` in the Y-axis.
-- MAGIC
-- MAGIC ## Save the custom calculation as a Metric View in UC
-- MAGIC 1. Go to the `Data` tab and click on the three vertical dots beside `lineitem`.
-- MAGIC 2. Click `Export to Metric View`.
-- MAGIC 3. Select the catalog and schema where you want to save the Metric View and click `Create`.