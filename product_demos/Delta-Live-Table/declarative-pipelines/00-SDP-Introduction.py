# Databricks notebook source
# MAGIC %md
# MAGIC ## Intro to Spark Declarative Pipelines
# MAGIC
# MAGIC Declarative Pipelines simplify batch and streaming ETL with automated reliability and built-in data quality. Let's give it a try!
# MAGIC
# MAGIC ## Optimizing our bike rental business - ETL pipeline
# MAGIC Our fictional company operates bike rental stations across the city. The primary goal of this data pipeline is to transform raw operational data—such as ride logs, maintenance records, and weather information—into a structured and refined format, enabling comprehensive analytics. <br/>
# MAGIC This allows us to track key business metrics like total revenue, forecast future earnings, understand revenue contributions from members versus non-members, analyze customer behavior and lifetime value, and crucially, identify and quantify revenue loss due to maintenance issues. 
# MAGIC
# MAGIC By providing these insights, the pipeline empowers us to optimize operations, improve bike availability, and ultimately maximize profitability.
# MAGIC
# MAGIC We'll be using as input a raw dataset containing information coming from our ride tracking system as well as data from our maintenence system, weather data, and customer CDC events. Our goal is to ingest this data in near real time and build table for our analyst team while ensuring data quality.
# MAGIC
# MAGIC ### Getting started with the new pipeline editor
# MAGIC Databricks provides a [rich editor](https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-0.png?raw=true) to help you build and navigate through your different pipeline steps! 
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=00-Lakeflow-Declarative-Pipeline-Introduction&demo_name=declarative-pipelines&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1/ Exploring the data
# MAGIC First, open the [notebook in the Exploration folder]($./1-sdp-sql/explorations/01-Exploring-the-Data) to discover our dataset.
# MAGIC
# MAGIC We'll consume data from 4 sources, all available to us as raw CSV or JSON file in our schema volume:
# MAGIC
# MAGIC - **maintenance_logs** (all the maintenance details, as csv files)
# MAGIC - **rides** (the ride informations, including comments from users using the mobile application)
# MAGIC - **weather** (current and forecast, as JSON file)
# MAGIC - **customers** (customer CDC data for Auto CDC processing, as parquet files)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2/ Get started with Streaming Tables and Materialized view
# MAGIC
# MAGIC Creating your pipeline is super simple! If you're new to the Declarative Pipelines, it's best to start with the [UI introduction from the documentation](https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor)!
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Your Spark Declarative Pipelines has been installed and started for you!** Open the <a dbdemos-pipeline-id="pipeline-bike" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Bike Rental Declarative Pipeline</a> to see it in action.<br/>
# MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3/ Ingesting and transforming your data
# MAGIC
# MAGIC Now that we reviewed the data available to us, it's time to start creating our pipeline! We'll do it one step at a time.
# MAGIC
# MAGIC Open the [01-pipeline-tutorial notebook]($./01-pipeline-tutorial) if you want to start with the basics behind Streaming Table and Materialized View.
# MAGIC
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC       <b>Bronze: Raw data ingested into Delta tables.</b>
# MAGIC       Our bronze layer contains our raw data loaded with minimal schema changes into tables using Autoloader.  
# MAGIC
# MAGIC Tables in our bronze layer:
# MAGIC - maintenance_logs_raw
# MAGIC - rides_raw
# MAGIC - weather_raw
# MAGIC - customers_cdc_raw
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <b>Silver: Cleaned and enriched with data quality rules</b><br/>
# MAGIC       Filter out invalid rides and maintenance logs, enrich data with ride revenue, categorize maintenance issues, and process customer CDC events using Auto CDC for SCD Type 2 (historical tracking).<br/>
# MAGIC
# MAGIC Tables in our silver layer:
# MAGIC - maintenance_logs
# MAGIC - rides
# MAGIC - weather
# MAGIC - customers (SCD Type 2)
# MAGIC     </td>
# MAGIC     <td>
# MAGIC       <b>Gold: Curated for analytics & AI.</b><br>
# MAGIC       Aggregates data for reporting by pre-calulating how much revenue each station makes as a origin and destination as well as calculates how much revenue loss each maintenance event costs.  
# MAGIC
# MAGIC Tables:
# MAGIC
# MAGIC - maintenance_events
# MAGIC - stations
# MAGIC - bikes
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td><a href="$./transformations/01-bronze.sql"><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-1.png?raw=true" width="300px" style="width:300px;" /></a></td>
# MAGIC     <td><a href="$./transformations/01-silver.sql"><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-2.png?raw=true" width="300px" style="width:300px;" /></a></td>
# MAGIC     <td><a href="$./transformations/01-gold.sql"><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-3.png?raw=true" width="300px" style="width:300px;" /></a></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td align="center">Open <a href="$./transformations/01-bronze.sql">transformations/01-bronze.sql</a></td>
# MAGIC     <td align="center">Open <a href="$./transformations/01-silver.sql">transformations/01-silver.sql</a></td>
# MAGIC     <td align="center">Open <a href="$./transformations/01-gold.sql">transformations/01-gold.sql</a></td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4/ Visualizing the data with Databricks AI/BI
# MAGIC
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC     <a dbdemos-dashboard-id="bike-rental" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Business Dasbhoard</a>
# MAGIC     </td>
# MAGIC     <td>
# MAGIC     - <a  dbdemos-dashboard-id="data-quality" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Bike Rental Data Monitoring Dashboard</a>
# MAGIC     <br/>
# MAGIC     - <a dbdemos-dashboard-id="operational" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Bike Rental Pipeline Operational Dashboard</a>
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-dashboard-1.png?raw=true" width="500px" style="width:300px;" /></td>
# MAGIC     <td><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/declarative-pipelines-dashboard-2.png?raw=true" width="500px" style="width:300px;" /></td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC