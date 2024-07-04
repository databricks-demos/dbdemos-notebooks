# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Your Lakehouse is the best Warehouse
# MAGIC
# MAGIC Traditional Data Warehouses can’t keep up with the variety of data and use cases. Business agility requires reliable, real-time data, with insight from ML models.
# MAGIC
# MAGIC Working with the lakehouse unlock traditional BI analysis but also real time applications having a direct connection to your entire data, while remaining fully secured.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/dbsql.png" width="700px" style="float: left" />
# MAGIC
# MAGIC <div style="float: left; margin-top: 240px; font-size: 23px">
# MAGIC   Instant, elastic compute<br>
# MAGIC   Lower TCO with Serveless<br>
# MAGIC   Zero management<br><br>
# MAGIC
# MAGIC   Governance layer - row level<br><br>
# MAGIC
# MAGIC   Your data. Your schema (star, data vault…)
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffsi%2Flakehouse_credit_scoring%2Fbi&dt=LAKEHOUSE_CREDIT_SCORING">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks SQL Warehouses: best-in-class BI engine
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
# MAGIC
# MAGIC Databricks SQL is a warehouse engine packed with thousands of optimizations to provide you with the best performance for all your tools, query types and real-world applications. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>It won the Data Warehousing Performance Record.</a>
# MAGIC
# MAGIC This includes the next-generation vectorized query engine Photon, which together with SQL warehouses, provides up to 12x better price/performance than other cloud data warehouses.
# MAGIC
# MAGIC **Serverless warehouse** provide instant, elastic SQL compute — decoupled from storage — and will automatically scale to provide unlimited concurrency without disruption, for high concurrency use cases.
# MAGIC
# MAGIC Make no compromise. Your best Datawarehouse is a Lakehouse.
# MAGIC
# MAGIC ### Creating a SQL Warehouse
# MAGIC
# MAGIC SQL Wharehouse are managed by databricks. [Creating a warehouse](/sql/warehouses) is a 1-click step: 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating your first Query
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-query.png" />
# MAGIC
# MAGIC Our users can now start running SQL queries using the SQL editor and add new visualizations.
# MAGIC
# MAGIC By leveraging auto-completion and the schema browser, we can start running adhoc queries on top of our data.
# MAGIC
# MAGIC While this is ideal for Data Analyst to start analysing our customer Churn, other personas can also leverage DBSQL to track our data ingestion pipeline, the data quality, model behavior etc.
# MAGIC
# MAGIC Open the [Queries menu](/sql/queries) to start writting your first analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC # Driving our Credit score business with Databricks DBSQL
# MAGIC
# MAGIC
# MAGIC Now that we have ingested all these various sources of data and enriched them with our ML model, we can visualize all the results integrate all the information in a single dashboard using Databricks SQL.
# MAGIC
# MAGIC  Here are the types of insights we are able to derive and display: 
# MAGIC
# MAGIC
# MAGIC * Opportunities ($) to upsell credit products to customers
# MAGIC * Risk based on alternative data sources like telco payments
# MAGIC * Loss Given Default - Risk is only a piece of the puzzle for credit decisioning. The bank or FSI needs to understand what losses are estimated given risk profiles.
# MAGIC * Explainability of credit decisions using all features and model explainability library SHAP
# MAGIC
# MAGIC <a dbdemos-dashboard-id="credit-decisioning" href='/sql/dashboardsv3/01ef3a3dcb7216b29bf2e778d084fa7c' target="_blank">Open the DBSQL dashboard</a> to explore this information. <br/><br/>
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-dashboard.png" width="800px" >

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Orchestrating with Workflow
# MAGIC
# MAGIC We're now ready to create an end to end job to deploy our entire pipeline in production, from data ingestion to model serving and DBSQL Dashboard.
# MAGIC
# MAGIC Open the [05-Workflow-Orchestration-credit-decisioning]($../05-Workflow-Orchestration/05-Workflow-Orchestration-credit-decisioning) notebook to see how Databricks can orchestrate your pipelines.
