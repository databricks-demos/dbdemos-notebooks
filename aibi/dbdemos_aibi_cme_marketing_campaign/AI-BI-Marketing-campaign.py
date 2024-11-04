# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Data Intelligence Platform for Marketing Campaign Analysis: AI/BI
# MAGIC
# MAGIC Databricks AI/BI is a new type of business intelligence product built to democratize analytics and insights for anyone in your organization. 
# MAGIC
# MAGIC Powered by data intelligence, AI/BI understands your unique data and business concepts by capturing signals from across your Databricks estate, continuously learning and improving to accurately answer your questions.
# MAGIC
# MAGIC AI/BI features two complementary capabilities: Dashboards and Genie. 
# MAGIC
# MAGIC - **Dashboards** provide a low-code experience to help analysts quickly build highly interactive data visualizations for their business teams using natural language
# MAGIC - **Genie** allows business users to converse with their data to ask questions and self-serve their own analytics.
# MAGIC
# MAGIC Databricks AI/BI is native to the Databricks Data Intelligence Platform, providing instant insights at massive scale while ensuring unified governance and fine-grained security are maintained across the entire organization.
# MAGIC
# MAGIC ## Marketing Campaign Analysis
# MAGIC
# MAGIC In this demo, we'll discover how organizations large and small are utilizing Databricks to democratize data access to their Marketing Campaigns Analytics, including emails click rates and other metrics.
# MAGIC
# MAGIC Using AI/BI, Analyst and Business users can quickly get insights on their data, quickly transforming your company into a Data Driven organization.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## It all starts with your data: Utilize Lakeflow Connect for Intuitive Data Ingestion
# MAGIC
# MAGIC <div style="background-color: #1B3139; color: #F9F7F4; padding: 40px; max-width: 400px; text-align: center; border-radius: 12px; box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2); float:right; margin: 0px 15px 0px 15px">
# MAGIC   <div style="font-size: 1.5em; color: #FFAB00; margin-bottom: 20px;">Data Ingestion with LakeFlow Connect</div>
# MAGIC   <p style="font-size: 1em; color: #EEEDE9; margin-bottom: 20px; line-height: 1.6;">
# MAGIC     Discover how to seamlessly ingest your data with LakeFlow Connect. Dive into our interactive tour to learn more!
# MAGIC   </p>
# MAGIC   <a href="https://app.getreprise.com/launch/BXZY58n/" target="_blank" style="background-color: #00A972; color: #F9F7F4; border: none; padding: 15px 25px; border-radius: 8px; font-size: 1em; font-weight: bold; text-decoration: none; display: inline-block; transition: background-color 0.3s ease; cursor: pointer;"
# MAGIC      onmouseover="this.style.backgroundColor='#FF3621';" onmouseout="this.style.backgroundColor='#00A972';">
# MAGIC     Open the Interactive Tour
# MAGIC   </a>
# MAGIC </div>
# MAGIC
# MAGIC As in any data project, your first step is to ingest and centralize your data to a central place.
# MAGIC
# MAGIC Databricks makes this super simple with LakeFlow connect, a **point-and-click data ingestion** supporting:
# MAGIC
# MAGIC - Databases -- including SQL Servers and more.
# MAGIC - Entreprise application such as Salesforce, Workday, Google Analytics or ServiceNow.
# MAGIC
# MAGIC If you want to know more about LakeFlow Connect and how to incrementally synchronize your external table to Databricks, you can open the [Lakeflow Connect Product Tour](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo).
# MAGIC
# MAGIC ## Simple transformation, guided by Databricks Assistant
# MAGIC
# MAGIC Once your data ingested, Databricks empower your teams to transform their data, focusing on their business requirements while Databricks Assistant empower Data Analysts:
# MAGIC
# MAGIC In this demo, we pre-loaded the data for you! For more details on how to simplify data transformation, [open the Delta Live Table Product Tour](https://www.databricks.com/resources/demos/tours/data-engineering/delta-live-tables).
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Governance and security with Unity Catalog
# MAGIC
# MAGIC <img src="https://www.databricks.com/en-website-assets/static/2a17bcb75bcbaf056598082feb010a92/22722.png" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Once your data ingested, Databricks Unity Catalog provides all the key features to support your business requirements:
# MAGIC
# MAGIC - **Fine Grained Access control on your data**: Control who can access which row or column based on your own organization
# MAGIC - **Full lineage, from data ingestion to ML models**: Analyze all downstream impact for any legal / privacy requirements
# MAGIC - **Audit and traceability**: Analyze who did what, when
# MAGIC - **Support for all data assets**: files, table, dashboard, ML/AI models, jobs: Simplify governance, support all your teams in one place
# MAGIC
# MAGIC Databricks leverages AI to automatically analyze and understand your own data, making it aware of your context.
# MAGIC
# MAGIC Explore the data and table ingested in [Unity Catalog](/explore/data)
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## AI/BI: Track your Marketing campaing KPI with Dashboards
# MAGIC
# MAGIC <img src="https://www.databricks.com/en-website-assets/static/a8e72bb9749b880d680db024692c062a/ai-bi-value-prop-1-1717716850.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Your Marketing Campaign data is now available for your Data Analyst to explore and track their main KPIs.
# MAGIC
# MAGIC AI/BI Dashboards make it easy to create and iterate on visualizations with natural language through AI-assisted authoring. 
# MAGIC
# MAGIC Dashboards offer standard data visualization capabilities including sleek charts, interactions such as cross-filtering, periodic snapshots via email, embedding and more. 
# MAGIC
# MAGIC And they live side-by-side with your data, delivering instant load and rapid interactive analysis — no matter the data or user scale.
# MAGIC
# MAGIC
# MAGIC Open the <a dbdemos-dashboard-id="warehouse-serverless-cost" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">Campain Marketing Dashboard to analyze & track main KPIs</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## AI/BI: Let business users converse with your data with Genie Space
# MAGIC
# MAGIC <img src="https://www.databricks.com/en-website-assets/static/a79b404a64dd94cb4028d31a8950b9a0/intro-genie-web1_0-1717676868.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Our data is now available as a Dashboard that our business users can open.
# MAGIC
# MAGIC However, they'll likely have extra questions or followup based on the insight they see in the dashboard, like: "What are this campaign issue exactly?" or "What is the CTR of campaign x compare to the rest?"
# MAGIC
# MAGIC Using Databricks Genie, business users can use plain english to deep dive into the data, making Data Analysis accessible to all.
# MAGIC
# MAGIC Open the <a dbdemos-genie-id="marketing-campaign" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">Campain Marketing Dashboard to analyze & track main KPIs</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Going further
# MAGIC
# MAGIC We saw how Databricks Data Intelligence Platform understand your data, simplifying the time to insight, from Data ingestion up to Dashboard with support for plain english questions.
# MAGIC
# MAGIC But that's not it, Databricks also provides advanced capabilities for:
# MAGIC
# MAGIC - Data Engineers to build and orchestrate advanced Data Pipelines (python/SQL)
# MAGIC - Data quality & monitoring
# MAGIC - Full governance and fine grained ACL, including tags
# MAGIC - State of the art Warehouse Engine, offering excellent TCO
# MAGIC - Support for ML, AI & genAI applications, hosted by Databricks
# MAGIC - GenAI capabilities to create your own agents & empower your business users even more.
# MAGIC
# MAGIC Want to know more? [Install one of our end to end Platform demos](https://www.databricks.com/resources/demos/tutorials?itm_data=demo_center) to know how to do it, step by step.
