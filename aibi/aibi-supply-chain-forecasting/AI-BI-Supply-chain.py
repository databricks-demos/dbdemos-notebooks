# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks AI/BI for Supply Chain Optimization
# MAGIC
# MAGIC [Databricks AI/BI](https://www.youtube.com/watch?v=5ctfW6Ac0Ws), the newest addition to the Databricks Intelligence Platform, is a new type of business intelligence product built to democratize analytics and insights for anyone in your organization - technical or nontechnical. 
# MAGIC
# MAGIC Powered by _your own organization's data,_ AI/BI understands your unique business concepts, challenges, and areas of opportunity, continuously learning and improving based on feedback - all behind a slick, nontechnical UI.
# MAGIC
# MAGIC AI/BI features two complementary capabilities: _Dashboards and Genie_. 
# MAGIC
# MAGIC - **Dashboards** provide a low-code experience to help analysts quickly build highly interactive data visualizations for their business teams using any natural language.
# MAGIC - **Genie** allows business users to converse with their data to ask questions and self-serve their own analytics.
# MAGIC
# MAGIC Databricks AI/BI is native to the Databricks Platform, providing instant insights at massive scale while ensuring unified governance and fine-grained security are maintained across the entire organization.
# MAGIC
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=aibi&notebook=AI-BI-Supply-chain-optimization&demo_name=supply-chain&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC # Supply Chain Forecasting — are we about to run out of a part?
# MAGIC ---
# MAGIC
# MAGIC ## The Story
# MAGIC
# MAGIC You run the supply chain for an **e-bike manufacturer**. Every model is built from shared components via a bill of materials — and the **Battery Cell** goes into all of them. A **new EMEA market just opened**, so City E-Bike demand is surging in Europe. That surge is quietly burning through Battery Cells: at the **Rotterdam** plant there are only **~2 weeks of cover** left — but the supplier needs **8 weeks** to deliver more. If nothing changes, the line stops.
# MAGIC
# MAGIC ## The Challenge
# MAGIC
# MAGIC Catching this in time means connecting data that usually lives apart: demand forecasts, the bill of materials, on-hand inventory by plant, supplier lead times, and the market event that started it all. Without a unified platform, by the time the pieces are stitched together the stockout has already happened.
# MAGIC
# MAGIC ## The Solution
# MAGIC
# MAGIC The **Databricks Platform** unifies all of it, then layers **AI/BI** on top. The dashboard forecasts each component's on-hand inventory and flags which one runs out first (vs its supplier lead time); **Genie** lets anyone ask *"which component is about to run out, when, and why?"* in plain language — and trace it from the demand surge, through the BOM, to the new EMEA market launch that caused it.
# MAGIC
# MAGIC ## This Notebook
# MAGIC
# MAGIC This notebook will guide you, the knowledgeable Databricks aficionado, through deploying a Databricks AI/BI project focused on demand forecasting. Follow the step-by-step process to familiarize yourself with the project. By installing this project, its dashboard and Genie Dataroom are already accessible at **these links**.
# MAGIC
# MAGIC In the following sections, this notebook will guide you through at a high-level:
# MAGIC 1. Data Ingestion with **Lakeflow Connect**
# MAGIC 2. Data Governance and Security with **Unity Catalog**
# MAGIC 3. Creating Interactive **Dashboards** with Databricks
# MAGIC 4. Utilizing **Genie** for natural language queries to analyze your supply chain data, even in complex scenarios

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1: Utilize Lakeflow Connect for Intuitive Data Ingestion
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
# MAGIC Databricks makes this super simple with LakeFlow Connect, a **point-and-click data ingestion solution** supporting:
# MAGIC
# MAGIC - Databases -- including SQL Servers and more.
# MAGIC - Enterprise applications such as Salesforce, Workday, Google Analytics, or ServiceNow.
# MAGIC
# MAGIC If you want to know more about LakeFlow Connect and how to incrementally synchronize your external table to Databricks, you can open the [Lakeflow Connect Product Tour](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo).
# MAGIC
# MAGIC
# MAGIC In this demo, we pre-loaded the data for you! For more details on how to simplify data transformation, [open the Lakeflow Pipelines Product Tour](https://www.databricks.com/resources/demos/tours/data-engineering/delta-live-tables).
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 2: Ensure data governance and security coverage with Databricks Unity Catalog
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_uc.png" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Once your data is ingested and ready to go, **Databricks Unity Catalog** provides all the key features to support your business's data governance requirements, _including but not limited to_:
# MAGIC
# MAGIC - **Fine Grained Access control on your data**: Control who can access which row or column based on your own organization
# MAGIC - **Full lineage, from data ingestion to ML models**: Analyze all downstream impact for any legal / privacy requirements
# MAGIC - **Audit and traceability**: Analyze who did what, when
# MAGIC - **Support for everything**: Including files, tables, dashboards, ML/AI models, jobs, and more! _Simplify governance, support all your teams in one place._
# MAGIC
# MAGIC Explore the data and table ingested in [Unity Catalog](/explore/data) and make sure that it looks appropriate for your organization's needs.
# MAGIC
# MAGIC Click [here](https://www.databricks.com/product/unity-catalog) for more information on Databricks Unity Catalog.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 3: Utilize Databricks Dashboards to clearly show data trends
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_dashboard_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Your unified supply chain data is now available for planners and leadership to track component risk.
# MAGIC
# MAGIC The dashboard tells the story in two pages:
# MAGIC - **Component supply risk** — weeks of cover and projected stockout per component, an **AI forecast** of on-hand inventory depleting to zero, a weeks-of-cover comparison by plant, and where demand sits on a map.
# MAGIC - **Why** — the **AI demand forecast** and the new EMEA market launch that caused the surge, the bill of materials showing every product draws on the Battery Cell, and demand by region where EMEA breaks away.
# MAGIC
# MAGIC AI/BI Dashboards make it easy to build and iterate on visualizations with natural language, with sleek charts, cross-filtering, scheduled snapshots and embedding — all side-by-side with your governed data.
# MAGIC
# MAGIC
# MAGIC Open the <a dbdemos-dashboard-id="supply-chain" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">Supply Chain Forecasting Dashboard to analyze & track main KPIs</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 4: Create Genie to allow end-users to converse with your data
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_genie_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Our data is now available as a Dashboard that our business users can open.
# MAGIC
# MAGIC However, they'll have follow-up questions based on what they see. This is where Genie shines — ask in plain language and let it trace the answer across demand, the BOM, inventory, suppliers and the market-launch event:
# MAGIC
# MAGIC - *"Which component is about to run out?"* → **Battery Cell**
# MAGIC - *"Why is Battery Cell demand surging?"* → the new **EMEA market launch** (City E-Bike)
# MAGIC - *"Why can't we just reorder more in time?"* → the supplier's **8-week lead time**
# MAGIC - *"Which products use the Battery Cell, and how many per unit?"*
# MAGIC
# MAGIC Notice the answer spans tables that usually live apart — demand, BOM, inventory, suppliers, and the market event — and Genie joins them for you.
# MAGIC
# MAGIC Open the <a dbdemos-genie-id="supply-chain" href='/genie/rooms/01ef775474091f7ba11a8a9d2075eb58' target="_blank">Supply Chain Forecasting Genie space to deep dive into your data</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## "I've had enough of AI/BI for now, what next?"
# MAGIC
# MAGIC We have seen how Databricks' Databricks Platform comprehensively understands your data, streamlining the journey from data ingestion to insightful dashboards, and supporting natural language queries.
# MAGIC
# MAGIC In addition, Databricks offers advanced capabilities, including:
# MAGIC
# MAGIC - **Data Engineering**: Build and orchestrate sophisticated data pipelines using Python and SQL
# MAGIC - **Data Quality & Monitoring**: Ensure your data remains accurate and reliable
# MAGIC - **Comprehensive Governance**: Implement fine-grained access controls and tagging
# MAGIC - **State-of-the-Art Warehouse Engine**: Achieve excellent total cost of ownership (TCO)
# MAGIC - **Support for ML, AI & GenAI Applications**: Fully hosted by Databricks
# MAGIC - **GenAI Capabilities**: Create custom agents to further empower your business users
# MAGIC
# MAGIC Interested in learning more? [Explore our end-to-end platform demos](https://www.databricks.com/resources/demos/tutorials?itm_data=demo_center) to see step-by-step implementations.
