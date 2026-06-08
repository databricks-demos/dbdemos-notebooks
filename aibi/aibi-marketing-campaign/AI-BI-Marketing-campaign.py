# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks AI/BI for Marketing Campaign Analysis
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
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=aibi&notebook=AI-BI-Marketing-Campaign&demo_name=aibi-marketing-campaign&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Channel Marketing Campaign Analysis
# MAGIC ---
# MAGIC
# MAGIC ### The Story
# MAGIC
# MAGIC You run marketing campaigns across several channels — **TikTok, Instagram, Google Ads and Email** — targeting audiences worldwide on **mobile and web**. For most of the year everything is healthy: revenue grows, conversions are steady, every dollar of spend returns ~$5 of revenue.
# MAGIC
# MAGIC Then, from **September 2025, revenue and conversions suddenly drop — even though spend stays flat.** Something is quietly burning budget. Where is it, and why?
# MAGIC
# MAGIC ### The Challenge
# MAGIC
# MAGIC Organizations often spend **7% to 25%** of their revenue on marketing. When performance drops, the answer is usually buried across siloed channel exports, creative trackers and regional reports — and by the time analysts stitch it together, weeks of budget are already wasted.
# MAGIC
# MAGIC ### The Solution
# MAGIC
# MAGIC The **Databricks Platform** unifies every channel, creative and market in one governed place, then layers **AI/BI** on top so anyone can investigate in plain language. In this demo you'll watch the revenue drop on a dashboard, then ask **Genie** *"why did revenue drop?"* and follow the trail in seconds: the failing **campaign** (Q4 Growth Push) → the underperforming **creative** inside it (a localized *Fall Sale – v2 (DE/FR)* launched Sept 1 that barely converts) → the affected **markets** (Germany & France) → the **dollar impact**.
# MAGIC
# MAGIC ## This Notebook
# MAGIC
# MAGIC This notebook will guide you, the amazing Databricks aficionado, through deploying a Databricks AI/BI project. Feel free to follow step-by-step to get comfortable with the project. But, just know that by installing this project, its dashboard and Genie Dataroom are already available at **these links**.
# MAGIC
# MAGIC In the following sections, this notebook will guide you through at a high-level:
# MAGIC 1. Data Ingestion with **Lakeflow Connect**
# MAGIC 2. Data Governance and Security with **Unity Catalog**
# MAGIC 3. Creating Beautiful **Dashboards** with Databricks
# MAGIC 4. Utilizing **Genie** to allow plain-english (or any language) questions of your data, no matter how messy

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
# MAGIC ## Step 3: Define a governed semantic layer with Unity Catalog Metric Views
# MAGIC
# MAGIC With your source tables now governed in **Unity Catalog**, the next step is to define a reusable **semantic layer** for your key marketing KPIs using [Unity Catalog Metric Views](https://docs.databricks.com/aws/en/metric-views/). 
# MAGIC
# MAGIC **Metric Views** centralize complex business logic (such as “CTR”, or "Delivery Rate") into YAML‑defined metrics and dimensions that can be reused consistently across AI/BI Dashboards, Genie spaces, and other tools.
# MAGIC
# MAGIC By modeling measures and dimensions once and registering them in Unity Catalog, you separate **business semantics** from raw tables while still inheriting all UC governance: permissions, lineage, and auditability apply to the metric view instead of every individual dashboard or query. You can enrich these Metric Views with semantic metadata such as user‑friendly display names, formatting (currency, percentage, dates), comments, and synonyms, making the fields much easier to discover and interpret in both visualizations and natural‑language experiences, while ensuring consistency across your organization.
# MAGIC
# MAGIC > _In short: Metric Views become the shared semantic “contract” that powers both your AI/BI Dashboards and Genie spaces, ensuring everyone sees the same numbers and speaks the same business language_

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 4: Utilize Databricks Dashboards to clearly show data trends
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_dashboard_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Your multi-channel marketing data is now available for your analysts to explore and track their main KPIs.
# MAGIC
# MAGIC The dashboard tells the story in two pages:
# MAGIC - **Marketing Performance** — top-line revenue, conversions, conversion rate and spend, an **AI-forecast** of revenue-per-dollar showing the decline, channel and audience performance, and a **world map** where Germany & France clearly turn red.
# MAGIC - **Root Cause** — the dollar impact, then a top-to-bottom trail: which **campaign** is failing → which **creative** inside it → so anyone can see *why* in one scroll.
# MAGIC
# MAGIC AI/BI Dashboards make it easy to create and iterate on visualizations with natural language through AI-assisted authoring, with sleek charts, cross-filtering, scheduled snapshots and embedding — all living side-by-side with your governed data for instant, large-scale interactivity.
# MAGIC
# MAGIC
# MAGIC Open the <a dbdemos-dashboard-id="web-marketing" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">Campaign Marketing Dashboard to analyze & track main KPIs</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 5: Create Genie to allow end-users to converse with your data
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_genie_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Our data is now available as a Dashboard that our business users can open.
# MAGIC
# MAGIC However, they'll have follow-up questions based on what they see in the dashboard. This is where Genie shines — ask in plain language and let it trace the root cause for you:
# MAGIC
# MAGIC - *"Why did our marketing revenue and conversions drop in late 2025?"*
# MAGIC - *"Which campaign is underperforming since September 2025?"* → **Q4 Growth Push**
# MAGIC - *"Inside the Q4 Growth Push campaign, which creative is dragging performance down?"* → the localized **Fall Sale – v2 (DE/FR)**
# MAGIC - *"Which markets have the lowest revenue per dollar?"* → **Germany & France**
# MAGIC
# MAGIC Notice how the answer to *why* lives in a different table (`creatives`) than the symptom (`campaign_performance`) — Genie joins them for you.
# MAGIC
# MAGIC Open the <a dbdemos-genie-id="marketing-campaign" href='/genie/rooms/01ef775474091f7ba11a8a9d2075eb58' target="_blank">Campaign Marketing Genie space to deep dive into your data</a>

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
