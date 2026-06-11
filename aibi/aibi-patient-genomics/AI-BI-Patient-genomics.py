# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks AI/BI for Genomic Patient Data Review
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
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=aibi&notebook=AI-BI-HLS-patient-genomics&demo_name=patient-genomics&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC # Precision Oncology — does our new drug work, and for whom?
# MAGIC ---
# MAGIC
# MAGIC ## The Story
# MAGIC
# MAGIC A health system rolled out a new targeted cancer therapy, **OncoTarget‑1**, and is reviewing the **real-world evidence** across a real ~10,000-patient oncology cohort. Overall, treated patients survive better than standard of care — but the benefit is **wildly uneven**: a near-miracle in some cancers (melanoma, breast, brain), nothing in others, and in **ovarian and prostate cancer it may actually do harm**. The obvious follow-up: *what separates the responders from the rest?* The answer turns out to be **molecular, not anatomical** — the responders share a gene-expression signature. Within breast cancer, a responder molecular subtype reaches **~99% survival vs ~89%** on standard of care.
# MAGIC
# MAGIC ## The Challenge
# MAGIC
# MAGIC Seeing this means connecting things that usually live apart: patient demographics and survival, cancer site and diagnosis, treatment arm and outcome, and **gene-expression embeddings (UMAP)** that reveal molecular subtypes. A simple "the drug works" average hides the truth — the benefit only shows up when you slice by cancer and by molecular subtype.
# MAGIC
# MAGIC ## The Solution
# MAGIC
# MAGIC The **Databricks Platform** unifies genomic, clinical and outcome data, then layers **AI/BI** on top. The dashboard ranks every cancer by survival lift (where the drug helps, does nothing, or harms), shows the survival curves diverging over time, and maps patients by molecular subtype to reveal *who* responds. **Genie** lets clinicians and researchers ask *"which subgroup benefits most, and where should we not use this drug?"* in plain language.
# MAGIC
# MAGIC ## This Notebook
# MAGIC
# MAGIC This notebook will guide you, the knowledgeable Databricks user, through deploying a Databricks AIBI project for precision-oncology real-world evidence. By installing this project, its dashboard and Genie space are already accessible at **these links**.
# MAGIC
# MAGIC In the following sections, this notebook will guide you through at a high level:
# MAGIC 1. Data Ingestion with **Lakeflow Connect**
# MAGIC 2. Data Governance and Security with **Unity Catalog**
# MAGIC 3. Creating Interactive **Dashboards** with Databricks
# MAGIC 4. Utilizing **Genie** for natural language queries to analyze and review genomic data across patient populations

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
# MAGIC Your unified genomic, clinical and outcome data is now available for clinical and research teams to explore.
# MAGIC
# MAGIC The dashboard tells the story across two pages:
# MAGIC - **Real-world evidence** — does OncoTarget‑1 work? The headline is a **survival lift by cancer site** chart that ranks every cancer from biggest benefit (green) to *not indicated* (red) — the drug helps melanoma, breast and brain but may harm ovary and prostate. Beside it, the **gene-expression (UMAP) map** hints that responders share a molecular signature, and survival curves show treated vs standard of care diverging over time.
# MAGIC - **Who benefits most** — the deep-dive: within breast cancer, a **responder molecular subtype** (a distinct UMAP cluster) where survival climbs to ~99% vs ~89% on standard of care, plus who those responders are.
# MAGIC
# MAGIC AI/BI Dashboards make it easy to build and iterate on visualizations with natural language — sleek charts, cross‑filtering, scheduled snapshots and embedding — all side‑by‑side with your governed data.
# MAGIC
# MAGIC
# MAGIC Open the <a dbdemos-dashboard-id="patient-genomics" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">Patient Genomics Dashboard to analyze & track main KPIs</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 4: Create Genie to allow end-users to converse with your data
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_genie_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Our data is now available as a Dashboard that clinical and research teams can open.
# MAGIC
# MAGIC However, they'll have follow-up questions based on what they see. This is where Genie shines — ask in plain language and let it trace the answer across cancer site, treatment arm, outcome and molecular subtype:
# MAGIC
# MAGIC - *"Which cancers does OncoTarget‑1 help — and which is it not indicated for?"*
# MAGIC - *"Which patient subgroup benefits most?"* → the breast-cancer **responder molecular subtype**
# MAGIC - *"How does the responder subtype compare to other breast patients?"* → ~99% vs ~89% survival
# MAGIC - *"What is the survival rate by arm for ovarian and prostate cancer?"* → where the drug should *not* be used
# MAGIC
# MAGIC Open the <a dbdemos-genie-id="patient-genomics" href='/genie/rooms/01ef775474091f7ba11a8a9d2075eb58' target="_blank">Patient Genomics Genie space to deep dive into your data</a>

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
