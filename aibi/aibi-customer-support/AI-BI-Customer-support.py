# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks AI/BI — Making Customer Support More Efficient with AI
# MAGIC
# MAGIC [Databricks AI/BI](https://www.youtube.com/watch?v=5ctfW6Ac0Ws), part of the Databricks Platform, is a new type of business intelligence product built to democratize analytics and insights for anyone in your organization - technical or nontechnical.
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
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=aibi&notebook=AI-BI-Customer-support&demo_name=aibi-customer-support&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC # The story: "Travel Co" turns support into a competitive advantage with AI
# MAGIC ---
# MAGIC
# MAGIC ## The Challenge
# MAGIC
# MAGIC **Travel Co** is a global travel company. Its support team handles thousands of cases every week — booking issues, billing questions, outages — across **4 regions** and dozens of **travel products**. Like most support orgs, costs were climbing, resolution times were too long, and satisfaction was flat. Leadership wanted one question answered: _can AI actually make support better, and can we prove it in dollars?_
# MAGIC
# MAGIC ## The Solution: an AI Support Copilot, measured end-to-end on Databricks
# MAGIC
# MAGIC On **June 2nd, 2025**, Travel Co launched an **AI Support Copilot built with [Agent Bricks](https://www.databricks.com/product/artificial-intelligence)**. It **auto-resolves** the most common ticket categories — *How-To, Access and Billing* — end to end, freeing human agents to focus on complex *Outage, Bug and Performance* cases.
# MAGIC
# MAGIC The impact was immediate and measurable. After the launch:
# MAGIC
# MAGIC | Metric | Before AI | After AI |
# MAGIC |---|---|---|
# MAGIC | ⏱️ Avg resolution time | ~28 h | **~12 h** |
# MAGIC | 😀 Customer satisfaction | ~3.6 / 5 | **~4.2 / 5** |
# MAGIC | 💸 Cost per case | ~$2,000 | **~$875** |
# MAGIC | 🤖 Cases auto-resolved by AI | 0% | **up to ~40%** |
# MAGIC
# MAGIC ## Why this is a great AI/BI demo
# MAGIC
# MAGIC The magic isn't just *seeing* the improvement on a **Dashboard** — it's being able to ask **Genie** _"why did our resolution time drop in 2025?"_ and have it trace the answer to **another table**: the AI Copilot's release log and its daily usage that jumped exactly when support got faster. That's the closed loop: **observe → ask why → act**, all in natural language on governed data.
# MAGIC
# MAGIC ## The data (pre-loaded for you)
# MAGIC
# MAGIC The demo ships a clean, governed schema with full lineage (`bronze → silver → enriched → metric view`):
# MAGIC
# MAGIC - **`support_cases`** — the fact table: every support case with resolution time, cost, satisfaction, channel, priority, and whether the AI handled it.
# MAGIC - **`ai_assistant_releases`** — the AI Copilot release log _(the "why": what each version does)_.
# MAGIC - **`ai_assistant_usage`** — daily AI queries & deflections _(rises in lockstep with the improvement)_.
# MAGIC - **`customers`, `products`, `cities`, `regions`, `calendar`** — dimensions to slice by segment, product, destination and time.
# MAGIC - **`support_cases_enriched`** — analysis-ready join, and **`support_metrics`** — a governed UC metric view powering the dashboard & Genie.
# MAGIC
# MAGIC This notebook walks you through:
# MAGIC 1. Ingesting data with **Lakeflow Connect**
# MAGIC 2. Governing everything with **Unity Catalog**
# MAGIC 3. Seeing the AI impact on an **AI/BI Dashboard**
# MAGIC 4. Asking **Genie** _why_ — and getting the answer from the data

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
# MAGIC Travel Co's support data lives across several systems — a ticketing tool, a CRM, the AI assistant's telemetry. The first step is to ingest and centralize it.
# MAGIC
# MAGIC Databricks makes this super simple with **LakeFlow Connect**, a **point-and-click data ingestion solution** supporting:
# MAGIC
# MAGIC - Databases -- including SQL Servers and more.
# MAGIC - Enterprise applications such as Salesforce, Workday, ServiceNow or Zendesk.
# MAGIC
# MAGIC Once landed as raw (bronze) data, **Lakeflow Pipelines** clean and join it into the governed `silver` tables and the `support_cases_enriched` table this demo runs on — giving you full **lineage** from raw files to dashboard.
# MAGIC
# MAGIC To learn more, open the [Lakeflow Connect Product Tour](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo) or the [Lakeflow Pipelines Product Tour](https://www.databricks.com/resources/demos/tours/data-engineering/delta-live-tables).
# MAGIC
# MAGIC _In this demo, the data is pre-loaded and transformed for you — explore the tables in the Catalog to see the bronze → silver → enriched lineage._

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 2: Ensure data governance and security coverage with Databricks Unity Catalog
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_uc.png" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Support data is sensitive — customer details, call transcripts, costs. Once ingested, **Databricks Unity Catalog** governs all of it with:
# MAGIC
# MAGIC - **Fine-grained access control**: control who can access which row or column
# MAGIC - **Full lineage, from raw files to AI models**: trace any downstream impact for legal / privacy requirements (you'll see the `bronze → silver → support_cases_enriched` lineage right in the Catalog)
# MAGIC - **Audit and traceability**: analyze who did what, when
# MAGIC - **Support for everything**: tables, dashboards, ML/AI models, the `support_metrics` metric view, jobs and more — _one place to govern every team_
# MAGIC
# MAGIC Explore the data and tables in [Unity Catalog](/explore/data) and check the lineage graph on `support_cases_enriched`.
# MAGIC
# MAGIC Click [here](https://www.databricks.com/product/unity-catalog) for more information on Databricks Unity Catalog.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 3: See the AI impact on an AI/BI Dashboard
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_dashboard_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC Travel Co's support data is now ready for the team to explore. The dashboard tells the story at a glance, across two pages:
# MAGIC
# MAGIC - **Customer Support** — volume, resolution time and satisfaction, a **forecast** of human-handled cases (watch it step down at the AI launch), plus a map of support by travel destination.
# MAGIC - **AI Copilot Impact** — the before/after: resolution time and cost dropping, AI deflection ramping up, and the **estimated annual savings** — with the release log explaining *what* changed.
# MAGIC
# MAGIC Built on a governed **metric view** (`support_metrics`), every KPI stays consistent whether you slice by **region** (try APAC — it's the slowest!), category, channel or product.
# MAGIC
# MAGIC AI/BI Dashboards make it easy to build and iterate on these visualizations with natural language, and they live side-by-side with your data for instant, interactive analysis at any scale.
# MAGIC
# MAGIC Open the <a dbdemos-dashboard-id="customer-support" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">Customer Support Dashboard to see the AI efficiency story</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 4: Ask Genie *why* — and get the answer from the data
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/aibi/dbx_aibi_genie_product.gif" style="float: right; margin: 10px" width="500px">
# MAGIC
# MAGIC The dashboard shows resolution time dropping sharply in mid-2025. A support manager's natural next question is simply: **"why?"**
# MAGIC
# MAGIC With **Genie**, they just ask — in plain language — and Genie joins the right tables to answer:
# MAGIC
# MAGIC - _"Why did our average support resolution time drop in 2025?"_ → Genie finds the **AI Copilot v1.0 GA release (2025-06-02)** and shows AI deflections jumping from 0 at that exact date.
# MAGIC - _"How much did support cost per case fall after the AI Copilot launched?"_
# MAGIC - _"Which region is the slowest, and which categories does the AI auto-resolve?"_
# MAGIC
# MAGIC This is the payoff: the answer to *"why"* doesn't live in someone's head — it lives in the **`ai_assistant_releases`** and **`ai_assistant_usage`** tables, and Genie surfaces it instantly on governed data.
# MAGIC
# MAGIC Open the <a dbdemos-genie-id="customer-support" href='/genie/rooms/01ef775474091f7ba11a8a9d2075eb58' target="_blank">Customer Support Genie space to deep dive into your data</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## "I've had enough of AI/BI for now, what next?"
# MAGIC
# MAGIC We have seen how the Databricks Platform comprehensively understands your data — from ingestion and governed lineage, to a dashboard that proves the **business value of AI on support**, to natural-language Genie that explains *why* it happened.
# MAGIC
# MAGIC And the loop can keep going: the very same AI Support Copilot is built on Databricks with **Agent Bricks**. In addition, Databricks offers:
# MAGIC
# MAGIC - **Data Engineering**: Build and orchestrate sophisticated data pipelines with Lakeflow (Python & SQL)
# MAGIC - **Data Quality & Monitoring**: Ensure your data remains accurate and reliable
# MAGIC - **Comprehensive Governance**: Fine-grained access controls and tagging across every asset
# MAGIC - **State-of-the-Art Warehouse Engine**: Excellent total cost of ownership (TCO)
# MAGIC - **Agent Bricks & GenAI**: Build the kind of AI Support Copilot featured in this demo, fully hosted and governed on Databricks
# MAGIC
# MAGIC Interested in learning more? [Explore our end-to-end platform demos](https://www.databricks.com/resources/demos/tutorials?itm_data=demo_center) to see step-by-step implementations.
