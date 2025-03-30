-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Data Intelligence Platform Demo for Retail & Consumer Goods 
-- MAGIC ---
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_clean.png?raw=true" style="float: left; margin-right: 30px" width="600px" />
-- MAGIC <br/>
-- MAGIC
-- MAGIC ## What is The Databricks Data Intelligence Platform?
-- MAGIC
-- MAGIC It's the only enterprise data platform that allows seamless integration of _all_ of your relevant data systems for building models, GenAI agents, applications, dashboards, and more! Basically, Databricks allows you to **create value from your data.**
-- MAGIC
-- MAGIC Specifically for Retail & Consumer Goods, the Databricks Data Intelligence Platform allows you to execute on high-value use-cases, including but not limited to:
-- MAGIC
-- MAGIC 1. Personalized consumer engagement. 
-- MAGIC 2. Monitoring employee productivity. 
-- MAGIC 3. New product ideation.
-- MAGIC
-- MAGIC _and many more,_ giving RCG organizations utilizing Databricks a major edge in business efficiencies.
-- MAGIC <br/><br/>
-- MAGIC ---
-- MAGIC <br/><br/>
-- MAGIC ## More specifically, Databricks is...
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_logo_intelligence_engine.png?raw=true" style="float: left; margin: 5px -18px 40px -22px; clip-path: inset(0 33px 0 31px);" width="127px" />
-- MAGIC
-- MAGIC <strong>1. Intelligent</strong><br/>
-- MAGIC Databricks infuses your Lakehouse with intelligent AI. Because the platform deeply understands your data, it can auto-optimize performance and manage infrastructure for you - meaning, you can focus on optimizing your business instead.
-- MAGIC
-- MAGIC <div style="clear: both; float: left; width: 49%">
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_logo_simple.png?raw=true" style="float: left; margin: 2px 5px 40px 0;" width="80px" />
-- MAGIC     <strong>2. Simple</strong><br/>
-- MAGIC     Ask questions of your data in plain English - or in any natural language that your organization utilizes. Databricks abstracts complex data tasks so that everyone in your organization - computer whiz or total novice - can gain insights & value from that data.
-- MAGIC </div>
-- MAGIC
-- MAGIC <div style="float: right; width: 50%;">
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_logo_governance.png?raw=true" style="float: left; margin: 2px 5px 40px 0;" width="80px" />
-- MAGIC     <strong>3. Secure</strong><br/>
-- MAGIC     Provide a single layer of governance and security for all your data assets, from data pipelines to AI models, letting you build and deploy data products that are secure and trusted. Accelerate innovation while ensuring data privacy and IP protection.
-- MAGIC </div>
-- MAGIC  
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00-churn-introduction-lakehouse&demo_name=lakehouse-retail-c360&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # <span style="color: #FF5733; display: block; text-align: center;">How to Reduce Customer Churn with Databricks</span>
-- MAGIC
-- MAGIC ## The Challenge
-- MAGIC Recurring revenue businesses struggle with customer churn, impacting growth and profitability. With customer acquisition costs 5-25x higher than retention costs, companies face significant revenue loss when customers leave. The challenge is compounded by siloed data across systems - customer profiles in CRM, purchase history in ERP, and engagement metrics in web/mobile analytics platforms. Without a unified view, teams can't identify at-risk customers before they churn or understand the underlying causes driving customer departures.
-- MAGIC
-- MAGIC ## Our Solution
-- MAGIC Databricks enables cross-functional teams to build a complete Customer 360 view and deploy proactive churn reduction strategies:
-- MAGIC
-- MAGIC Our team leverages the Databricks Data Intelligence Platform to transform fragmented customer data into actionable insights:
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cross_demo_assets/Lakehouse_Demo_Team_architecture.png?raw=true" style="float: right; margin-right: 20px; width: 50%;" alt="Team Workflow"/>
-- MAGIC 1. **Data Unification**: `John`, our Data Engineer, builds Delta Live Tables pipelines to synchronize and transform data from disparate sources (CRM, ERP, web analytics, mobile app) into a unified Customer 360 database. This creates a single source of truth with real-time data ingestion capabilities.
-- MAGIC
-- MAGIC 2. **Governance & Security**: `Emily` implements Unity Catalog to ensure proper data governance, providing role-based access controls while maintaining data lineage and compliance. This enables secure collaboration across business units while protecting sensitive customer information.
-- MAGIC
-- MAGIC 3. **Advanced Analytics**: `Alice`, our BI Analyst, uses SQL Analytics to identify churn patterns, segment customers by risk level, and quantify the financial impact of retention improvements. These insights drive strategic decision-making across the organization.
-- MAGIC
-- MAGIC 4. **Predictive Intelligence**: `Marc` applies AutoML to develop custom machine learning models that predict which customers are likely to churn and identify the key factors contributing to churn risk. These models continuously improve as new data becomes available.
-- MAGIC
-- MAGIC 5. **AI-Powered Intervention**: `Liza` leverages Mosaic AI to transform predictive insights into preventative actions. Using generative AI, she creates personalized retention campaigns, tailored offers, and proactive customer service interventions that address specific churn risk factors.
-- MAGIC
-- MAGIC By connecting these capabilities in a seamless workflow, the team not only predicts which customers might leave but also understands why they're at risk and automatically triggers the most effective retention strategies for each customer segment.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **Ready to transform your customer retention strategy? Let's get started with Databricks today!**

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting and preparing the data (Data Engineering)
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 20px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_de.png?raw=true" />
-- MAGIC
-- MAGIC
-- MAGIC <br/>
-- MAGIC Our first step is to ingest and clean the raw data we received so that our Data Analyst team can start running analysis on top of it.
-- MAGIC
-- MAGIC
-- MAGIC ### Simple ingestion with Lakeflow Connect
-- MAGIC
-- MAGIC Lakeflow Connect offers built-in data ingestion connectors for popular SaaS applications, databases and file sources, such as Salesforce, Workday, and SQL Server to build incremental data pipelines at scale, fully integrated with Databricks.
-- MAGIC
-- MAGIC To give it a try, check our [Lakeflow Connect Product Tour](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo)
-- MAGIC
-- MAGIC ### Simplify ingestion with DLT
-- MAGIC
-- MAGIC Databricks simplifies data ingestion and transformation with DLT by allowing SQL users to create advanced pipelines, in batch or streaming. The engine will simplify pipeline deployment and testing and reduce operational complexity, so that you can focus on your business transformation and ensure data quality.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the customer churn 
-- MAGIC   <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">DLT pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-churn-SQL) *(Alternatives: [DLT Python version]($./01-Data-ingestion/01.3-DLT-churn-python) - [plain Delta+Spark version]($./01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-churn))*. <br>
-- MAGIC   For more details on DLT: `dbdemos.install('dlt-load')` or `dbdemos.install('dlt-cdc')`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ Securing data & governance (Unity Catalog)
-- MAGIC
-- MAGIC <img style="float: left" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_gov.png?raw=true" />
-- MAGIC
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 520px">
-- MAGIC   Now that our first tables have been created, we need to grant our Data Analyst team READ access to be able to start alayzing our Customer churn information.
-- MAGIC   
-- MAGIC   Let's see how Unity Catalog provides Security & governance across our data assets with, including data lineage and audit log.
-- MAGIC   
-- MAGIC   Note that Unity Catalog integrates Delta Sharing, an open protocol to share your data with any external organization, regardless of their stack. For more details:  `dbdemos.install('delta-sharing-airlines')`
-- MAGIC  </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-security-churn) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ Analysing churn analysis  (BI / Data warehousing / SQL) 
-- MAGIC
-- MAGIC <img width="300px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-churn.png?raw=true"  style="float: right; margin: 100px 0px 10px 10px;"/>
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC <img style="float: left" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_bi_dw.png?raw=true" />
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
-- MAGIC
-- MAGIC Data Analysts are now ready to run BI interactive queries, with low latencies & high throughput, including Serverless Data Warehouses providing instant stop & start.
-- MAGIC
-- MAGIC Let's see how we Data Warehousing can done using Databricks, including with external BI solutions like PowerBI, Tableau and other!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Data Warehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing) to start running your BI queries or access or directly open the <a dbdemos-dashboard-id="churn-universal" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Churn Analysis Dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Predict churn with Data Science & Auto-ML
-- MAGIC
-- MAGIC <img style="float: left" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_ml_ds.png?raw=true" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC Being able to run analysis on our past data already gave us a lot of insight to drive our business. We can better understand which customers are churning to evaluate the impact of churn.
-- MAGIC
-- MAGIC However, knowing that we have churn isn't enough. We now need to take it to the next level and build a predictive model to determine our customers at risk of churn and increase our revenue.
-- MAGIC
-- MAGIC This is where the Intelligence Data Platform value comes in. Within the same platform, anyone can start building ML model to run such analysis, including low code solution with AutoML.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ Transform Predictions into Action with GenAI
-- MAGIC
-- MAGIC <img style="float: left" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_4_gen_ai.png?raw=true" />
-- MAGIC
-- MAGIC
-- MAGIC Predicting which customers will churn is powerful, but it's only half the solution. The real business value comes from taking proactive, personalized action to retain those at-risk customers before they leave.
-- MAGIC
-- MAGIC This is where Liza, our Gen AI Engineer, leverages Databricks Mosaic AI to transform predictive insights into targeted interventions that drive measurable retention improvements.
-- MAGIC
-- MAGIC With the Databricks Intelligence Platform, Liza can:
-- MAGIC
-- MAGIC * **Generate personalized outreach campaigns** tailored to each customer's specific churn risk factors
-- MAGIC * **Automate contextual recommendations** for customer service teams with specific retention offers
-- MAGIC * **Create dynamic content** for email, SMS, and in-app messaging that addresses individual customer concerns
-- MAGIC * **Design intelligent conversation flows** for support teams to guide retention discussions
-- MAGIC * **Continuously optimize messaging** based on which interventions successfully prevent churn
-- MAGIC
-- MAGIC By connecting ML predictions directly to GenAI-powered interventions, we close the loop between insight and action—turning churn prediction into churn prevention and measurably improving customer lifetime value.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Automate action to reduce churn based on predictions
-- MAGIC
-- MAGIC
-- MAGIC <img style="float: right" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-churn-prediction.png?raw=true">
-- MAGIC
-- MAGIC We now have an end-to-end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
-- MAGIC
-- MAGIC - Send targeting email campaigns to the customers that are most likely to churn
-- MAGIC - Phone campaign to discuss with our customers and understand what's going on
-- MAGIC - Understand what's wrong with our line of product and fix it
-- MAGIC
-- MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
-- MAGIC
-- MAGIC ## Track churn impact over the next month and campaign impact
-- MAGIC
-- MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
-- MAGIC
-- MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to set up this pipeline end-to-end and we have potential gain of $129,914 / month!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Open the <a dbdemos-dashboard-id="churn-prediction" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Churn prediction DBSQL dashboard</a> to have a complete view of your business, including churn prediction and proactive analysis.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 6/ Deploying and orchestrating the full workflow
-- MAGIC
-- MAGIC <img style="float: left" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_5_orchestration.png?raw=true" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC
-- MAGIC With Databricks Lakehouse, no need to manage an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./06-Workflow-orchestration/06-Workflow-orchestration-churn) to schedule our pipeline (data ingetion, model re-training etc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC We demonstrated how to implement an end-to-end pipeline with the Intelligence Data Platform, using a single, unified and secured platform:
-- MAGIC
-- MAGIC - Data ingestion
-- MAGIC - Data analysis / DW / BI 
-- MAGIC - Data science / ML
-- MAGIC - AI and GenAI app
-- MAGIC - Workflow & orchestration
-- MAGIC
-- MAGIC As result, our analyst team was able to simply build a system to not only understand but also forecast future churn and take action accordingly.
-- MAGIC
-- MAGIC This was only an introduction to the Databricks Intelligence Data Platform. For more details, contact your account team and explore more demos with `dbdemos.list()`
