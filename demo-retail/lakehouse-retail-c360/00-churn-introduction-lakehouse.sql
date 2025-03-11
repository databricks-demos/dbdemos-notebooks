-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Data Intelligence Platform for Retail & Consumer Goods 
-- MAGIC
-- MAGIC ### Reducing Churn with a Customer 360 platform
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

-- MAGIC %md  
-- MAGIC ## Demo: Building a C360 Database to Reduce Customer Churn with Databricks  
-- MAGIC
-- MAGIC In this demo, we'll step into the role of a retail company that sells goods through a recurring business model.  
-- MAGIC The company has identified **customer churn** as a key challenge and is focused on reducing it.  
-- MAGIC
-- MAGIC ### Objectives  
-- MAGIC We need to:  
-- MAGIC - **Analyze and explain customer churn**: quantify churn, identify trends, and assess business impact.  
-- MAGIC - **Build a proactive system** to forecast and reduce churn through automated actions, such as targeted emails and customer outreach.  
-- MAGIC
-- MAGIC ### What We'll Build  
-- MAGIC Using the **Lakehouse architecture**, we’ll develop an end-to-end solution that integrates data from multiple sources:  
-- MAGIC - **Customer profiles** from the website  
-- MAGIC - **Order details** from the ERP system  
-- MAGIC - **Clickstream data** from the mobile app to track customer activity  
-- MAGIC
-- MAGIC The implementation will follow these key steps:  
-- MAGIC 1. **Ingest and unify data** into a **C360 database** with structured tables for easy SQL queries.  
-- MAGIC 2. **Secure the data** and grant access to Data Analysts and Data Scientists.  
-- MAGIC 3. **Run BI queries** to analyze customer churn.  
-- MAGIC 4. **Train an ML model** to predict churn and understand key risk factors.  
-- MAGIC
-- MAGIC With these insights, we can trigger **automated retention strategies**, such as personalized emails, special offers, or direct outreach.  
-- MAGIC
-- MAGIC ### Our Dataset  
-- MAGIC For this demo, we'll assume an external system periodically sends data to cloud storage (S3, ADLS, GCS), including:  
-- MAGIC - **Customer profile data** (name, age, address, etc.)  
-- MAGIC - **Order history** (purchases over time)  
-- MAGIC - **App events** (last activity, clicks, streaming data)  
-- MAGIC
-- MAGIC *Note: Databricks can ingest data from virtually any source, including Salesforce, Fivetran, Kafka, blob storage, and relational or NoSQL databases.*  
-- MAGIC
-- MAGIC Now, let's explore how Databricks' **Intelligence Data Platform** can help us analyze and reduce customer churn!  

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
-- MAGIC ### Simplify ingestion with Delta Live Tables (DLT)
-- MAGIC
-- MAGIC Databricks simplifies data ingestion and transformation with Delta Live Tables by allowing SQL users to create advanced pipelines, in batch or streaming. The engine will simplify pipeline deployment and testing and reduce operational complexity, so that you can focus on your business transformation and ensure data quality.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the customer churn 
-- MAGIC   <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Delta Live Table pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-churn-SQL) *(Alternatives: [DLT Python version]($./01-Data-ingestion/01.3-DLT-churn-python) - [plain Delta+Spark version]($./01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-churn))*. <br>
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

-- MAGIC %md
-- MAGIC Let's see how to train an ML model within 1 click with the [04.1-automl-churn-prediction notebook]($./04-Data-Science-ML/04.1-automl-churn-prediction)
-- MAGIC
-- MAGIC #### Reducing churn leveraging Databricks GenAI and LLMs capabilities 
-- MAGIC
-- MAGIC GenAI provides unique capabilities to improve your customer relationship, providing better services but also better analyzing your churn risk.
-- MAGIC
-- MAGIC Databricks provides built-in GenAI capabilities for you to accelerate such GenAI apps deployment. 
-- MAGIC
-- MAGIC Discover how with the [04.4-GenAI-for-churn]($./04-Data-Science-ML/04.4-GenAI-for-churn) Notebook.

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
-- MAGIC ## 5/ Deploying and orchestrating the full workflow
-- MAGIC
-- MAGIC <img style="float: left" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/di_platform_5_orchestration.png?raw=true" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC
-- MAGIC With Databricks Lakehouse, no need to manage an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./05-Workflow-orchestration/05-Workflow-orchestration-churn) to schedule our pipeline (data ingetion, model re-training etc)

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
