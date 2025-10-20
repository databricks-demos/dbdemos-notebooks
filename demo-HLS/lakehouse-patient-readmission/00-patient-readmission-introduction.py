# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Data Intelligence Platform for HLS - Patient Cohorts and re-admission 
# MAGIC <br />
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-0.png" style="float: left; margin-right: 30px; margin-top:10px" width="650px" />
# MAGIC
# MAGIC ## What is The Data Intelligence Platform for HLS?
# MAGIC
# MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, on any workload at the lowest cost.
# MAGIC
# MAGIC ### 1. Simple
# MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
# MAGIC
# MAGIC ### 2. Open
# MAGIC   Open source in healthcare presents a pivotal opportunity for data ownership, prevention of vendor lock-in, and seamless integration with external solutions. By leveraging open standards, healthcare organizations gain the flexibility to share data with any external entity. This promotes interoperability, advances collaboration, and enables comprehensive data analysis, driving improved patient outcomes and operational efficiency.
# MAGIC
# MAGIC ### 3. Multicloud
# MAGIC   Adoption of a multi-cloud strategy in healthcare organizations is inevitable and integral to competitive success, delivering cost reduction, flexibility, and improved remote services.<br/>
# MAGIC  
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00-patient-readmission-introduction&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## DEMO: Predicting patient re-admissions 
# MAGIC Accurate prediction of patient readmissions holds immense value for hospitals and insurance companies:
# MAGIC
# MAGIC ***Higher Quality Care:***
# MAGIC It enables hospitals to enhance patient care by proactively identifying individuals at a higher risk of readmission. By identifying these patients early on, healthcare providers can implement targeted interventions, such as personalized discharge plans, medication adherence support, or follow-up appointments, to mitigate the likelihood of readmissions.
# MAGIC
# MAGIC  This approach not only improves patient outcomes but also reduces the burden on healthcare facilities and optimizes resource allocation.
# MAGIC
# MAGIC ***Cost Optimization***
# MAGIC Precise readmission prediction plays a pivotal role in cost containment for both hospitals and insurance groups. Hospital readmissions contribute significantly to healthcare expenditures, and accurate prediction can help identify patterns and risk factors associated with costly readmission cases. Developpigng proactive approach not only reduces healthcare costs but also promotes financial sustainability for hospitals and insurance providers.
# MAGIC
# MAGIC
# MAGIC Databricks offers hospitals and insurance companies unique capabilities to predict readmissions and drive value. Databricks' holistic approach empowers healthcare organizations to leverage data effectively and achieve accurate readmission predictions while saving time and resources.
# MAGIC
# MAGIC
# MAGIC ### What we will build
# MAGIC
# MAGIC To predict patient re-admissions, we'll build an end-to-end solution with the Lakehouse, leveraging data from external and internal systems: patient demographics, logitudinal health records (past visits, conditions, allergies, etc), and real-time patient admission, discharge, transofrm (ADT) information...  
# MAGIC
# MAGIC At a high level, we will implement the following flow:
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>
# MAGIC
# MAGIC
# MAGIC <div style="margin-left: 20px">
# MAGIC   <div class="badge_b"><div class="badge">1</div> Ingest all the various sources of data and create our longitudinal health records database (based on OMOP CDM) (<strong>unification of data</strong>) </div>
# MAGIC   <div class="badge_b"><div class="badge">2</div>  Secure data and grant read access to the Data Analyst and Data Science teams, including row- and column-level filtering, PHI data masking, and others (<strong>data security and control</strong>). Use the Databricks unified <strong>data lineage</strong> to understand how your data flows and is used in your organisation</div>
# MAGIC   <div class="badge_b"><div class="badge">4</div> Run BI  queries and EDA to analyze population-level trends</div>
# MAGIC   <div class="badge_b"><div class="badge">5</div>  Build ML model to <strong>predict readmission risk</strong> and deploy ML models for real-time serving</div>
# MAGIC </div>
# MAGIC <br/><br/>
# MAGIC <img width="1250px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse.png" />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1/ Ingesting and preparing the data (Data Engineering)
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-2.png" />
# MAGIC
# MAGIC
# MAGIC <br/>
# MAGIC <div style="padding-left: 420px">
# MAGIC Our first step is to ingest and clean the raw data we received so that our Data Analyst team can start running analysis on top of it.
# MAGIC
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
# MAGIC
# MAGIC ### Delta Lake
# MAGIC
# MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake table. [Delta Lake](https://delta.io) is an open storage framework for reliability and performance. <br/>
# MAGIC It provides many functionalities *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
# MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`
# MAGIC
# MAGIC ### Simplify ingestion with Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC Databricks simplifies data ingestion and transformation with Spark Declarative Pipelines by allowing SQL users to create advanced pipelines, in batch or streaming. The engine will simplify pipeline deployment and testing and reduce operational complexity, so that you can focus on your business transformation and ensure data quality.<br/>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Open the HLS patient readmission  <a dbdemos-pipeline-id="sdp-patient-readmission" href="#joblist/pipelines/ff2fd2cb-733b-4166-85ed-a34b84129a35" target="_blank">Spark Declarative Pipelines pipeline</a> or the [SQL notebook]($./01-Data-Ingestion/01.1-SDP-patient-readmission-SQL) *(python SDP notebook alternative coming soon).* <br>
# MAGIC   For more details on SDP: `dbdemos.install('pipeline-bike')` or `dbdemos.install('sdp-cdc')`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Securing data & governance (Unity Catalog)
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-6.png" />
# MAGIC
# MAGIC <br/><br/><br/>
# MAGIC <div style="padding-left: 420px">
# MAGIC
# MAGIC   Now that our data has been ingested, we can explore the catalogs and schemas created using the [Data Explorer](/explore/data/dbdemos/fsi_credit_decisioning). 
# MAGIC
# MAGIC To leverage our data assets across the entire organization, we need:
# MAGIC
# MAGIC * Fine grained ACLs for our Analysts & Data Scientists teams
# MAGIC * Lineage between all our data assets
# MAGIC * real-time PII data encryption 
# MAGIC * Audit logs
# MAGIC * Data Sharing with external organization 
# MAGIC
# MAGIC   
# MAGIC  </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open the [Unity Catalog notebook]($./02-Data-governance/02-Data-Governance-patient-readmission) to see how to setup ACL and explore lineage with the Data Explorer.
# MAGIC   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3/ Analyse patient and build cohorts dashboards (BI / Data warehousing / SQL) 
# MAGIC
# MAGIC <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-patient-dashboard.png"  style="float: right; margin: 0px 0px 10px;"/>
# MAGIC
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-3.png" />
# MAGIC  
# MAGIC <br><br>
# MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
# MAGIC
# MAGIC Data Analysts are now ready to run adhoc analysis, building custom cohort on top of the existing data. In addition, Databricks provides BI interactive queries, with low latencies & high througput, including Serverless Datawarehouses providing instant stop & start.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Open the [Clinical Data Analysis notebook]($./03-Data-Analysis-BI-Warehousing/03-Data-Analysis-BI-Warehousing-patient-readmission) to start building your cohorts or directly open the <a dbdemos-dashboard-id="patient-summary" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Patient analysis dashboard</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4/ Predict readmission risk with Data Science & Auto-ML
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-4.png" />
# MAGIC
# MAGIC <br><br>
# MAGIC Being able to run analysis on our past data already gave us a lot of insight to drive understand over-all patient risk factors.
# MAGIC
# MAGIC However, knowing re-admission risk in the past isn't enough. We now need to take it to the next level and build a predictive model to forecast risk. 
# MAGIC
# MAGIC This is where the Lakehouse value comes in. Within the same platform, anyone can start building ML model to run such analysis, including low code solution with AutoML.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Machine Learning next steps:
# MAGIC
# MAGIC * [04.1-Feature-Engineering-patient-readmission]($./04-Data-Science-ML/04.1-Feature-Engineering-patient-readmission): Open the first notebook to analyze our data and start building our model leveraging Databricks AutoML.
# MAGIC * [04.2-AutoML-patient-admission-risk]($./04-Data-Science-ML/04.2-AutoML-patient-admission-risk): Leverage AutoML to accelerate your model creation.
# MAGIC * [04.3-Batch-Scoring-patient-readmission]($./04-Data-Science-ML/04.3-Batch-Scoring-patient-readmission): score our entire dataset and save the result as a new delta table for downstream usage.
# MAGIC * [04.4-Model-Serving-patient-readmission]($./04-Data-Science-ML/04.4-Model-Serving-patient-readmission): leverage Databricks Serverless model serving to deploy instant risk evaluation and personalization.
# MAGIC * [04.5-Explainability-patient-readmission]($./04-Data-Science-ML/04.5-Explainability-patient-readmission): Explain your model and provide specialized care.
# MAGIC Extra:
# MAGIC * [04.6-EXTRA-Feature-Store-ML-patient-readmission]($./04-Data-Science-ML/04.6-EXTRA-Feature-Store-ML-patient-readmission): Discover how to leverage Databricks Feature Store to create and share Feature tables with your entire organization.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 5: Workflow orchestration for patient readmission
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-5.png" />
# MAGIC
# MAGIC Now that all our lakehouse pipeline is working, we need to orchestrate all the different steps to deploy a production-grade pipeline. Typical steps involve:
# MAGIC
# MAGIC - Incrementally consume all new data every X hours (or in realtime)
# MAGIC - Refresh our dashboards
# MAGIC - Retrain and re-deploy our models
# MAGIC
# MAGIC Databricks lakehouse provides Workflow, a complete orchestrator to build your pipeline, supporting taks with any kind of transformation.

# COMMAND ----------

# MAGIC %md 
# MAGIC Open the [05-Workflow-Orchestration-patient-readmission notebook]($./05-Workflow-Orchestration/05-Workflow-Orchestration-patient-readmission) to review how we can leverage Databricks Workflows to orchestrate our tasks and link them together.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demo, we reviewed how to build a complete data pipeline, from ingestion up to advanced Clinical Data Analyzis.
# MAGIC
# MAGIC Databricks Lakehouse is uniquely position to cover all your data use-case as a single, unify platform.
# MAGIC
# MAGIC This simplify your Data Processes and let you accelerate and focus on your core Health Care business, increasing health care quality, reducing patient readmission risk and unlocking all sort of use-cases: cohort analyzis, clinical trial analysis and modeling, RWE and more.
