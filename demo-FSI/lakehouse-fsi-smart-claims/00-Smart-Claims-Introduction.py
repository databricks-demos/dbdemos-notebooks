# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Insurance Data Intelligence Platform for Financial Services
# MAGIC #### Automating claims processing with the Databricks Data Intelligence Platform
# MAGIC
# MAGIC ## What is The Insurance Databricks Data Intelligence Platform for Financial Services?
# MAGIC
# MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, on any workload to always offer more engaging customer experiences driven by real time data, at the lowest cost. 
# MAGIC <br/><br/>
# MAGIC
# MAGIC <div style="width: 30%; float: left; margin: 0px 10px 0px 10px; background-color: #fafbff; padding: 15px; min-height: 230px;">
# MAGIC   <div style="text-align: center"><h3>Simple</h3></div>
# MAGIC   One single platform and governance/security layer for your data warehousing and AI to <strong>accelerate innovation</strong> and <strong>reduce risks</strong>. <br>
# MAGIC   No need to stitch together multiple solutions with disparate governance and high complexity.
# MAGIC </div>
# MAGIC <div style="width: 30%; float: left; margin: 0px 0px 0px 0px; background-color: #fafbff; padding: 15px; min-height: 230px;">
# MAGIC   <div style="text-align: center"><h3>Open</h3></div>
# MAGIC   Built on open source and open standards. You own your data and <strong>prevent vendor lock-in</strong>, with easy integration with external solution. <br>
# MAGIC   Being open also lets you share your data with any external organization, regardless of their data stack/vendor.
# MAGIC </div>
# MAGIC <div style="width: 30%; float: left; margin: 0px 10px 0px 10px; background-color: #fafbff; padding: 15px; min-height: 230px;">
# MAGIC   <div style="text-align: center"><h3>Multicloud</h3></div>
# MAGIC   Multicloud is one of the key strategic considerations for FS companies, with 88% of single-cloud FS customers are adopting a <strong>multi-cloud architecture</strong> (<a href="https://www.fstech.co.uk/fst/88_Of_FIs_Considering_Multicloud.php">reference</a>). The Lakehouse provides one consistent data platform across clouds and gives companies the ability to process your data where your need.
# MAGIC </div>
# MAGIC  
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-Smart-Claims-Introduction&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Accelerate claims processing by automating insight generation and decision process
# MAGIC
# MAGIC We'll showcase how the Lakehouse can accelerate claims resolution and reduce fraud risks for car crash.<br/>
# MAGIC In traditional flow, claims transit through an <b>operational</b> system such as Guidewire and an <b>analytic</b> system such as Databricks as shown in the diagram below. <br/>
# MAGIC
# MAGIC Here is the application we will implement:
# MAGIC
# MAGIC <img width="800px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-diagram.png?raw=true" />
# MAGIC
# MAGIC We will consume informations from our main operational system:
# MAGIC
# MAGIC - Customer profile information including their policy details
# MAGIC - Customer Telematics: an app that collects driving metrics (location, speed, how you turn, braking habits...)
# MAGIC - Claims details: contains the claim information, including damage photos (car crash).
# MAGIC
# MAGIC This information is then anlyzed and returned to our operational system to act on the claim:
# MAGIC - Claims images are analyzed with AI to ensure consistency with declaration
# MAGIC - Rule engine review the claims and customer profile, providing recommendation and taking automated actions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Typical Challenges in Claims Processing:
# MAGIC   * Insurance companies have to constantly innovate to beat competition
# MAGIC   * Customer Retention & Loyalty can be a challenge as people are always shopping for more competitive rates leading to churn
# MAGIC   * Fraudulent transactions can erode profit margins 
# MAGIC   * Processing Claims can be very time consuming 
# MAGIC <br>
# MAGIC ### Required Solution: 
# MAGIC How to improve the Claims Management process for <b>faster</b> claims settlement, <b>lower</b> claims processing costs, and <b>quicker</b> identification of possible fraud. <br><b>Smart Claims</b> demonstrates automation of key components of this process on the Lakehouse for higher operational efficiencies & to aid human investigation </b>
# MAGIC
# MAGIC ### How
# MAGIC   * How to manage operational costs so as to offer lower premiums, be competitive & yet remain profitable?
# MAGIC   * How can customer loyalty & retention be improved to reduce churn?
# MAGIC   * How to improve process efficiencies to reduce the response time to customers on the status/decision on their claims?
# MAGIC   * How can funds and resources be released in a timely manner to deserving parties?
# MAGIC   * How can suspicious activities be flagged for further investigation?
# MAGIC ### Why
# MAGIC   * Faster approvals leads to Better Customer NPS scores and Lower Operating expenses
# MAGIC   * Detecting and preventing fraudulent scenarios leads to a lower Leakage ratio
# MAGIC   * Improving customer satisfaction leads to a Lower Loss ratio
# MAGIC ### What is Claims Automation
# MAGIC   * Automating certain aspects of the claims processing pipeline to reduce dependence on human personnel especially in mundane predictable tasks
# MAGIC   * Augmenting additional info/insights to existing claims data to aid/expedite human investigation, eg. Recommend Next Best Action
# MAGIC   * Providing greater explainability of the sitution/case for better decision making in the human workflow
# MAGIC   * Serving as a sounding board to avoid human error/bias as well as providing an audit trail for personel in Claims Roles 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Smart Claims DEMO: Personas involved along the steps & High-level flow:
# MAGIC <br/>
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-flow-1.png?raw=true" width="1200px">
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
# MAGIC   <div class="badge_b"><div class="badge">1</div> Ingest various data sources to create a Claims Datalake. Curate the data and enrich it by joining it with additional information such as policy, telematics, accident and location data  </div>
# MAGIC   <div class="badge_b"><div class="badge">2</div> Build an ML model to assess the severity of damage of vehicles in accident and a Rules engine to score the case as a routine scenario that should be addressed quickly or an inconsistent one that requires further investigation</div>
# MAGIC   <div class="badge_b"><div class="badge">3</div> Visualise your business insights</div>
# MAGIC   <div class="badge_b"><div class="badge">4</div> Provide an easy and simple way to run the workflows periodically & securely share these insights with business stakeholders and claim investigators </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1: Data Engineering - Ingesting data & building our FS Data Intelligence Platform Database
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-flow-2.png?raw=true" style="float: right" width="700px">
# MAGIC
# MAGIC Our claims analysis requires huge volumes of data of all types - structured/semi/unstructured - coming at different velocities. For eg. telematic data from a vehicle from an app is both high volume & high velocity and mostly structured.
# MAGIC
# MAGIC First, we need to ingest and transform claim, policy, party, and accident data sources to build our database.

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC Open the [Delta Live Table SQL Pipeline notebook]($./01-Data-Ingestion/01.1-DLT-Ingest-Policy-Claims). 
# MAGIC
# MAGIC This will create a <a dbdemos-pipeline-id="dlt-fsi-smart-claims" href="#joblist/pipelines/bf6b21bb-ff10-480c-bdae-c8c91c76d065" target="_blank">DLT Pipeline</a> running in batch or streaming.
# MAGIC
# MAGIC ### Security and Governance with Unity Catalog
# MAGIC
# MAGIC Unity Catalog provides the centralized governance of <b> all data and AI assets </b> on Databricks. 
# MAGIC
# MAGIC It not only provides for easy data discovery but also protects data from unauthorized access through ACLS at the table, row, and column level ensuring PII data remains masked even for the claims investigation officer. <br/>
# MAGIC In addition, it provides audit trails of who accessed what data which is critical in regulated industries. 
# MAGIC
# MAGIC Once our data ingested, we can simply grant access to our Data Scientist team for them to explore the data and start building models.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 2: AI - Detect the the severity of the damage in the car accident
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-flow-4.png?raw=true" style="float: right" width="700px">
# MAGIC
# MAGIC The Lakehouse offers a wide range of modeling capabilities to cater to ML practitioners' needs- from autoML to classical, deep learning, and Gen AI models, including the end-to-end lifecycle management of these models as well.
# MAGIC
# MAGIC Now that our data is ready and secured, let's create a model to predict the severity of the damage to the vehicle from the accident data. The intent here is to assess if the reported severity matches the actual damage and we can use computer vision algorithms to aid in the automation of this task.

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [02.1-Model-Training]($./02-Data-Science-ML/02.1-Model-Training) notebook to start training a model to recognize claims severity from images.
# MAGIC
# MAGIC Once our accident severity level is automatically detected as part of our ingestion pipeline, we can apply dynamic rules to start a first level of classification and Analysis. Jump to [02.3-Dynamic-Rule-Engine]($./02-Data-Science-ML/02.3-Dynamic-Rule-Engine) for more details.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3: BI - Visualize Business Insights
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/dashboard-smart-claims-1.png?raw=true" width="400px" style="float: right; margin-right: 50px">
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/dashboard-smart-claims-2.png?raw=true" width="400px" style="float: right; margin-right: 50px">
# MAGIC
# MAGIC <br>As a <b> Data Warehouse</b>, it provides reporting and dashboarding capabilities. The DB SQL Dashboard is a convenient place to bring together all the key metrics and insights for consumption by different stakeholders along with alerting capabilities all of which lead to a faster decisioning process. </br>
# MAGIC   * Faster approvals leads to Better Customer <b> NPS scores </b> and lower <b> Operating expenses </b>
# MAGIC   * Detecting and preventing fraudulent scenarios leads to a lower <b> Leakage ratio</b>
# MAGIC   * Improving customer satisfaction leads to lower <b>Loss ratio</b>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open the [03-BI-Data-Warehousing-Smart-Claims]($./03-BI-Data-Warehousing/03-BI-Data-Warehousing-Smart-Claims) notebook to know more about Databricks SQL Warehouse and explore your Smart Claims dashboards:  <a href="/sql/dashboards/8731d250-a786-4fab-a832-b2479d4b8c34" target="_blank">Summary Report Dashboard</a>  | <a href="/sql/dashboards/e3cde222-ab48-4217-bf78-2243aae4ff88" target="_blank">Investigation Dashboard</a> 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4: Deploying and orchestrating the full workflow
# MAGIC <br>
# MAGIC While our data pipeline is almost ready, we're missing one last step: orchestrating the full workflow in production.
# MAGIC
# MAGIC With Databricks Lakehouse, there is no need to manage an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc. <br>

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [04-Workflow-Orchestration]($./04-Workflow-Orchestration/04-Workflow-Orchestration-Smart-Claims)  notebook to schedule or <a dbdemos-workflow-id="init-job" href="#job/629612721485383/tasks" target="_blank">access your workflow</a> (data ingetion, model re-training, dashboard update etc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC <b>Databricks Lakehouse </b> combines the best characteristics of a <b>Data Lake</b> and a <b>Data Warehouse</b> and rides on the tenants of <b> open, easy and multi-cloud </b> to ensure you make the most of your infrastructure and investment in data and AI initiatives. <br> <br>
# MAGIC * It provides a <b> Unified  Architecture </b>for
# MAGIC   * All data personas to work collaboratively on a single platform contributing to a single pipeline
# MAGIC   * All big data architecture paradigms including streaming, ML, BI, DE & Ops are supported on a single platform - no need to stitch services!
# MAGIC * End to End <b> Workflow Pipelines </b> are easier to create, monitor and maintain
# MAGIC   * Multi-task Workflows accommodate multiple node types (notebooks, DLT, ML tasks, QL dashboard and support repair&run & compute sharing)
# MAGIC   * DLT pipelines offer quality constraints and faster path to flip dev workloads to production
# MAGIC   * Robust, Scalable, and fully automated via REST APIs thereby improving team agility and productivity
# MAGIC * Supports all <b> BI & AI </b> workloads
# MAGIC   * Models are created, managed with MLFlow for easy reproducibility and audibility
# MAGIC   * Parameterized Dashboards that can access all data in the Lake and can be setup in minutes
