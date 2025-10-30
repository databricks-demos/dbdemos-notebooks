-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # FSI & Banking platform with Databricks Data Intelligence Platform - Fraud detection in real time
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-0.png " style="float: left; margin-right: 30px; margin-bottom: 50px" width="600px" />
-- MAGIC
-- MAGIC <br/>
-- MAGIC
-- MAGIC ## What is The Databricks Data Intelligence Platform for Banking?
-- MAGIC
-- MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, on any workload to optimize your business with real time data, at the lowest cost. 
-- MAGIC
-- MAGIC The Lakehouse allows you to centralize all your data, from customer & retail banking data to real time fraud detection, providing operational speed and efficiency at a scale never before possible.
-- MAGIC
-- MAGIC
-- MAGIC ### Simple
-- MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
-- MAGIC
-- MAGIC ### Open
-- MAGIC   Built on open source and open standards. You own your data and prevent vendor lock-in, with easy integration with external solutions. Being open also lets you share your data with any external organization, regardless of their data stack/vendor.
-- MAGIC
-- MAGIC ### Multicloud
-- MAGIC   One consistent data platform across clouds. Process your data where you need.
-- MAGIC  
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00-FSI-fraud-detection-introduction-lakehouse&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Reducing Fraud  with the lakehouse
-- MAGIC
-- MAGIC Being able to collect and centralize information in real time is critical for the industry. Data is the key to unlock critical capabilities such as realtime personalization or fraud prevention. <br/> 
-- MAGIC
-- MAGIC ### What we'll build
-- MAGIC
-- MAGIC In this demo, we'll build an end-to-end Banking platform, collecting data from multiple sources in real time. 
-- MAGIC
-- MAGIC With this information, we'll not only be able to analyse existing past fraud and understand common patterns, but we'll also be able to rate financial transaction risk in realtime.
-- MAGIC
-- MAGIC Based on this information, we'll be able to proactively reduce Fraud. A typical example could be asking for an extra security challenge or having human intervention when our model scores high.
-- MAGIC
-- MAGIC At a very high level, this is the flow we'll implement:
-- MAGIC
-- MAGIC <img width="1000px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-overview-0.png" />
-- MAGIC
-- MAGIC 1. Ingest and create our Banking database, with tables easy to query in SQL
-- MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
-- MAGIC 3. Run BI queries to analyse existing Fraud
-- MAGIC 4. Build ML models & deploy them to provide real-time fraud detection capabilities.
-- MAGIC
-- MAGIC ### Our dataset
-- MAGIC
-- MAGIC To simplify this demo, we'll consider that an external system is periodically sending data into our blob storage (S3/ADLS/GCS):
-- MAGIC
-- MAGIC - Banking transaction history
-- MAGIC - Customer data (profile)
-- MAGIC - Metadata table
-- MAGIC - Report of past fraud (used as our label)
-- MAGIC
-- MAGIC *Note that at a technical level, our data could come from any source. Databricks can ingest data from any system (Salesforce, Fivetran, message queues like kafka, blob storage, SQL & NoSQL databases...).*
-- MAGIC
-- MAGIC Let's see how this data can be used within the Lakehouse to analyse our customer transactions & detect potential fraud in realtime.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting and preparing the data (Data Engineering)
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-1.png" />
-- MAGIC
-- MAGIC
-- MAGIC <br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC Our first step is to ingest and clean the raw data we received so that our Data Analyst team can start running analysis on top of it.
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
-- MAGIC
-- MAGIC ### Delta Lake
-- MAGIC
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. [Delta Lake](https://delta.io) is an open storage framework for reliability and performance. <br/>
-- MAGIC It provides many functionalities *(ACID Transactions, DELETE/UPDATE/MERGE, zero-copy clone, Change Data Capture...)* <br />
-- MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`
-- MAGIC
-- MAGIC ### Simplify ingestion with Spark Declarative Pipelines (SDP)
-- MAGIC
-- MAGIC Databricks simplifies data ingestion and transformation with Spark Declarative Pipelines by allowing SQL users to create advanced pipelines, in batch or streaming. The engine will simplify pipeline deployment and testing and reduce operational complexity, so that you can focus on your business transformation and ensure data quality.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the FSI Banking & Fraud  <a dbdemos-pipeline-id="sdp-fsi-fraud" href="#joblist/pipelines/c8083360-9492-446d-9293-e648527c85eb" target="_blank">Spark Declarative Pipelines pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-sdp-sql/01-SDP-fraud-detection-SQL) *(Alternative: SDP Python version will be available soon)*. <br>
-- MAGIC   For more details on SDP: `dbdemos.install('pipeline-bike')` or `dbdemos.install('declarative-pipeline-cdc')`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ Securing data & governance (Unity Catalog)
-- MAGIC
-- MAGIC <img style="float: left" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-2.png" />
-- MAGIC
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC   Now that our first tables have been created, we need to grant our Data Analyst team READ access to be able to start analyzing our banking database information.
-- MAGIC   
-- MAGIC   Let's see how Unity Catalog provides security & governance across our data assets, including data lineage and audit logs.
-- MAGIC   
-- MAGIC   Note that Unity Catalog integrates Delta Sharing, an open protocol to share your data with any external organization, regardless of their stack. For more details:  `dbdemos.install('delta-sharing-airlines')`
-- MAGIC  </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-ACL-fsi-fraud) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ Analysing existing Fraud (BI / Data warehousing / SQL) 
-- MAGIC
-- MAGIC <img width="500px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-dashboard.png"  style="float: right; margin: 0px 0px 10px;"/>
-- MAGIC
-- MAGIC
-- MAGIC <img width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-3.png"  style="float: left; margin-right: 10px"/>
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC Our datasets are now properly ingested, secured, high-quality, and easily discoverable within our organization.
-- MAGIC
-- MAGIC Data Analysts are now ready to run BI interactive queries, with low latencies & high throughput, including Serverless data warehouses providing instant stop & start.
-- MAGIC
-- MAGIC Let's see how Data Warehousing can be done using Databricks, including integration with external BI solutions like PowerBI, Tableau and others!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Data warehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing-fraud) to start running your BI queries, or directly open the <a dbdemos-dashboard-id="fraud-detection" href='/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956' target="_blank">Banking Fraud Analysis dashboard.</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Predict Fraud risk with Data Science & AutoML
-- MAGIC
-- MAGIC <img style="float: right;" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-real-time-serving.png" width="700px" />
-- MAGIC
-- MAGIC
-- MAGIC <img width="400px" style="float: left; margin-right: 10px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-4.png" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC Being able to run analysis on our past data already gave us a lot of insight to drive our business. We can better understand our customer data and past fraud.
-- MAGIC
-- MAGIC However, knowing where we had fraud in the past isn't enough. We now need to take it to the next level and build a predictive model to detect potential threats before they happen, reducing our risk and increasing customer satisfaction.
-- MAGIC
-- MAGIC This is where the Lakehouse value comes in. Within the same platform, anyone can start building ML models to run such analysis, including low-code solution with AutoML.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ML: Fraud Detection Model Training
-- MAGIC Let's see how Databricks accelerates ML projects with AutoML: one-click model training with the [04.1-AutoML-FSI-fraud]($./04-Data-Science-ML/04.1-AutoML-FSI-fraud)
-- MAGIC
-- MAGIC ### ML: Realtime Model Serving
-- MAGIC
-- MAGIC Once our model is trained and available, Databricks Model Serving can be used to enable real time inferences, allowing fraud detection in real-time.
-- MAGIC
-- MAGIC Review the [04.3-Model-serving-realtime-inference-fraud]($./04-Data-Science-ML/04.3-Model-serving-realtime-inference-fraud)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ Deploying and orchestrating the full workflow
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 10px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-5.png" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC
-- MAGIC With Databricks Lakehouse, there is no need to manage an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options, etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./06-Workflow-orchestration/06-Workflow-orchestration-fsi-fraud) to schedule our pipeline (data ingestion, model re-training etc.)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC We demonstrated how to implement an end-to-end pipeline with the Lakehouse, using a single, unified and secure platform:
-- MAGIC
-- MAGIC - Data ingestion
-- MAGIC - Data analysis / DW / BI 
-- MAGIC - Data science / ML
-- MAGIC - Workflow & orchestration
-- MAGIC
-- MAGIC As a result, our analyst team was able to simply build a system to not only understand but also forecast future failures and take action accordingly.
-- MAGIC
-- MAGIC This was only an introduction to the Databricks Platform. For more details, contact your account team and explore more demos with `dbdemos.list()`
