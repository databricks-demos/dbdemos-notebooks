-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # IoT Platform with Databricks Intelligence Data Platform - Ingesting Industrial Sensor Data for Real-Time Analysis
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-full.png " style="float: left; margin-right: 30px" width="600px" />
-- MAGIC
-- MAGIC <br/>
-- MAGIC
-- MAGIC ## What is The Databricks Intelligence Data Platform for IoT & Manufacturing?
-- MAGIC
-- MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, running any workload to optimize your production lines with real time data at the lowest cost. 
-- MAGIC
-- MAGIC The Lakehouse allow you to centralize all your data, from IoT realtime streams to inventory and sales data, providing operational speed and efficiency at a scale never before possible.
-- MAGIC
-- MAGIC
-- MAGIC ### Simple
-- MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
-- MAGIC
-- MAGIC ### Open
-- MAGIC   Built on open source and open standards. You own your data and prevent vendor lock-in, with easy integration with a wide range of 3rd party software vendors and services. Being open also lets you share your data with any external organization, regardless of their make-up of their software stack or vendor.
-- MAGIC
-- MAGIC ### Multi-cloud
-- MAGIC   One consistent data platform across clouds. Process your data where you need it or where it resides.
-- MAGIC  
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Flakehouse_iot_turbine%2Fintro&dt=LAKEHOUSE_MANUFACTURING_TURBINE">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Wind Turbine Predictive Maintenance with the Databricks Intelligence Data Platform
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-photo-open-license.jpg" width="400px" style="float:right; margin-left: 20px"/>
-- MAGIC
-- MAGIC Being able to collect and centralize industrial equipment information in real time is critical in the energy space. When a wind turbine is down, it is not generating power which leads to poor customer service and lost revenue. Data is the key to unlock critical capabilities such as energy optimization, anomaly detection, and/or predictive maintenance. <br/> 
-- MAGIC
-- MAGIC Predictive maintenance examples include:
-- MAGIC
-- MAGIC - Predict mechanical failure in an energy pipeline
-- MAGIC - Detect abnormal behavior in a production line
-- MAGIC - Optimize supply chain of parts and staging for scheduled maintenance and repairs
-- MAGIC
-- MAGIC ### What we'll build
-- MAGIC
-- MAGIC In this demo, we'll build a end-to-end IoT platform, collecting data from multiple sources in real time. 
-- MAGIC
-- MAGIC Based on this information, we will show how analyst can proactively identify and schedule repairs for Wind turbines prior to failure, in order to increase energy production.
-- MAGIC
-- MAGIC In addition, the business requested a dashboard that would allow their Turbine Maintenance group to monitor the turbines and identify any that are currently inoperable and those that are at risk of failure. This will also allow us to track our ROI and ensure we reach our productivity goals over the year.
-- MAGIC
-- MAGIC At a very high level, this is the flow we will implement:
-- MAGIC
-- MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-maintenance.png" />
-- MAGIC
-- MAGIC
-- MAGIC 1. Ingest and create our IoT database and tables which are easily queriable via SQL
-- MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
-- MAGIC 3. Run BI queries to analyze existing failures
-- MAGIC 4. Build ML model to monitor our wind turbine farm & trigger predictive maintenance operations
-- MAGIC
-- MAGIC Being able to predict which wind turbine will potentially fail is only the first step to increase our wind turbine farm efficiency. Once we're able to build a model predicting potential maintenance, we can dynamically adapt our spare part stock and even automatically dispatch maintenance team with the proper equipment.
-- MAGIC
-- MAGIC ### Our dataset
-- MAGIC
-- MAGIC To simplify this demo, we'll consider that an external system is periodically sending data into our blob storage (S3/ADLS/GCS):
-- MAGIC
-- MAGIC - Turbine data *(location, model, identifier etc)*
-- MAGIC - Wind turbine sensors, every sec *(energy produced, vibration, typically in streaming)*
-- MAGIC - Turbine status over time, labelled by our analyst team *(historical data to train on model on)*
-- MAGIC
-- MAGIC *Note that at a technical level, our data could come from any source. Databricks can ingest data from any system (SalesForce, Fivetran, queuing message like kafka, blob storage, SQL & NoSQL databases...).*
-- MAGIC
-- MAGIC Let's see how this data can be used within the Lakehouse to analyze sensor data & trigger predictive maintenance.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting and Preparing the Data (Data Engineering)
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-1.png" />
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
-- MAGIC It provides many functionalities such as *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
-- MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`
-- MAGIC
-- MAGIC ### Simplify ingestion with Delta Live Tables (DLT)
-- MAGIC
-- MAGIC Databricks simplifies data ingestion and transformation with Delta Live Tables by allowing SQL users to create advanced pipelines via batch or streaming. Databricks also simplifies pipeline deployment, testing, and tracking data quality which reduces operational complexity, so that you can focus on the needs of the business.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the Wind Turbine 
-- MAGIC   <a dbdemos-pipeline-id="dlt-iot-wind-turbine" href="#joblist/pipelines/c8083360-9492-446d-9293-e648527c85eb" target="_blank">Delta Live Table pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-Wind-Turbine-SQL) *(Alternatives: DLT Python version Soon available - [plain Delta+Spark version]($./01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-iot-turbine))*. <br>
-- MAGIC   For more details on DLT: `dbdemos.install('dlt-load')` or `dbdemos.install('dlt-cdc')`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ Securing Data & Governance (Unity Catalog)
-- MAGIC
-- MAGIC <img style="float: left" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-5.png" />
-- MAGIC
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC   Now that our first tables have been created, we need to grant our Data Analyst team READ access to be able to start analyzing our turbine failure information.
-- MAGIC   
-- MAGIC   Let's see how Unity Catalog provides Security & governance across our data assets and includes data lineage and audit logs.
-- MAGIC   
-- MAGIC   Note that Unity Catalog integrates Delta Sharing, an open protocol to share your data with any external organization, regardless of their software or data stack. For more details:  `dbdemos.install('delta-sharing-airlines')`
-- MAGIC  </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-security-iot-turbine) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ Analysing Failures (BI / Data warehousing / SQL) 
-- MAGIC
-- MAGIC <img width="300px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-1.png"  style="float: right; margin: 100px 0px 10px;"/>
-- MAGIC
-- MAGIC
-- MAGIC <img width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-3.png"  style="float: left; margin-right: 10px"/>
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC Our datasets are now properly ingested, secured, are of high quality and easily discoverable within our organization.
-- MAGIC
-- MAGIC Data Analysts are now ready to run BI interactive queries which are low latency & high throughput. They can choose to either create a new compute cluster, use a shared cluster, or for even faster response times, use Databricks Serverless Datawarehouses which provide instant stop & start.
-- MAGIC
-- MAGIC Let's see how Data Warehousing is done using Databricks! We will look at our built-in dashboards as Databricks provides a complete data platform from ingest to analysis but also provides to integrations with many popular BI tools such as PowerBI, Tableau and others!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Datawarehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing-iot-turbine) to start running your BI queries or access or directly open the <a href="/sql/dashboards/a6bb11d9-1024-47df-918d-f47edc92d5f4" target="_blank">Wind turbine sensor dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Predict Failure with Data Science & Auto-ML
-- MAGIC
-- MAGIC <img width="400px" style="float: left; margin-right: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-3.png" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC Being able to run analysis on our historical data provided the team with a lot of insights to drive our business. We can now better understand the impact of downtime and see which turbines are currently down in our near real-time dashboard.
-- MAGIC
-- MAGIC However, knowing what turbines have failed isn't enough. We now need to take it to the next level and build a predictive model to detect potential failures before they happen and increase uptime and minimize costs.
-- MAGIC
-- MAGIC This is where the Lakehouse value comes in. Within the same platform, anyone can start building an ML model to predict the failures using traditional ML development or with our low code solution AutoML.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how to train an ML model within 1 click with the [04.1-automl-iot-turbine-predictive-maintenance]($./04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Automate Action to Reduce Turbine Outage Based on Predictions
-- MAGIC
-- MAGIC
-- MAGIC <img style="float: right" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
-- MAGIC
-- MAGIC We now have an end-to-end data pipeline analyzing sensor data and detecting potential failures before they happen. We can now easily trigger actions to reduce outages such as:
-- MAGIC
-- MAGIC - Schedule maintenance based on teams availability and fault gravity
-- MAGIC - Stage parts and supplies accordingly to predictive maintenance operations, while keeping a low stock on hand
-- MAGIC - Analyze past issues and component failures to improve resiliency
-- MAGIC - Track our predictive maintenance model efficiency by measuring its efficiency and ROI
-- MAGIC
-- MAGIC *Note: These actions are out of the scope of this demo and simply leverage the Predictive maintenance result from our ML model.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Open the <a href='/sql/dashboards/d966eb63-6d37-4762-b90f-d3a2b51b9ba8' target="_blank">DBSQL Predictive maintenance Dashboard</a> to have a complete view of your wind turbine farm, including potential faulty turbines and action to remedy that.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ Deploying and Orchestrating the Full Workflow
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 10px" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-4.png" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC
-- MAGIC With Databricks Lakehouse, there is no need to utilize an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./05-Workflow-orchestration/05-Workflow-orchestration-iot-turbine) to schedule our pipeline (data ingetion, model re-training etc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC We demonstrated how to implement an end-to-end pipeline with the Lakehouse, using a single, unified and secured platform. We saw:
-- MAGIC
-- MAGIC - Data Ingestion
-- MAGIC - Data Analysis / DW / BI 
-- MAGIC - Data Science / ML
-- MAGIC - Workflow & Orchestration
-- MAGIC
-- MAGIC And as a result, our business analysis team was able to build a system to not only understand failures better but also forecast future failures and take action accordingly.
-- MAGIC
-- MAGIC *This was only an introduction to the Databricks Platform. For more details, contact your account team and explore more demos with `dbdemos.list()`!*
