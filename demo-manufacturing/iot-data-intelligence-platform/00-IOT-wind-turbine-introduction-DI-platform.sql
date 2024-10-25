-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # IoT Platform with Databricks Intelligence Data Platform - Ingesting real-time Industrial Sensor Data for Prescriptive Maintenance
-- MAGIC
-- MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.54.51.png?raw=true " style="float: left; margin-right: 30px" width="600px" />
-- MAGIC
-- MAGIC <br/>
-- MAGIC
-- MAGIC ## What is The Databricks Intelligence Data Platform for IoT & Manufacturing?
-- MAGIC The Databricks Data Intelligence Platform for Manufacturing is the only enterprise data platform that can unleash the full value of manufacturing data to deliver intelligent manufacturing networks, differentiated customer experiences, smarter products and sustainable businesses. It offers an unmatched scale advantage that enables data teams to be more productive and innovative, regardless of data type, sources or workloads. With Databricks, manufacturers are empowered with real-time insights to make critical decisions that reduce cost, boost industrial productivity, improve customer responsiveness and accelerate innovation. The Databricks Data Intelligence Platform for Manufacturing brings together disparate data sources, paired with best-in-class data and AI processing capabilities, and surrounds this with an ecosystem of manufacturing-specific Solution Accelerators and partners. Manufacturers can take advantage of the full power of all their data and deliver powerful real-time decisions. 
-- MAGIC
-- MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Intelligence%20Engine.png?raw=true " style="float: left; margin-right: 30px" width="200px" />
-- MAGIC
-- MAGIC **Intelligent**
-- MAGIC Databricks combines generative AI with the unification benefits of a lakehouse to power a Data Intelligence Engine that understands the unique semantics of your data. This allows the Databricks Platform to automatically optimize performance and manage infrastructure in ways unique to your business. 
-- MAGIC
-- MAGIC <img src="https://github.com/Datastohne/demo/blob/main/24840.png?raw=true " style="float: right; margin-left: 30px" width="200px" />
-- MAGIC
-- MAGIC **Simple** Natural language substantially simplifies the user experience on Databricks. The Data Intelligence Engine understands your organization’s language, so search and discovery of new data is as easy as asking a question like you would to a coworker. Additionally, developing new data and applications is accelerated through natural language assistance to write code, remediate errors and find answers.
-- MAGIC
-- MAGIC <img src="https://github.com/Datastohne/demo/blob/main/24841.png?raw=true " style="float: left; margin-right: 30px" width="200px" />
-- MAGIC
-- MAGIC **Private** Data and AI applications require strong governance and security, especially with the advent of generative AI. Databricks provides an end-to-end MLOps and AI development solution that’s built upon our unified approach to governance and security. You’re able to pursue all your AI initiatives — from using APIs like OpenAI to custom-built models — without compromising data privacy and IP control.
-- MAGIC  
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00-IOT-wind-turbine-introduction-lakehouse&demo_name=lakehouse-patient-readmission&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Wind Turbine Prescriptive Maintenance with the Databricks Intelligence Data Platform: Bringing Generative AI to Predictive Maintenance
-- MAGIC
-- MAGIC Being able to collect and centralize industrial equipment information in real time is critical in the energy space. When a wind turbine is down, it is not generating power which leads to poor customer service and lost revenue. Data is the key to unlock critical capabilities such as energy optimization, anomaly detection, and/or predictive maintenance. The rapid rise of Generative AI provides the opportunity to revolutionize maintenance by not only predicting when equipment is likely to fail, but also generating prescriptive maintenance actions to prevent failurs before they arise and optimize equipment performance. This enables a shift from predictive to prescriptive maintenance. <br/> 
-- MAGIC
-- MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-10-01%20at%2010.23.40.png?raw=true" width="700px" style="float:right; margin-left: 20px"/>
-- MAGIC
-- MAGIC Prescriptive maintenance examples include:
-- MAGIC
-- MAGIC - Analyzing equipment IoT sensor data in real time
-- MAGIC - Predict mechanical failure in an energy pipeline
-- MAGIC - Diagnose root causes for predicted failure and generate prescriptive actions
-- MAGIC - Detect abnormal behavior in a production line
-- MAGIC - Optimize supply chain of parts and staging for scheduled maintenance and repairs
-- MAGIC
-- MAGIC ### What we'll build
-- MAGIC
-- MAGIC In this demo, we'll build a end-to-end IoT platform, collecting data from multiple sources in real time. 
-- MAGIC
-- MAGIC Based on this information, we will build a predictive model that predicts wind turbine failure (predictive maintenance). Next, we will leverage this predictive model as AI tool in an AI system that will generate maintenance work orders (prescriptive maintenance), based on similar historical maintenance reports and turbine specifications. This way our maintenance teams can proactively address potential failures before they arise, to reduce downtime and increase Overall Equipment Effectiveness (OEE).
-- MAGIC
-- MAGIC In addition, the business requested a dashboard that would allow their Turbine Maintenance group to monitor the turbines, identify the ones at risk of failure and browse through the generated maintenance work orders. This will also allow us to track our ROI and ensure we reach our productivity goals over the year.
-- MAGIC
-- MAGIC At a very high level, this is the flow we will implement:
-- MAGIC
-- MAGIC <div style="text-align: center;">
-- MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/Prescriptive%20Maintenance%20Demo%20Overview%20(26).png?raw=true" width="1000px">
-- MAGIC </div>
-- MAGIC
-- MAGIC 1. Ingest and create our IoT database and tables which are easily queriable via SQL.
-- MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
-- MAGIC 3. Run BI queries to analyze existing failures.
-- MAGIC 4. Build ML model to monitor our wind turbine farm & trigger predictive maintenance operations.
-- MAGIC 5. Generate maintenance work orders for field service engineers utilizing Generative AI.
-- MAGIC
-- MAGIC Being able to predict which wind turbine will potentially fail is only the first step to increase our wind turbine farm efficiency. Once we're able to build a model predicting potential maintenance, we can dynamically adapt our spare part stock, generate work orders for field service engineers and even automatically dispatch maintenance team with the proper equipment.
-- MAGIC
-- MAGIC ### Our dataset
-- MAGIC
-- MAGIC To simplify this demo, we'll consider that an external system is periodically sending data into our blob storage (S3/ADLS/GCS):
-- MAGIC
-- MAGIC - Turbine data *(location, model, identifier etc)*
-- MAGIC - Wind turbine sensors, every sec *(energy produced, vibration, typically in streaming)*
-- MAGIC - Turbine status over time, labelled by our analyst team, and historical maintenance reports *(historical data to train on model on and to index into vector database)*
-- MAGIC
-- MAGIC *Note that at a technical level, our data could come from any source. Databricks can ingest data from any system (SalesForce, Fivetran, queuing message like kafka, blob storage, SQL & NoSQL databases...).*
-- MAGIC
-- MAGIC Let's see how this data can be used within the Data Intelligence Platform to analyze sensor data,  trigger predictive maintenance and generate work orders.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting and Preparing the Data (Data Engineering)
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 20px" width="500px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.55.15.png?raw=true" />
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
-- MAGIC <img width="500px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.55.03.png?raw=true"  style="float: left; margin-right: 10px"/>
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
-- MAGIC <img width="500px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.55.27.png?raw=true"  style="float: left; margin-right: 10px"/>
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC Our datasets are now properly ingested, secured, are of high quality and easily discoverable within our organization.
-- MAGIC
-- MAGIC Data Analysts are now ready to run BI interactive queries which are low latency & high throughput. They can choose to either create a new compute cluster, use a shared cluster, or for even faster response times, use Databricks Serverless Datawarehouses which provide instant stop & start.
-- MAGIC
-- MAGIC Let's see how Data Warehousing is done using Databricks! We will look at our built-in dashboards as Databricks provides a complete data platform from ingest to analysis but also provides to integrations with many popular BI tools such as PowerBI, Tableau and others!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Datawarehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing-iot-turbine) to start running your BI queries or access or directly open the <a dbdemos-dashboard-id="turbine-analysis" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956" target="_blank">Turbine analysis AI/BI dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Predict Failure with Data Science & Auto-ML
-- MAGIC
-- MAGIC <img width="500px" style="float: left; margin-right: 10px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.55.35.png?raw=true" />
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
-- MAGIC ## 5/ Generate Maintenance Work Orders with Generative AI
-- MAGIC
-- MAGIC <img width="500px" style="float: left; margin-right: 10px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.55.44.png?raw=true" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC
-- MAGIC The rise of Generative AI enables a shift from Predictive to Prescriptive Maintenance ML Models. By going from ML models to compound AI systems, we can now leverage the predictive model as one of the many components of the AI system. This opens up a whole lot of new opportunities for automation and efficiency gains, which will further increase uptime and minimize costs.
-- MAGIC
-- MAGIC Databricks offers a a set of tools to help developers build, deploy and evaluate production-quality AI agents like Retrievel Augmented Generation (RAG) applications, including a vector database, model serving endpoints, governance, monitoring and evaluation capabilties. 
-- MAGIC
-- MAGIC _Disclaimer: if your organization doesn't allow (yet) the use of Databricks Vector Search and/or Model Serving, you can skip this section._

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's create our first compound AI system with the [05.1-ai-tools-iot-turbine-prescriptive-maintenance]($./05-Generative-AI/05.1-ai-tools-iot-turbine-prescriptive-maintenance)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Automate Action to Reduce Turbine Outage Based on Predictions
-- MAGIC
-- MAGIC
-- MAGIC <img style="float: right" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
-- MAGIC
-- MAGIC We now have an end-to-end data pipeline analyzing sensor data, detecting potential failures and generating prescriptive actions based on past maintenance reports to prevent failures before they even arise. With that, we can now easily trigger follow-up actions to reduce outages such as:
-- MAGIC
-- MAGIC - Schedule maintenance based on teams availability and fault gravity
-- MAGIC - Stage parts and supplies accordingly to predictive maintenance operations, while keeping a low stock on hand
-- MAGIC - Track our predictive maintenance model efficiency by measuring its efficiency and ROI
-- MAGIC
-- MAGIC *Note: These actions are out of the scope of this demo and simply leverage the Predictive maintenance result from our ML model.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Open the <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Prescriptive maintenance AI/BI dashboard</a> to have a complete view of your wind turbine farm, including potential faulty turbines, work orders and actions to remedy that.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 6/ Deploying and Orchestrating the Full Workflow
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 10px" width="500px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-27%20at%2015.55.52.png?raw=true" />
-- MAGIC
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC
-- MAGIC With Databricks Lakehouse, there is no need to utilize an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./06-Workflow-orchestration/06-Workflow-orchestration-iot-turbine) to schedule our pipeline (data ingetion, model re-training etc)

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
-- MAGIC - Generative AI
-- MAGIC - Workflow & Orchestration
-- MAGIC
-- MAGIC And as a result, our business analysis team was able to build a system to not only understand failures better but also forecast future failures and let the maintenance team take action accordingly.
-- MAGIC
-- MAGIC *This was only an introduction to the Databricks Platform. For more details, contact your account team and explore more demos with `dbdemos.list()`!*
