-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Data engineering with Databricks - Building our Manufacturing IOT platform
-- MAGIC
-- MAGIC Building an IOT platform requires to ingest multiple datasources.  
-- MAGIC
-- MAGIC It's a complex process requiring batch loads and streaming ingestion to support real-time insights, used for real-time monitoring among others.
-- MAGIC
-- MAGIC Ingesting, transforming and cleaning data to create clean SQL tables for our downstream user (Data Analysts and Data Scientists) is complex.
-- MAGIC
-- MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
-- MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
-- MAGIC   <div style="height: 300px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
-- MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
-- MAGIC       73%
-- MAGIC     </div>
-- MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">of enterprise data goes unused for analytics and decision making</div>
-- MAGIC   </div>
-- MAGIC   <div style="color: #bfbfbf; padding-top: 5px">Source: Forrester</div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br>
-- MAGIC
-- MAGIC ## <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/john.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John, as Data engineer, spends immense time….
-- MAGIC
-- MAGIC
-- MAGIC * Hand-coding data ingestion & transformations and dealing with technical challenges:<br>
-- MAGIC   *Supporting streaming and batch, handling concurrent operations, small files issues, GDPR requirements, complex DAG dependencies...*<br><br>
-- MAGIC * Building custom frameworks to enforce quality and tests<br><br>
-- MAGIC * Building and maintaining scalable infrastructure, with observability and monitoring<br><br>
-- MAGIC * Managing incompatible governance models from different systems
-- MAGIC <br style="clear: both">
-- MAGIC
-- MAGIC This results in **operational complexity** and overhead, requiring expert profile and ultimatly **putting data projects at risk**.
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.1-DLT-Wind-Turbine-SQL&demo_name=lakehouse-patient-readmission&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Simplify Ingestion and Transformation with Spark Declarative Pipelines
-- MAGIC
-- MAGIC <img style="float: right" width="500px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/team_flow_john.png" />
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our IOT platform. <br>
-- MAGIC We'll ingest and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC Databricks simplifies this task with Spark Declarative Pipelines by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC Spark Declarative Pipelines allows Data Analysts to create advanced pipeline with plain SQL, or python.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your Spark Declarative Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="sdp-iot-wind-turbine" href="#joblist/pipelines/c8083360-9492-446d-9293-e648527c85eb" target="_blank">IOT Wind Turbine Spark Declarative Pipeline</a> to see it in action.<br/>
-- MAGIC
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed with dbdemos, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Building a Spark Declarative Pipeline to ingest IOT sensor and detect faulty equipments
-- MAGIC
-- MAGIC In this example, we'll implement a end 2 end Spark Declarative Pipeline consuming our Wind Turbine sensor data. <br/>
-- MAGIC We'll use the medaillon architecture but we could build star schema, data vault or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our predictive maintenance analysis.
-- MAGIC
-- MAGIC This information will then be used to build our AI/BI dashboards to track our wind turbine farm status, faulty equipment impact and recommendations to reduce potential downtime.
-- MAGIC
-- MAGIC ### Dataset:
-- MAGIC
-- MAGIC * <strong>Turbine metadata</strong>: Turbine ID, location (1 row per turbine)
-- MAGIC * <strong>Turbine sensor stream</strong>: Realtime streaming flow from wind turbine sensor (vibration, energy produced, speed etc)
-- MAGIC * <strong>Turbine status</strong>: Historical turbine status based to analyse which part is faulty (used as label in our ML model)
-- MAGIC
-- MAGIC
-- MAGIC Let's implement the following flow: 
-- MAGIC  
-- MAGIC <div><img width="1100px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-full.png"/></div>
-- MAGIC
-- MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-predictive-maintenance-turbine) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1/ Data Exploration
-- MAGIC
-- MAGIC All Data projects start with some exploration. Open the [/explorations/sample_exploration]($./explorations/sample_exploration) notebook to get started and discover the data made available to you

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 2/ Ingest data: Bronze layer
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-1.png" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC Ingesting data from stream source can be challenging. In this example we'll incrementally load the files from our cloud storage, only getting the new one (in near real-time or triggered every X hours).
-- MAGIC
-- MAGIC Note that while our streaming data is added to our cloud storage, we could easily ingest from kafka directly : `.format(kafka)`
-- MAGIC
-- MAGIC Auto-loader provides for you:
-- MAGIC
-- MAGIC - Schema inference and evolution
-- MAGIC - Scalability handling million of files
-- MAGIC - Simplicity: just define your ingestion folder, Databricks take care of the rest!
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('data-ingestion')`
-- MAGIC
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/manufacturing/iot_turbine/...`. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/01-bronze.sql]($./transformations/01-bronze.sql) notebook to review the SQL queries ingesting the raw data and creating our bronze layer.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 3/ Silver layer
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-2.png" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC To be able to analyze our data, we'll compute statistical metrics every at an ourly basis, such as standard deviation and quartiles.
-- MAGIC
-- MAGIC *Note that we'll be recomputing all the table to keep this example simple. We could instead UPSERT the current hour with a stateful agregation*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/01-silver.sql]($./transformations/01-silver.sql) notebook to review the SQL queries creating our features and our training dataset

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 4/ Gold layer: Get model from registry and add flag faulty turbines
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-4.png" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC Our Data scientist team has been able to read data from our previous table and build a predictive maintenance model using Auto ML and saved it into Databricks Model registry (we'll see how to do that next).
-- MAGIC
-- MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict faulty turbines with into our pipeline directly.
-- MAGIC
-- MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.
-- MAGIC
-- MAGIC All we have to do is load the model, and call it as a SQL function (or python)

-- COMMAND ----------

-- DBTITLE 1,Let's create an intermediate feature table
-- MAGIC %md Open the [transformations/01-gold.sql]($./transformations/01-gold.sql) notebook to review the SQL queries creating our features and our training dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC Our <a dbdemos-pipeline-id="sdp-iot-wind-turbine" href="#joblist/pipelines/c8083360-9492-446d-9293-e648527c85eb" target="_blank">Spark Declarative Pipeline</a> is now ready using purely SQL. We have an end 2 end cycle, and our ML model has been integrated seamlessly by our Data Engineering team.
-- MAGIC
-- MAGIC
-- MAGIC For more details on model training, open the [model training notebook]($../../04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance)
-- MAGIC
-- MAGIC Our final dataset includes our ML prediction for our Predictive Maintenance use-case. 
-- MAGIC
-- MAGIC We are now ready to build our <a dbdemos-dashboard-id="turbine-analysis" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">AI/BI Dashboard</a> to track the main KPIs and status of our entire Wind Turbine Farm and build complete <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Predictive maintenance AI/BI Dashboard</a>.
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-1.png" width="1000px">
