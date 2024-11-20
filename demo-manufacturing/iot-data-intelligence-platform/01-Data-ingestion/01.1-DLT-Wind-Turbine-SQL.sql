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
-- MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John, as Data engineer, spends immense time….
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
-- MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
-- MAGIC
-- MAGIC <img style="float: right" width="500px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-10-01%20at%2014.44.32.png?raw=true" />
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our IOT platform. <br>
-- MAGIC We'll ingest and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC Databricks simplifies this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC DLT allows Data Analysts to create advanced pipeline with plain SQL.
-- MAGIC
-- MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>Accelerate ETL development</strong> <br/>
-- MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>Remove operational complexity</strong> <br/>
-- MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>Trust your data</strong> <br/>
-- MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>Simplify batch and streaming</strong> <br/>
-- MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC All the tables we'll create in the Data Intelligence Platform will be stored as Delta Lake table. Delta Lake is an open storage framework for reliability and performance.<br>
-- MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
-- MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Building a Delta Live Table pipeline to ingest IOT sensor and detect faulty equipments
-- MAGIC
-- MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our Wind Turbine sensor data. <br/>
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
-- MAGIC Your DLT Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="dlt-iot-wind-turbine" href="#joblist/pipelines/c8083360-9492-446d-9293-e648527c85eb" target="_blank">IOT Wind Turbine Delta Live Table pipeline</a> to see it in action.<br/>
-- MAGIC
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed with dbdemos, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- DBTITLE 1,Wind Turbine metadata:
-- %python #uncomment to scan the data from the notebook
-- display(spark.read.json('/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/turbine'))
-- display(spark.read.json('/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/historical_turbine_status')) #Historical turbine status analyzed

-- COMMAND ----------

-- DBTITLE 1,Wind Turbine sensor data
-- %python #uncomment to scan the data from the notebook
-- display(spark.read.parquet('/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/incoming_data'))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 1/ Ingest data: ingest data using Auto Loader (cloudFiles)
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
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/manufacturing/iot_turbine/...`. 

-- COMMAND ----------

-- DBTITLE 1,Turbine metadata
CREATE STREAMING TABLE turbine (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Turbine details, with location, wind turbine model type etc"
AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/turbine", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- DBTITLE 1,Wind Turbine sensor 
CREATE STREAMING TABLE sensor_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL),
  CONSTRAINT correct_energy EXPECT (energy IS NOT NULL and energy > 0) ON VIOLATION DROP ROW
)
COMMENT "Raw sensor data coming from json files ingested in incremental with Auto Loader: vibration, energy produced etc. 1 point every X sec per sensor."
AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/incoming_data", "parquet", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- DBTITLE 1,Historical status
CREATE STREAMING TABLE historical_turbine_status (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Turbine status to be used as label in our predictive maintenance model (to know which turbine is potentially faulty)"
AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/historical_turbine_status", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

CREATE STREAMING TABLE parts 
COMMENT "Turbine parts from our manufacturing system"
AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_iot_platform/turbine_raw_landing/parts", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 2/ Compute aggregations: merge sensor data at an hourly level
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-2.png" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC To be able to analyze our data, we'll compute statistical metrics every at an ourly basis, such as standard deviation and quartiles.
-- MAGIC
-- MAGIC *Note that we'll be recomputing all the table to keep this example simple. We could instead UPSERT the current hour with a stateful agregation*

-- COMMAND ----------

CREATE VIEW sensor_hourly (
  CONSTRAINT turbine_id_valid EXPECT (turbine_id IS not NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT timestamp_valid EXPECT (hourly_timestamp IS not NULL)  ON VIOLATION DROP ROW
)
COMMENT "Hourly sensor stats, used to describe signal and detect anomalies"
AS
SELECT turbine_id,
      date_trunc('hour', from_unixtime(timestamp)) AS hourly_timestamp, 
      avg(energy)          as avg_energy,
      stddev_pop(sensor_A) as std_sensor_A,
      stddev_pop(sensor_B) as std_sensor_B,
      stddev_pop(sensor_C) as std_sensor_C,
      stddev_pop(sensor_D) as std_sensor_D,
      stddev_pop(sensor_E) as std_sensor_E,
      stddev_pop(sensor_F) as std_sensor_F,
      percentile_approx(sensor_A, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_A,
      percentile_approx(sensor_B, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_B,
      percentile_approx(sensor_C, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_C,
      percentile_approx(sensor_D, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_D,
      percentile_approx(sensor_E, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_E,
      percentile_approx(sensor_F, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_F
  FROM LIVE.sensor_bronze GROUP BY hourly_timestamp, turbine_id

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 3/ Build our table used by ML Engineers: join sensor aggregates with wind turbine metadata and historical status
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-3.png" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC Next, we'll build a final table joining sensor aggregate with our turbine information.
-- MAGIC
-- MAGIC This table will contain all the data required to be able to infer potential turbine failure.

-- COMMAND ----------

CREATE VIEW turbine_training_dataset 
COMMENT "Hourly sensor stats, used to describe signal and detect anomalies"
AS
SELECT CONCAT(t.turbine_id, '-', s.start_time) AS composite_key, array(std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F) AS sensor_vector, * except(t._rescued_data, s._rescued_data, m.turbine_id) FROM LIVE.sensor_hourly m
    INNER JOIN LIVE.turbine t USING (turbine_id)
    INNER JOIN LIVE.historical_turbine_status s ON m.turbine_id = s.turbine_id AND from_unixtime(s.start_time) < m.hourly_timestamp AND from_unixtime(s.end_time) > m.hourly_timestamp

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 4/ Get model from registry and add flag faulty turbines
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

-- Note: The AI model predict_maintenance is loaded from the 01.2-DLT-Wind-Turbine-SQL-UDF notebook
CREATE VIEW turbine_current_status 
COMMENT "Wind turbine last status based on model prediction"
AS
WITH latest_metrics AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY turbine_id, hourly_timestamp ORDER BY hourly_timestamp DESC) AS row_number FROM LIVE.sensor_hourly
)
SELECT * EXCEPT(m.row_number), 
    predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, percentiles_sensor_A, percentiles_sensor_B, percentiles_sensor_C, percentiles_sensor_D, percentiles_sensor_E, percentiles_sensor_F, location, model, state) as prediction 
  FROM latest_metrics m
   INNER JOIN LIVE.turbine t USING (turbine_id)
   WHERE m.row_number=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC Our <a dbdemos-pipeline-id="dlt-iot-wind-turbine" href="#joblist/pipelines/c8083360-9492-446d-9293-e648527c85eb" target="_blank">DLT Data Pipeline</a> is now ready using purely SQL. We have an end 2 end cycle, and our ML model has been integrated seamlessly by our Data Engineering team.
-- MAGIC
-- MAGIC
-- MAGIC For more details on model training, open the [model training notebook]($../04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance)
-- MAGIC
-- MAGIC Our final dataset includes our ML prediction for our Predictive Maintenance use-case. 
-- MAGIC
-- MAGIC We are now ready to build our <a dbdemos-dashboard-id="turbine-analysis" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">AI/BI Dashboard</a> to track the main KPIs and status of our entire Wind Turbine Farm and build complete <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Predictive maintenance AI/BI Dashboard</a>.
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-1.png" width="1000px">
