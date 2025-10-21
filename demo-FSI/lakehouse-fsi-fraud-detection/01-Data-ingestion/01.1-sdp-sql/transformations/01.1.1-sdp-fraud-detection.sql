-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Data engineering with Databricks - Realtime data ingestion for Financial transactions
-- MAGIC
-- MAGIC Building realtime system consuming messages from live system is required to build reactive data application. 
-- MAGIC
-- MAGIC Near real-time is key to detect new fraud pattern and build a proactive system, offering better protection for your customers.
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
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.1-DLT-fraud-detection-SQL&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Demo: build a banking database and detect fraud on transaction in real-time (ms)
-- MAGIC
-- MAGIC In this demo, we'll step in the shoes of a retail banking company processing transaction.
-- MAGIC
-- MAGIC The business has determined that we should improve our transaction fraud system and offer a better protection to our customers (retail and institutions using our payment systems). We're asked to:
-- MAGIC
-- MAGIC * Analyse and explain current transactions: quantify fraud, understand pattern and usage
-- MAGIC * Build a proactive system to detect fraud and serve prediction in real-time (ms latencies)
-- MAGIC
-- MAGIC
-- MAGIC ### What we'll build
-- MAGIC
-- MAGIC To do so, we'll build an end-to-end solution with the Lakehouse. To be able to properly analyse and detect fraud, we'll mainly focus on transactional data, received by our banking system.
-- MAGIC
-- MAGIC At a very high level, this is the flow we'll implement:
-- MAGIC
-- MAGIC <img width="1000px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/lakehouse-fsi-fraud-overview-1.png" />
-- MAGIC
-- MAGIC 1. Ingest and create our banking database, with tables easy to query in SQL
-- MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
-- MAGIC 3. Run BI queries to analyse existing fraud
-- MAGIC 4. Build an ML model to detect fraud and deploy this model for real-time inference
-- MAGIC
-- MAGIC As a result, we'll have all the information required to trigger alerts and ask our customer for stronger authentication if we believe there is a high fraud risk.
-- MAGIC
-- MAGIC **A note on Fraud detection in real application** <br/>
-- MAGIC *This demo is a simple example to showcase the Lakehouse benefits. We'll keep the data model and ML simple for the sake of the demo. Real-world application would need more data sources and also deal with imbalanced class and more advanced models. If you are interested in a more advanced discussion, reach out to your Databricks team!*
-- MAGIC
-- MAGIC Let's see how this data can be used within the Lakehouse to analyse and reduce fraud!  

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Building a Spark Declarative Pipelines pipeline to analyze and reduce fraud detection in real-time
-- MAGIC
-- MAGIC In this example, we'll implement a end 2 end SDP pipeline consuming our banking transactions information. We'll use the medaillon architecture but we could build star schema, data vault or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader and enrich this information.
-- MAGIC
-- MAGIC This information will then be used  to:
-- MAGIC
-- MAGIC * Build our DBSQL dashboard to track transactions and fraud impact.
-- MAGIC * Train & deploy a model to detect potential fraud in real-time.
-- MAGIC
-- MAGIC Let's implement the following flow: 
-- MAGIC  
-- MAGIC <img width="1200px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-dlt-full.png"/>
-- MAGIC
-- MAGIC
-- MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-fraud-detection) using Databricks AutoML to predict fraud. We'll cover that in the next section.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your SDP Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="sdp-fsi-fraud" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Fraud detection Spark Declarative Pipelines pipeline</a> to see it in action.<br/>
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- DBTITLE 1,Let's explore our raw incoming data: transactions (json)
-- MAGIC %python
-- MAGIC display(spark.read.json('/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/transactions'))

-- COMMAND ----------

-- DBTITLE 1,Raw incoming customers (json)
-- MAGIC %python
-- MAGIC display(spark.read.csv('/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/customers', header=True, multiLine=True))

-- COMMAND ----------

-- DBTITLE 1,Raw incoming country
-- MAGIC %python
-- MAGIC display(spark.read.csv('/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/country_code', header=True))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC
-- MAGIC <img  style="float:right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-dlt-1.png"/>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loadeoner')`
-- MAGIC
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/dbdemos/fsi/fraud-detection/...`. 

-- COMMAND ----------

-- DBTITLE 1,Ingest transactions
CREATE STREAMING TABLE bronze_transactions 
  COMMENT "Historical banking transaction to be trained on fraud detection"
AS 
  SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/transactions", "json", map("cloudFiles.maxFilesPerTrigger", "1", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,Customers
CREATE STREAMING LIVE TABLE banking_customers (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Customer data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution"
AS 
  SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/customers", "csv", map("cloudFiles.inferColumnTypes", "true", "multiLine", "true"))

-- COMMAND ----------

-- DBTITLE 1,Reference table
CREATE STREAMING LIVE TABLE country_coordinates
AS 
  SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/country_code", "csv")

-- COMMAND ----------

-- DBTITLE 1,Fraud report (labels for ML training)
CREATE STREAMING LIVE TABLE fraud_reports
AS 
  SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/fraud_report", "csv")

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC
-- MAGIC <img style="float:right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-dlt-2.png"/>
-- MAGIC
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information:
-- MAGIC
-- MAGIC * Clean up the codes of the countries of origin and destination (removing the "--")
-- MAGIC * Calculate the difference between the Originating and Destination Balances.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced SDP capabilities run `dbdemos.install('pipeline-bike')` or `dbdemos.install('declarative-pipeline-cdc')` for CDC/SCDT2 example.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- DBTITLE 1,Silver
CREATE STREAMING LIVE TABLE silver_transactions (
  CONSTRAINT correct_data EXPECT (id IS NOT NULL),
  CONSTRAINT correct_customer_id EXPECT (customer_id IS NOT NULL)
)
AS 
  SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data, f._rescued_data), 
          regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
          regexp_replace(countryDest, "\-\-", "") as countryDest, 
          newBalanceOrig - oldBalanceOrig as diffOrig, 
          newBalanceDest - oldBalanceDest as diffDest
FROM STREAM(live.bronze_transactions) t
  LEFT JOIN live.fraud_reports f using(id)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Aggregate and join data to create our ML features
-- MAGIC
-- MAGIC <img style="float:right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-dlt-3.png"/>
-- MAGIC
-- MAGIC We're now ready to create the features required for Fraud detection.
-- MAGIC
-- MAGIC We need to enrich our transaction dataset with extra information our model will use to help detecting churn.

-- COMMAND ----------

-- DBTITLE 1,Gold, ready for Data Scientists to consume
CREATE LIVE TABLE gold_transactions (
  CONSTRAINT amount_decent EXPECT (amount > 10)
)
AS 
  SELECT t.* EXCEPT(countryOrig, countryDest, is_fraud), c.* EXCEPT(id, _rescued_data),
          boolean(coalesce(is_fraud, 0)) as is_fraud,
          o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
          d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat
FROM live.silver_transactions t
  INNER JOIN live.country_coordinates o ON t.countryOrig=o.alpha3_code 
  INNER JOIN live.country_coordinates d ON t.countryDest=d.alpha3_code 
  INNER JOIN live.banking_customers c ON c.id=t.customer_id 

-- COMMAND ----------

-- MAGIC %md ## Our pipeline is now ready!
-- MAGIC
-- MAGIC As you can see, building Data Pipeline with databricks let you focus on your business implementation while the engine solves all hard data engineering work for you.
-- MAGIC
-- MAGIC The table is now ready for our Data Scientist to train a model detecting fraud risk.
-- MAGIC
-- MAGIC Open the <a dbdemos-pipeline-id="sdp-fsi-fraud" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Fraud detection Spark Declarative Pipelines pipeline</a> and click on start to visualize your lineage and consume the new data incrementally!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC
-- MAGIC Jump to the [Governance with Unity Catalog notebook]($../00-churn-introduction-lakehouse) or [Go back to the introduction]($../00-churn-introduction-lakehouse)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### Source Data
-- MAGIC
-- MAGIC This dataset is built with PaySim, an open source banking transactions simulator.
-- MAGIC
-- MAGIC [PaySim](https://github.com/EdgarLopezPhD/PaySim) simulates mobile money transactions based on a sample of real transactions extracted from one month of financial logs from a mobile money service implemented in an African country. 
