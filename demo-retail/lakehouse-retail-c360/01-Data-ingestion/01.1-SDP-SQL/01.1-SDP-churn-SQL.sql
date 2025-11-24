-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Data engineering with Databricks - Building our C360 database
-- MAGIC
-- MAGIC Building a C360 database requires ingesting multiple data sources.
-- MAGIC
-- MAGIC It's a complex process requiring batch loads and streaming ingestion to support real-time insights, used for personalization and marketing targeting among other.
-- MAGIC
-- MAGIC Ingesting, transforming and cleaning data to create clean SQL tables for our downstream user (Data Analysts and Data Scientists) is complex.
-- MAGIC
-- MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
-- MAGIC <div style="width: 300px; height: 300px; text-align: center; float: right; margin: 30px 60px 10px 10px; font-family: 'DM Sans'; border-radius: 50%; border: 25px solid #fcba33ff; box-sizing: border-box; overflow: hidden;">
-- MAGIC   <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; width: 100%;">
-- MAGIC     <div style="font-size: 70px; color: #70c4ab; font-weight: bold;">
-- MAGIC       73%
-- MAGIC     </div>
-- MAGIC     <div style="color: #1b5162; padding: 0 30px; text-align: center;">
-- MAGIC       of enterprise data goes unused for analytics and decision making
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div style="color: #bfbfbf; padding-top: 5px;">
-- MAGIC     Source: Forrester
-- MAGIC   </div>
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
-- MAGIC This results in **operational complexity** and overhead, requiring expert profile and ultimately **putting data projects at risk**.
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.1-DLT-churn-SQL&demo_name=lakehouse-retail-c360&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Simplify Ingestion and Transformation with Lakeflow Connect & SDP
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cross_demo_assets/Lakehouse_Demo_Team_architecture_1.png?raw=true" style="float: right" width="500px">
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our c360 database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC We want to ingest the datasets below from Salesforce Sales Cloud and blob storage (`/demos/retail/churn/`) incrementally into our Data Warehousing tables:
-- MAGIC
-- MAGIC - Customer profile data *(name, age, address etc)*
-- MAGIC - Orders history *(what our customer bought over time)*
-- MAGIC - Streaming Events from our application *(when was the last time customers used the application, typically a stream from a Kafka queue)*
-- MAGIC
-- MAGIC
-- MAGIC <a href="https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo" target="_blank"><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/lakeflow-connect-anim.gif?raw=true" style="float: right; margin-right: 20px" width="250px"></a>
-- MAGIC
-- MAGIC ## 1/ Ingest data with Lakeflow Connect
-- MAGIC
-- MAGIC
-- MAGIC Lakeflow Connect offers built-in data ingestion connectors for popular SaaS applications, databases and file sources, such as Salesforce, Workday, and SQL Server to build incremental data pipelines at scale, fully integrated with Databricks.
-- MAGIC
-- MAGIC
-- MAGIC ## 2/ Prepare and transform your data with SDP
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p style="min-height: 65px;">
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/diganparikh-dp/Images/refs/heads/main/Icons/LakeFlow%20Connect.jpg"/>
-- MAGIC       <strong>Efficient end-to-end ingestion</strong> <br/>
-- MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/diganparikh-dp/Images/refs/heads/main/Icons/LakeFlow%20Pipelines.jpg"/>
-- MAGIC       <strong>Flexible and easy setup</strong> <br/>
-- MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p style="min-height: 65px;">
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/>
-- MAGIC       <strong>Trust your data</strong> <br/>
-- MAGIC       With built-in orchestration, quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Building an SDP pipeline to analyze and reduce churn
-- MAGIC
-- MAGIC In this example, we'll implement an end-to-end Spark Declarative Pipeline consuming our customers information. We'll use the medallion architecture but we could build star schema, data vault or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our customer churn prediction.
-- MAGIC
-- MAGIC This information will then be used to build our DBSQL dashboard to track customer behavior and churn.
-- MAGIC
-- MAGIC Let's implement the following flow:
-- MAGIC
-- MAGIC <div><img width="1100px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-churn-de.png?raw=true"/></div>
-- MAGIC
-- MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-churn-prediction) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your SDP Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="sdp-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Churn SDP pipeline</a> to see it in action.<br/>
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

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
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-churn-de-small-1.png?raw=true" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC Ingesting data from stream sources can be challenging. In this example we'll incrementally load the files from our cloud storage, only getting the new ones (in near real-time or triggered every X hours).
-- MAGIC
-- MAGIC Note that while our streaming data is added to our cloud storage, we could easily ingest from kafka directly : `.format(kafka)`
-- MAGIC
-- MAGIC Auto-loader provides for you:
-- MAGIC
-- MAGIC - Schema inference and evolution
-- MAGIC - Scalability handling millions of files
-- MAGIC - Simplicity: just define your ingestion folder, Databricks takes care of the rest!
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('data-ingestion')`
-- MAGIC
-- MAGIC Let's use it in our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/01-bronze.sql]($./transformations/01-bronze.sql) notebook to review the SQL queries ingesting the raw data and creating our bronze layer.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 3/ Silver layer: Clean and prepare data
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-churn-de-small-2.png?raw=true" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC The next layer often called silver is consuming **incremental** data from the bronze layer, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different fields to enforce and track our Data Quality. This will ensure that our dashboards are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced SDP capabilities run `dbdemos.install('pipeline-bike')` or `dbdemos.install('declarative-pipeline-cdc')` for CDC/SCD Type 2 examples.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/02-silver.sql]($./transformations/02-silver.sql) notebook to review the SQL queries creating our clean silver tables.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 4/ Gold layer: Aggregate features and apply ML model
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-churn-de-small-3.png?raw=true" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC We're now ready to create the features required for our Churn prediction.
-- MAGIC
-- MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, such as:
-- MAGIC
-- MAGIC * last command date
-- MAGIC * number of items bought
-- MAGIC * number of actions in our website
-- MAGIC * device used (iOS/iPhone)
-- MAGIC * ...
-- MAGIC
-- MAGIC Our Data scientist team has built a churn prediction model using Auto ML and saved it into Databricks Model registry.
-- MAGIC
-- MAGIC One of the key values of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline.
-- MAGIC
-- MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstracts that for us.
-- MAGIC
-- MAGIC All we have to do is load the model, and call it as a SQL function (or python)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/03-gold.sql]($./transformations/03-gold.sql) notebook to review the SQL queries creating our features and predictions.
-- MAGIC
-- MAGIC The ML model is loaded as a UDF in [transformations/04-churn-UDF.py]($./transformations/04-churn-UDF.py)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC Our <a dbdemos-pipeline-id="sdp-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Spark Declarative Pipeline</a> is now ready using purely SQL. We have an end-to-end cycle, and our ML model has been integrated seamlessly by our Data Engineering team.
-- MAGIC
-- MAGIC
-- MAGIC For more details on model training, open the [model training notebook]($../04-Data-Science-ML/04.1-automl-churn-prediction)
-- MAGIC
-- MAGIC Our final dataset includes our ML prediction for our Churn prediction use-case.
-- MAGIC
-- MAGIC We are now ready to build our dashboards to track customer behavior and churn.
-- MAGIC
-- MAGIC <img style="float: left; margin-right: 50px;" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-churn-prediction.png?raw=true" />
-- MAGIC
-- MAGIC <img width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-churn.png?raw=true"/>
-- MAGIC
-- MAGIC <a dbdemos-dashboard-id="churn-universal" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1'  target="_blank">Open the DBSQL Dashboard</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC
-- MAGIC Jump to the [Governance with Unity Catalog notebook]($../02-Data-governance/02.1-UC-data-governance-security-churn) or [Go back to the introduction]($../00-churn-introduction-lakehouse)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optional: Checking your data quality metrics with SDP
-- MAGIC SDP tracks all of your data quality metrics. You can leverage the expectations directly as SQL tables with Databricks SQL to track your expectation metrics and send alerts as required. This lets you build the following dashboards:
-- MAGIC
-- MAGIC <img width="1000" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-dlt-stat.png?raw=true">
-- MAGIC
-- MAGIC <a dbdemos-dashboard-id="sdp-quality-stat" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Data Quality Dashboard</a>
