# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Data engineering with Databricks - Building our C360 database
# MAGIC
# MAGIC Building a C360 database requires to ingest multiple datasources.  
# MAGIC
# MAGIC It's a complex process requiring batch loads and streaming ingestion to support real-time insights, used for personalization and marketing targeting among other.
# MAGIC
# MAGIC Ingesting, transforming and cleaning data to create clean SQL tables for our downstream user (Data Analysts and Data Scientists) is complex.
# MAGIC
# MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
# MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
# MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
# MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
# MAGIC       73%
# MAGIC     </div>
# MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">of enterprise data goes unused for analytics and decision making</div>
# MAGIC   </div>
# MAGIC   <div style="color: #bfbfbf; padding-top: 5px">Source: Forrester</div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John, as Data engineer, spends immense time….
# MAGIC
# MAGIC
# MAGIC * Hand-coding data ingestion & transformations and dealing with technical challenges:<br>
# MAGIC   *Supporting streaming and batch, handling concurrent operations, small files issues, GDPR requirements, complex DAG dependencies...*<br><br>
# MAGIC * Building custom frameworks to enforce quality and tests<br><br>
# MAGIC * Building and maintaining scalable infrastructure, with observability and monitoring<br><br>
# MAGIC * Managing incompatible governance models from different systems
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC This results in **operational complexity** and overhead, requiring expert profile and ultimatly **putting data projects at risk**.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.3-DLT-churn-python&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
# MAGIC
# MAGIC <img style="float: right" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-1.png" />
# MAGIC
# MAGIC In this notebook, we'll work as a Data Engineer to build our c360 database. <br>
# MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
# MAGIC
# MAGIC We have 3 data sources sending new files in our blob storage (`/demos/retail/churn/`) and we want to incrementally load this data into our Datawarehousing tables:
# MAGIC
# MAGIC - Customer profile data *(name, age, adress etc)*
# MAGIC - Orders history *(what our customer bough over time)*
# MAGIC - Streaming Events from our application *(when was the last time customers used the application, typically a stream from a Kafka queue)*
# MAGIC
# MAGIC
# MAGIC Databricks simplify this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
# MAGIC
# MAGIC DLT allows Data Analysts to create advanced pipeline with plain SQL.
# MAGIC
# MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC
# MAGIC <div>
# MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
# MAGIC       <strong>Accelerate ETL development</strong> <br/>
# MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
# MAGIC       <strong>Remove operational complexity</strong> <br/>
# MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC     </p>
# MAGIC   </div>
# MAGIC   <div style="width: 48%; float: left">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
# MAGIC       <strong>Trust your data</strong> <br/>
# MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
# MAGIC       <strong>Simplify batch and streaming</strong> <br/>
# MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC     </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br style="clear:both">
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
# MAGIC
# MAGIC ## Delta Lake
# MAGIC
# MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake table. Delta Lake is an open storage framework for reliability and performance.<br>
# MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
# MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Building a Delta Live Table pipeline to analyze and reduce churn
# MAGIC
# MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our customers information. We'll use the medaillon architecture but we could build star schema, data vault or any other modelisation.
# MAGIC
# MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our customer churn prediction.
# MAGIC
# MAGIC This information will then be used to build our DBSQL dashboard to track customer behavior and churn.
# MAGIC
# MAGIC Let's implement the following flow: 
# MAGIC  
# MAGIC <div><img width="1100px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de.png"/></div>
# MAGIC
# MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-churn-prediction) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

# COMMAND ----------

# MAGIC %md
# MAGIC Your DLT Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Churn Delta Live Table pipeline</a> to see it in action.<br/>
# MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
# MAGIC </div>
# MAGIC   
# MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC
# MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
# MAGIC
# MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/retail/churn/...`. 

# COMMAND ----------

# DBTITLE 1,Ingest raw app events stream in incremental mode 
import dlt
from pyspark.sql import functions as F

@dlt.create_table(comment="Application events and sessions")
@dlt.expect("App events correct schema", "_rescued_data IS NULL")
def churn_app_events():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/Volumes/main__build/dbdemos_retail_c360/c360/events"))

# COMMAND ----------

# DBTITLE 1,Ingest raw orders from ERP
@dlt.create_table(comment="Spending score from raw data")
@dlt.expect("Orders correct schema", "_rescued_data IS NULL")
def churn_orders_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/Volumes/main__build/dbdemos_retail_c360/c360/orders"))

# COMMAND ----------

# DBTITLE 1,Ingest raw user data
@dlt.create_table(comment="Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution")
@dlt.expect("Users correct schema", "_rescued_data IS NULL")
def churn_users_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/Volumes/main__build/dbdemos_retail_c360/c360/users"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
# MAGIC </div>
# MAGIC
# MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
# MAGIC
# MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
# MAGIC
# MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
# MAGIC
# MAGIC These tables are clean and ready to be used by the BI team!

# COMMAND ----------

# DBTITLE 1,Clean and anonymise User data
@dlt.create_table(comment="User data cleaned and anonymized for analysis.")
@dlt.expect_or_drop("user_valid_id", "user_id IS NOT NULL")
def churn_users():
  return (dlt
          .read_stream("churn_users_bronze")
          .select(F.col("id").alias("user_id"),
                  F.sha1(F.col("email")).alias("email"), 
                  F.to_timestamp(F.col("creation_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"), 
                  F.to_timestamp(F.col("last_activity_date"), "MM-dd-yyyy HH:mm:ss").alias("last_activity_date"), 
                  F.initcap(F.col("firstname")).alias("firstname"), 
                  F.initcap(F.col("lastname")).alias("lastname"), 
                  F.col("address"), 
                  F.col("canal"), 
                  F.col("country"),
                  F.col("gender").cast("int").alias("gender"),
                  F.col("age_group").cast("int").alias("age_group"), 
                  F.col("churn").cast("int").alias("churn")))

# COMMAND ----------

# DBTITLE 1,Clean orders
@dlt.create_table(comment="Order data cleaned and anonymized for analysis.")
@dlt.expect_or_drop("order_valid_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("order_valid_user_id", "user_id IS NOT NULL")
def churn_orders():
  return (dlt
          .read_stream("churn_orders_bronze")
          .select(F.col("amount").cast("int").alias("amount"),
                  F.col("id").alias("order_id"),
                  F.col("user_id"),
                  F.col("item_count").cast("int").alias("item_count"),
                  F.to_timestamp(F.col("transaction_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"))
         )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ Aggregate and join data to create our ML features
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
# MAGIC </div>
# MAGIC
# MAGIC We're now ready to create the features required for our Churn prediction.
# MAGIC
# MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, sucj as:
# MAGIC
# MAGIC * last command date
# MAGIC * number of item bought
# MAGIC * number of actions in our website
# MAGIC * device used (ios/iphone)
# MAGIC * ...

# COMMAND ----------

@dlt.create_table(comment="Final user table with all information for Analysis / ML")
def churn_features():
  churn_app_events_stats_df = (dlt
          .read("churn_app_events")
          .groupby("user_id")
          .agg(F.first("platform").alias("platform"),
               F.count('*').alias("event_count"),
               F.count_distinct("session_id").alias("session_count"),
               F.max(F.to_timestamp("date", "MM-dd-yyyy HH:mm:ss")).alias("last_event"))
                              )
  
  churn_orders_stats_df = (dlt
          .read("churn_orders")
          .groupby("user_id")
          .agg(F.count('*').alias("order_count"),
               F.sum("amount").alias("total_amount"),
               F.sum("item_count").alias("total_item"),
               F.max("creation_date").alias("last_transaction"))
         )
  
  return (dlt
          .read("churn_users")
          .join(churn_app_events_stats_df, on="user_id")
          .join(churn_orders_stats_df, on="user_id")
          .withColumn("days_since_creation", F.datediff(F.current_timestamp(), F.col("creation_date")))
          .withColumn("days_since_last_activity", F.datediff(F.current_timestamp(), F.col("last_activity_date")))
          .withColumn("days_last_event", F.datediff(F.current_timestamp(), F.col("last_event")))
         )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5/ Enriching the gold data with a ML model
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-4.png"/>
# MAGIC </div>
# MAGIC
# MAGIC Our Data scientist team has build a churn prediction model using Auto ML and saved it into Databricks Model registry. 
# MAGIC
# MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline. 
# MAGIC
# MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

# COMMAND ----------

# DBTITLE 1,Load the model as SQL function
import mlflow
#                                                                                                     Stage/version  
#                                                                                   Model name               |        
#                                                                                       |                    |        
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/main__build.dbdemos_retail_c360.dbdemos_customer_churn@prod", "int")
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# DBTITLE 1,Call our model and predict churn in our pipeline
model_features = predict_churn_udf.metadata.get_input_schema().input_names()

@dlt.create_table(comment="Customer at risk of churn")
def churn_prediction():
  return (dlt
          .read('churn_features')
          .withColumn('churn_prediction', predict_churn_udf(*model_features)))

# COMMAND ----------

# MAGIC %md ## Our pipeline is now ready!
# MAGIC
# MAGIC As you can see, building Data Pipeline with databricks let you focus on your business implementation while the engine solves all hard data engineering work for you.
# MAGIC
# MAGIC Open the <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Churn Delta Live Table pipeline</a> and click on start to visualize your lineage and consume the new data incrementally!

# COMMAND ----------

# MAGIC %md
# MAGIC # Next: secure and share data with Unity Catalog
# MAGIC
# MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
# MAGIC
# MAGIC Jump to the [Governance with Unity Catalog notebook]($../00-churn-introduction-lakehouse) or [Go back to the introduction]($../00-churn-introduction-lakehouse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Checking your data quality metrics with Delta Live Tables
# MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC
# MAGIC <a dbdemos-dashboard-id="dlt-quality-stat" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Data Quality Dashboard</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Building our first business dashboard with Databricks SQL
# MAGIC
# MAGIC Our data now is available! we can start building dashboards to get insights from our past and current business.
# MAGIC
# MAGIC <img style="float: left; margin-right: 50px;" width="500px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png" />
# MAGIC
# MAGIC <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
# MAGIC
# MAGIC <a dbdemos-dashboard-id="churn-prediction" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Data Quality Dashboard</a>
