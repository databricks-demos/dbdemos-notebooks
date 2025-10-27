-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify Ingestion and Transformation with Spark Declarative Pipelines
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our Credit Decisioning database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC We have four data sources sending new files in our blob storage and we want to incrementally load this data into our Data warehousing tables:
-- MAGIC
-- MAGIC - **Internal banking** data *(KYC, accounts, collections, applications, relationship)* come from the bank's internal relational databases and is ingested *once a day* through a CDC pipeline,
-- MAGIC - **Credit Bureau** data (usually in XML or CSV format and *accessed monthly* through API) comes from government agencies (such as a central banks) and contains a lot of valuable information for every customer. We also use this data to re-calculate whether a user has defaulted in the past 60 days,
-- MAGIC - **Partner** data - used to augment the internal banking data and ingested *once a week*. In this case we use telco data in order to further evaluate the character and creditworthiness of banking customers,
-- MAGIC - **Fund transfer** are the banking transactions (such as credit card transactions) and are *available real-time* through Kafka streams.
-- MAGIC
-- MAGIC
-- MAGIC ## Spark Declarative Pipelines: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC
-- MAGIC Databricks simplifies this task with Spark Declarative Pipelines (SDP) by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC SDP allows Data Analysts to create advanced pipeline with plain SQL.
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
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. Delta Lake is an open storage framework for reliability and performance.<br>
-- MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
-- MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01-DLT-Internal-Banking-Data-SQL&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Building a Spark Declarative Pipelines pipeline to analyze consumer credit
-- MAGIC
-- MAGIC In this example, we'll implement an end-to-end SDP pipeline consuming the aforementioned information. We'll use the medaillon architecture but we could build star schema, data vault, or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our credit decisioning prediction.
-- MAGIC
-- MAGIC This information will then be used to build our DBSQL dashboard to create credit scores, decisioning, and risk.
-- MAGIC
-- MAGIC Let's implement the following flow:
-- MAGIC
-- MAGIC <div><img width="1000px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_0.png" /></div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your SDP Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="sdp-fsi-credit-decisioning" href="#joblist/pipelines" target="_blank">Spark Declarative Pipelines pipeline</a> to see it in action.<br/>
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 1/ Data Exploration
-- MAGIC
-- MAGIC All Data projects start with some exploration. Open the [/explorations/sample_exploration]($./explorations/sample_exploration) notebook to get started and discover the data made available to you

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_1.png"/>
-- MAGIC
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC We'll ingest the following data sources:
-- MAGIC
-- MAGIC 1. **Credit Bureau** - Credit history and creditworthiness information from government agencies (monthly)
-- MAGIC 2. **Customer** - Customer-related data from internal KYC processes (daily)
-- MAGIC 3. **Relationship** - Bank-customer relationship data (daily)
-- MAGIC 4. **Account** - Customer account information (daily)
-- MAGIC 5. **Fund Transfer** - Real-time payment transactions through Kafka streams
-- MAGIC 6. **Telco** - Partner data to augment creditworthiness evaluation (weekly)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [/transformations/01-bronze.sql]($./transformations/01-bronze.sql) file to review the SQL code ingesting the raw data and creating our bronze layer.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_2.png"/>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced SDP capabilities run `dbdemos.install('pipeline-bike')` or `dbdemos.install('declarative-pipeline-cdc')` for CDC/SCDT2 example.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!
-- MAGIC
-- MAGIC We'll create cleaned silver tables for:
-- MAGIC - **Fund Transfer** - Link transactions to customer IDs through account joins
-- MAGIC - **Customer** - Join customer with relationship data and extract birth year
-- MAGIC - **Account** - Calculate account aggregations per customer (count, average balance)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [/transformations/02-silver.sql]($./transformations/02-silver.sql) file to review the SQL code creating our clean silver tables.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### 4/ Aggregation layer for analytics & ML
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_3.png"/>
-- MAGIC
-- MAGIC We curate all the tables in Delta Lake using Spark Declarative Pipelines so we can apply all the joins, masking, and data constraints in real-time. Data scientists can now use these datasets to built high-quality models, particularly to predict credit worthiness. Because we are masking sensitive data as part of Unity Catalog capabilities, we are able to confidently expose the data to many downstream users from data scientists to data analysts and business users.
-- MAGIC
-- MAGIC We'll create gold tables with:
-- MAGIC - **Credit Bureau** - Clean credit bureau data with data quality constraints
-- MAGIC - **Fund Transfer** - Aggregate transaction metrics over 3, 6, and 12 month windows for behavioral analysis
-- MAGIC - **Telco** - Partner data enriched with customer information
-- MAGIC - **Customer** - Comprehensive customer profile with account aggregations
-- MAGIC - **Customer Secured** - Masked PII view using dynamic encryption for data science users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [/transformations/03-gold.sql]($./transformations/03-gold.sql) file to review the SQL code creating our enriched gold tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that we have ingested all these various sources of data, we can jump to the:
-- MAGIC
-- MAGIC * [Governance with Unity Catalog notebook]($../../02-Data-Governance/02-Data-Governance-credit-decisioning) to see how to grant fine-grained access to every user and persona and explore the **data lineage graph**,
-- MAGIC * [Feature Engineering notebook]($../../03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning) and start creating features for our machine learning models,
-- MAGIC * Go back to the [Introduction]($../../00-Credit-Decisioning).
