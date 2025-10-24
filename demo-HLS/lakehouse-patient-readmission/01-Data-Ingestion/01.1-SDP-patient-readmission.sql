-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Ingesting and preparing data to build the OMOP 5.3.1 Lakehouse
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-flow-1.png" style="float: left; margin-right: 30px; margin-top:10px" width="650px" />
-- MAGIC
-- MAGIC Our first step in reducing patient readmission risk is to ingest and prepare data from multiple external sources as simple tables our downstream Analysts can leverage. To do so, we will leverage the OMOP data model.
-- MAGIC
-- MAGIC The OMOP database is a Common Data Model to harmonize data analysis required for medical product safety surveillance, comparative effectiveness, quality of care, and patient-level predictive modeling.
-- MAGIC
-- MAGIC Databricks is uniquely positioned to create a leverage this model: not only it provides capabilities to build, ingest and transform the data, but it also let your Data Analyst and Data Scientist team to run advanced analysis on top of it.
-- MAGIC
-- MAGIC <br style="clear: both">
-- MAGIC
-- MAGIC ## Implementing the OMOP model
-- MAGIC
-- MAGIC <img src="https://ohdsi.github.io/TheBookOfOhdsi/images/CommonDataModel/cdmDiagram.png" width="400px" style="float: right; margin-left: 50px" >
-- MAGIC
-- MAGIC The OMOP database contains several tables.
-- MAGIC
-- MAGIC - Clinical Data Tables (CDT)
-- MAGIC - Health System Data Tables (HSDT)
-- MAGIC - Health Economics Data Tables (HEDT)
-- MAGIC - Standardized Derived Elements (SDE)
-- MAGIC - Metadata Tables (MT)
-- MAGIC - Vocabulary Tables (VT)
-- MAGIC
-- MAGIC In this demo, we will consume raw data artificially generated from Synthea and apply transformations to translate them as OMOP schema.
-- MAGIC
-- MAGIC *To keep this demo simple, we will only implement a subste of the OMOP data model*
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.1-DLT-patient-readmission-SQL&demo_name=lakehouse-patient-readmission&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Spark Declarative Pipelines: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our OMOP CDM database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC The lakehouse makes it easy to ingest from any external or internal HLS datasources, such as HL7 FHIR, EHR, ioMT, SQL databases). For our demo, our data will be files received in a cloud blob storage in different format. We will then apply a couple of transformations while ensuring data quality.
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

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC
-- MAGIC The SDP pipeline was started for you! Click to <a dbdemos-pipeline-id="sdp-patient-readmission" href="#joblist/pipelines/ff2fd2cb-733b-4166-85ed-a34b84129a35" target="_blank">open your Pipeline</a> and review its execution.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Building a Spark Declarative Pipelines pipeline to ingest and prepare HLS data with the OMOP model
-- MAGIC
-- MAGIC In this example, we'll implement an end-to-end SDP pipeline consuming the aforementioned information. We'll use the medaillon architecture but we could build star schema, data vault, or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader, clean and enrich this information.
-- MAGIC
-- MAGIC This information will then be used to build our cohorts and build SQL dashboard to analyze our patients.
-- MAGIC
-- MAGIC Let's implement the following flow: 
-- MAGIC
-- MAGIC  <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-0.png" width="1000px" />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1/ Data Exploration
-- MAGIC All Data projects start with some exploration. Open the [explorations/sample_exploration]($./explorations/sample_exploration) notebook to get started and discover the data made available to you
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Loading our data using Databricks Autoloader (read_files)
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-1.png"/>
-- MAGIC
-- MAGIC   
-- MAGIC Our raw files are available in our landing zone as CSV. We need to ingest them at scale, while handling schema inference and evolution.
-- MAGIC Databricks Autoloader makes this very easy.
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/01-bronze.sql]($./transformations/01-bronze.sql) notebook to review the SQL queries ingesting the raw data and creating our bronze layer.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-2.png"/>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an TODO [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced SDP capabilities run `dbdemos.install('pipeline-bike')` or `dbdemos.install('declarative-pipeline-cdc')` for CDC/SCDT2 example.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [transformations/01-silver.sql]($./transformations/01-silver.sql) notebook to review the SQL queries creating our features and our training dataset

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 4/ Final tables for our Data Analysis and ML model
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-3.png"/>
-- MAGIC
-- MAGIC Finally, let's cbuild our final tables containing clean data that we'll be able to use of to build our cohorts and predict patient risks.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Open the [transformations/01-gold.sql]($./transformations/01-gold.sql) notebook to review the SQL queries creating our features and our training dataset

-- COMMAND ----------

-- MAGIC %md ## Our pipeline is now ready!
-- MAGIC
-- MAGIC As you can see, building Data Pipeline with databricks let you focus on your business implementation while the engine solves all hard data engineering work for you.
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC
-- MAGIC Open the <a dbdemos-pipeline-id="sdp-patient-readmission" href="#joblist/pipelines/ff2fd2cb-733b-4166-85ed-a34b84129a35" target="_blank">OMOP data model Spark Declarative Pipelines pipeline</a> and click on start to visualize your lineage and consume the new data incrementally!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC
-- MAGIC Jump to the [Governance with Unity Catalog notebook]($../02-Data-Governance/02-Data-Governance-patient-readmission) or [Go back to the introduction]($../00-patient-readmission-introduction)
