# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Orchestrating and running dbt Pipelines on Databricks
# MAGIC
# MAGIC In this demo, we'll show you how to run dbt pipelines with Databricks Lakehouse.
# MAGIC
# MAGIC ## What's dbt
# MAGIC
# MAGIC <img style="float: right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/partners/dbt/dbt-logo.png">
# MAGIC
# MAGIC dbt is a transformation workflow that helps you get more work done while producing higher quality results. You can use dbt to modularize and centralize your analytics code. 
# MAGIC
# MAGIC dbt helps you write data transformations (typically in SQL) with templating capabilities. You can then easily test, explore and document your data transformations.
# MAGIC
# MAGIC ## dbt + Databricks Lakehouse
# MAGIC
# MAGIC
# MAGIC Once your dbt pipeline is written, dbt will compile and generate SQL queries. <br/>
# MAGIC The SQL Transformation are then sent to your Databricks warehouse endpoint which will process the data available in your Lakehouse and update your data accordingly.<br/>
# MAGIC By leveraging Databricks SQL warehouse, you will get the best TCO for ETL, leveraging our engine (photon) for blazing fast transformation. 
# MAGIC
# MAGIC
# MAGIC ## Ingesting data for C360 platfom
# MAGIC
# MAGIC This demo replicate the Spark Declarative Pipelines ingestion pipeline available in the Lakehouse C360 platform demo `dbdemos.install('lakehouse-retail-c360')`
# MAGIC
# MAGIC In this dbt pipeline, we'll work as a Data Engineer to build our c360 database. We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
# MAGIC
# MAGIC We have 3 data sources sending new files in our blob storage (/demos/retail/churn/) and we want to incrementally load this data into our Datawarehousing tables:
# MAGIC
# MAGIC - Customer profile data (name, age, adress etc)
# MAGIC - Orders history (what our customer bough over time)
# MAGIC - Streaming Events from our application (when was the last time customers used the application, typically a stream from a Kafka queue)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## DBT as a Databricks Workflow task
# MAGIC
# MAGIC <img style="float: right; margin-left: 15px" height="500px"  src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/partners/dbt/dbt-task.png"/>
# MAGIC
# MAGIC Databricks Workflow, the lakehouse orchestration tool, has a native integration with dbt. <br />
# MAGIC You can easily add a new task launching a DBT pipeline within your ETL workflow.
# MAGIC
# MAGIC To do so, simply select the "dbt" type in the task configuration and select which dbt CLI and command you want to run. 
# MAGIC
# MAGIC You can then select the SQL Warehouse you want to use to run the transformations. 
# MAGIC
# MAGIC Databricks will handle the rest.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Adding an ingestion step before dbt pipeline: "Extract" part of your ETL
# MAGIC
# MAGIC DBT doesn't offer direct ways to ingest data from different sources. Typical ingestion source can be:
# MAGIC
# MAGIC - files delivered on blob storage (S3/ADLS/GCS...)
# MAGIC - Message queue (kafka)
# MAGIC - External databases...
# MAGIC
# MAGIC Databricks lakehouse solves this gap easily. You can leverage all our connectors, including partners (ex: Fivetran) to incrementally load new incoming data.
# MAGIC
# MAGIC In this demo, our workflow will have 3 tasks:
# MAGIC
# MAGIC  - 01: task to incrementally extract files from a blob storage using Databricks Autoloader and save this data in our raw layer.<br/>
# MAGIC  - 02: run the dbt pipeline to do the transformations, consuming the data from these raw tables<br/>
# MAGIC  - 03: final task for the final operation (ex: ML predictions, or refreshing a dashboard)
# MAGIC
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/partners/dbt/dbt-databricks-workflow.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Accessing the dbt pipeline & demo content
# MAGIC
# MAGIC ### A workflow with all the steps have been created for you
# MAGIC
# MAGIC <a dbdemos-workflow-id="dbt" href="/#job/104444623965854">Click here to access your Workflow job</a>, it was setup and has been started when you installed your demo.
# MAGIC
# MAGIC ### The dbt project has been loaded as part of your repos
# MAGIC
# MAGIC Because dbt integration works with git repos, we loaded the [demo dbt repo](https://github.com/databricks-demos/dbt-databricks-c360) in your repo folder : 
# MAGIC
# MAGIC <a dbdemos-repo-id="dbt-databricks-c360" href="/#workspace/PLACEHOLDER_CHANGED_AT_INSTALL_TIME/README.md">Click here to explore the dbt pipeline installed as a repository</a><br/>
# MAGIC The workflow has been setup to use this repo. 

# COMMAND ----------

# MAGIC %md 
# MAGIC # Going further with dbt

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Setting up dbt Cloud + Databricks
# MAGIC
# MAGIC
# MAGIC <iframe style="float:right; margin-left: 20px" width="560" height="315" src="https://www.youtube.com/embed/12wOO88ZEJo" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
# MAGIC
# MAGIC dbt cloud helps team developping dbt pipelines faster. Once your pipeline is ready in dbt cloud, you can easily launch it in your Databricks Lakehouse with one of the best TCO.
# MAGIC
# MAGIC The integration between dbt Cloud and Databricks is available out of the boxin the Partner Connect menu (bottom left of your screen).
# MAGIC
# MAGIC You'll find all the required information to setup the connection between dbt Cloud and Databricks warehouse endpoints
# MAGIC
# MAGIC In addition, you can watch this video going through the setup steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### A note on Spark Declarative Pipelines
# MAGIC
# MAGIC Spark Declarative Pipelines is a declarative framework build by Databricks. It can also be used to build data pipeline within Databricks and provides among other:
# MAGIC
# MAGIC  - Ingestion capabilities to ingest data from any sources within your pipeline (no need for external step)
# MAGIC  - Out of the box streaming capabilities for near-realtime inferences
# MAGIC  - Incremental support: ingest and transform new data as they come 
# MAGIC  - Advanced capabilities (simple Change Data Capture, SCDT2 etc)
# MAGIC
# MAGIC If you wish to know more about dlt, install the SDP demos: `dbdemos.install('sdp-loan')`
