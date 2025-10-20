# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Ingesting data from any sources with Databricks Data Intelligence Platform
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/di_platform_0.png" style="float: left; margin-right: 30px" width="600px" />
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC
# MAGIC With the Databricks Data Intelligence Platform, you can effortlessly ingest data from virtually any source, unifying your entire data estate into a single, intelligent foundation. Whether it's batch, streaming, or change data capture (CDC), we provide robust, scalable, and easy-to-use tools to bring all your data into the Lakehouse.
# MAGIC
# MAGIC Key capabilities include:
# MAGIC
# MAGIC - **Universal Connectivity**: Native connectors for enterprise applications (Salesforce, ServiceNow, Workday), databases (SQL Server, Oracle, PostgreSQL), cloud object storage (S3, ADLS, GCS), messaging queues (Kafka, Kinesis, Pub/Sub), and custom APIs.
# MAGIC
# MAGIC - **Automated & Incremental Ingestion**: Leverage powerful features like Auto Loader for efficient, incremental processing of new files as they arrive, and Lakeflow Connect for managed, serverless pipelines.
# MAGIC
# MAGIC - **Real-time Ready**: Seamlessly ingest and process high-throughput streaming data for immediate insights, real-time dashboards, and instant decision-making.
# MAGIC
# MAGIC - **Unified Governance**: Every ingested dataset is automatically governed by Unity Catalog, providing unified visibility, access control, lineage, and discovery across your entire data and AI assets.
# MAGIC
# MAGIC - **Simplified Data Engineering**: Build and manage data ingestion pipelines with ease using declarative frameworks like Spark Declarative Pipelines (formely known as SDP), reducing complexity and accelerating time to value.
# MAGIC
# MAGIC Break down data silos, accelerate your data and AI initiatives, and unlock the full potential of your data with the Databricks Data Intelligence Platform.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1/ Ingest from Business Applications with Lakeflow connect
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/product/data-ingestion/lakeflow-connect.png" style="float: left; margin-right: 30px; margin-top: 30px; margin-bottom: 30px;" width="600px" />
# MAGIC
# MAGIC **Databricks Lakeflow Connect**
# MAGIC Lakeflow Connect is Databricks Lakeflow’s ingestion tool that makes it easy to bring data from many sources—including apps like Salesforce, Workday, and ServiceNow, plus databases, cloud storage, and streaming services—into your Databricks Lakehouse.
# MAGIC
# MAGIC * Features an intuitive UI for fast setup
# MAGIC * Supports incremental and scalable, serverless ingestion
# MAGIC * Unifies governance with Unity Catalog for security and discoverability
# MAGIC
# MAGIC Want to try Lakeflow Connect? Check these Product Tours:
# MAGIC
# MAGIC [**Ingest from Salesforce:**](https://app.getreprise.com/launch/BXZjz8X/) Seamlessly integrates Salesforce CRM data (including custom objects and formula fields) with Databricks for analytics and AI.
# MAGIC
# MAGIC [**Ingest Workday Reports:**](https://app.getreprise.com/launch/ryNY32X/) Quickly ingest and manage Workday data, as well as databases, cloud, and local files, with simple pipeline configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2/ Read or ingest data in SQL with `read_file` and Spark Declarative Pipelines
# MAGIC
# MAGIC ### Instantly Access Any File with Databricks SQL's `read_files`
# MAGIC
# MAGIC **Unlock your data's potential, no matter where it lives or what format it's in.**
# MAGIC
# MAGIC The `read_files` function in Databricks SQL empowers you to directly query and analyze raw data files—from CSVs and JSONs to Parquet and more—stored in your cloud object storage or Unity Catalog volumes. Skip the complex setup and jump straight into insights.
# MAGIC
# MAGIC **Simply point, query, and transform.** `read_files` intelligently infers schemas, handles diverse file types, and integrates seamlessly with streaming tables for real-time ingestion. It's the fast, flexible way to bring all your files into the Databricks Lakehouse, accelerating your journey from raw data to actionable intelligence.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open [01-ingestion-with-sql-read_files]($./01-ingestion-with-sql-read_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Ingest Files with Databricks Auto Loader
# MAGIC
# MAGIC **Databricks Auto Loader** makes file ingestion simple and automatic. It incrementally detects and loads new data files from cloud storage into your Lakehouse—no manual setup required.
# MAGIC
# MAGIC - **Fully automated streaming:** New data is picked up and processed instantly as it arrives.
# MAGIC - **Exactly-once data integrity:** Ensures files are processed just once, even after failures.
# MAGIC - **Supports schema evolution:** Automatically adapts as your data evolves.
# MAGIC - **Works with many file formats:** Handles JSON, CSV, Parquet, Avro, and more.
# MAGIC - **Seamless integration:** Built to work with Spark Declarative Pipelines and streaming pipelines.
# MAGIC
# MAGIC Auto Loader lets you focus on getting insights from data—not on managing file arrivals or pipeline code.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open [02-Auto-loader-schema-evolution-Ingestion]($./02-Auto-loader-schema-evolution-Ingestion)
