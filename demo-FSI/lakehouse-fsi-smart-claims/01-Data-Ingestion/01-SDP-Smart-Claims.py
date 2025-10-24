# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Policy, Claims & Telematics SDP Ingestion pipeline
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-0.png?raw=true" style="float: right" width="800px">
# MAGIC
# MAGIC Using <b>Spark Declarative Pipelines</b> (SDP) for ETL helps simplify and operationalize the pipeline with its support for autoloader, data quality via constraints, efficient auto-scaling for streaming workloads, resiliency via restart on failure, execution of administrative operations among others.
# MAGIC
# MAGIC We'll show how Databricks makes it easy to incrementally ingest and transform our incoming claims data.
# MAGIC
# MAGIC For more advanced SDP capabilities run `dbdemos.install('pipeline-bike')` or `dbdemos.install('declarative-pipeline-cdc')` for CDC/SCD Type 2 example.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Ingestion made it easy with Databricks Assistant
# MAGIC
# MAGIC Databricks Data Intelligent Platform simplify our journey, empowering Data Analyst to do more and build robust Data Pipelines.
# MAGIC
# MAGIC Questions can be asked in plain text to our assistant, which will suggest how to build the pipeline, fix bug and offer potential improvements.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01.1-DLT-Ingest-Policy-Claims&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This demo started for you a <a dbdemos-pipeline-id="sdp-fsi-smart-claims" href="#joblist/pipelines/bf6b21bb-ff10-480c-bdae-c8c91c76d065" target="_blank">SDP Pipeline</a> using this notebook! Explore it to see its execution out of the box.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building a Spark Declarative Pipeline for Smart Claims Processing
# MAGIC
# MAGIC In this example, we'll implement an end-to-end Spark Declarative Pipeline consuming our claims, policy, and telematics data. We'll use the medallion architecture but we could build star schema, data vault or any other modelisation.
# MAGIC
# MAGIC We'll incrementally load new data with the autoloader, enrich this information, and prepare it for downstream analysis and ML models.
# MAGIC
# MAGIC Let's implement the following flow:
# MAGIC
# MAGIC <div><img width="1100px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-0.png?raw=true"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1/ Data Exploration
# MAGIC
# MAGIC All Data projects start with some exploration. Open the [/01.1-sdp-python/explorations/sample_exploration]($./01.1-sdp-python/explorations/sample_exploration) notebook to get started and discover the data made available to you

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 2/ Ingest data: Bronze layer
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-1.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC The raw data is saved within our Unity Catalog Volume.
# MAGIC
# MAGIC Databricks makes it easy to incrementally ingest raw files and save them as a Delta Table using  `cloud_files`.
# MAGIC
# MAGIC In this notebook, we'll be using Python, but we could have declare the same transformation in pure SQL.  You could ask the assistant to convert it from python to SQL !
# MAGIC
# MAGIC Let's use it to our pipeline and ingest the raw JSON, CSV & Parquet data being delivered in our Unity Catalog Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [01.1-sdp-python/transformations/01-bronze.py]($./01.1-sdp-python/transformations/01-bronze.py) notebook to review the Python code ingesting the raw data and creating our bronze layer.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3/ Silver layer: Clean and transform data
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-2.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC Once our raw data are ingested, we can start adding some transformation and business logic to cleanup our data.
# MAGIC
# MAGIC These tables are typically made available to downstream teams, such as Data Scientists or Data Analysts.
# MAGIC
# MAGIC We'll:
# MAGIC - Aggregate telematics data by vehicle
# MAGIC - Clean and standardize policy data
# MAGIC - Clean and flatten claim data

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [01.1-sdp-python/transformations/02-silver.py]($./01.1-sdp-python/transformations/02-silver.py) notebook to review the Python code creating our clean silver tables.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 4/ Gold layer: Join data for analysis
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-3.png?raw=true" style="float: right" width="600px">
# MAGIC
# MAGIC Let's now join our policy and claim information within a single table.
# MAGIC
# MAGIC This curated dataset will be used for:
# MAGIC - Business Intelligence dashboards
# MAGIC - Machine Learning models for fraud detection
# MAGIC - Advanced analytics

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [01.1-sdp-python/transformations/03-gold.py]($./01.1-sdp-python/transformations/03-gold.py) notebook to review the Python code creating our joined gold table.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, we demonstrated how <b> policy, claims & telematics </b> data flow into the system at different speeds, in different formats, from different sources and are consolidated in the claims lakehouse to weave the story of what happened and why and what to do next.
# MAGIC
# MAGIC The gold layer also includes geolocation enrichment using the open-source geopy library to add latitude/longitude coordinates to policy addresses, providing better visualization capabilities for Claims Investigators.
# MAGIC
# MAGIC Go back to the [00-Smart-Claims-Introduction]($../00-Smart-Claims-Introduction) to explore AI and Data Visualization.
