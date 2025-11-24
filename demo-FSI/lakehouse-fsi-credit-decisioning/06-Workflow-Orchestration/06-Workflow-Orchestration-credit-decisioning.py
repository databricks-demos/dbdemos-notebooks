# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Deploying and orchestrating the full workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="550px" src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/lakehouseDAIWTusecases.jpg" />
# MAGIC
# MAGIC All our assets are ready. We now need to define when we want our SDP pipeline to kick in and refresh the tables.
# MAGIC
# MAGIC One option is to switch SDP pipeline in continuous mode to have a streaming pipeline, providing near-realtime insight.
# MAGIC
# MAGIC An alternative is to wakeup the SDP pipeline every X hours, ingest the new data (incremental) and shut down all your compute. 
# MAGIC
# MAGIC This is a simple configuration offering a tradeoff between uptime and ingestion latencies.
# MAGIC
# MAGIC In our case, we decided that the best trade-off is to ingest new data every hours:
# MAGIC
# MAGIC - Start the SDP pipeline to ingest new data and refresh our tables
# MAGIC - Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC - Retrain our model to include the lastest date and capture potential behavior change
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fworkflow&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Orchestrating our Credit Decisioning pipeline with Databricks Workflows
# MAGIC
# MAGIC With Databricks Lakehouse we do not need any external orchestrators. We can use [Workflows](/#job/list) (available on the left menu) to orchestrate our Credit Decisioning and Scoring pipelines with just a few clicks.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ###  Orchestrate anything anywhere
# MAGIC With workflow, you can run diverse workloads for the full data and AI lifecycle on any cloud. Orchestrate Spark Declarative Pipelines and Jobs for SQL, Spark, notebooks, dbt, ML models and more.
# MAGIC
# MAGIC ### Simple - Fully managed
# MAGIC Remove operational overhead with a fully managed orchestration service, so you can focus on your workflows not on managing your infrastructure.
# MAGIC
# MAGIC ### Proven reliability
# MAGIC Have full confidence in your workflows leveraging our proven experience running tens of millions of production workloads daily across AWS, Azure and GCP.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### The workflow created as part of the Credit Decisioning process will orchestrate the following main tasks (as seen in the visualization below also): 
# MAGIC
# MAGIC <p></p>
# MAGIC
# MAGIC * Set up data sources and feature tables
# MAGIC * Ingest credit bureau data and banking internal customer data with Spark Declarative Pipelines
# MAGIC * Secure your tables, lineage, audit logs, and set up encryption
# MAGIC * Create Feature Engineering pipeline
# MAGIC * Set up AutoML configuration and execute a run
# MAGIC * Set up Batch Scoring for credit decisions
# MAGIC * Create explainability and fairness for decisions
# MAGIC * Set up sample Data warehousing queries and a decisions dashboard 
# MAGIC
# MAGIC <a dbdemos-workflow-id="credit-job" href="/#job/104444623965854">Click here to access your Workflow job</a>, it was setup when you installed your demo.
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/Workflows.png" />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating your workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-workflow.png" />
# MAGIC
# MAGIC A Databricks Workflow is composed of Tasks.
# MAGIC
# MAGIC Each task can trigger a specific job:
# MAGIC
# MAGIC * Spark Declarative Pipelines
# MAGIC * SQL query / dashboard
# MAGIC * Model retraining / inference
# MAGIC * Notebooks
# MAGIC * dbt
# MAGIC * ...
# MAGIC
# MAGIC In this example, can see our 3 tasks:
# MAGIC
# MAGIC * Start the SDP pipeline to ingest new data and refresh our tables
# MAGIC * Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC * Retrain our Churn model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Monitoring your runs
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-workflow-monitoring.png" />
# MAGIC
# MAGIC Once your workflow is created, we can access historical runs and receive alerts if something goes wrong!
# MAGIC
# MAGIC In the screenshot, we can see that our workflow had multiple errors, with different runtimes, and ultimately got fixed.
# MAGIC
# MAGIC Workflow monitoring includes errors, abnormal job duration and more advanced control!
