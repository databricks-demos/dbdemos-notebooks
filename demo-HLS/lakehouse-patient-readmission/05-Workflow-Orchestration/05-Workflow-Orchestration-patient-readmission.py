# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Deploying and orchestrating the full workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-lakehouse-5.png" />
# MAGIC
# MAGIC All our assets are ready. We now need to define when we want our DLT pipeline to kick in and refresh the tables.
# MAGIC
# MAGIC One option is to switch DLT pipeline in continuous mode to have a streaming pipeline, providing near-realtime insight.
# MAGIC
# MAGIC An alternative is to wakeup the DLT pipeline every X hours, ingest the new data (incremental) and shut down all your compute. 
# MAGIC
# MAGIC This is a simple configuration offering a tradeoff between uptime and ingestion latencies.
# MAGIC
# MAGIC In our case, we decided that the best trade-off is to ingest new data every hours:
# MAGIC
# MAGIC - Start the DLT pipeline to ingest new data and refresh our tables
# MAGIC - Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC - Retrain our model to include the lastest date and capture potential behavior change
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05-Workflow-Orchestration-patient-readmission&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Orchestrating our Credit Patient readmission pipeline with Databricks Workflows
# MAGIC
# MAGIC With Databricks Lakehouse we do not need any external orchestrators. We can use [Workflows](/#job/list) (available on the left menu) to orchestrate our Credit Decisioning and Scoring pipelines with just a few clicks.
# MAGIC
# MAGIC ###  Orchestrate anything anywhere
# MAGIC With workflow, you can run diverse workloads for the full data and AI lifecycle on any cloud. Orchestrate Delta Live Tables and Jobs for SQL, Spark, notebooks, dbt, ML models and more.
# MAGIC
# MAGIC ### Simple - Fully managed
# MAGIC Remove operational overhead with a fully managed orchestration service, so you can focus on your workflows not on managing your infrastructure.
# MAGIC
# MAGIC ### Proven reliability
# MAGIC Have full confidence in your workflows leveraging our proven experience running tens of millions of production workloads daily across AWS, Azure and GCP.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### The workflow created as part of the Patient readmission demo will orchestrate the following tasks (as seen in the visualization below also): 
# MAGIC
# MAGIC <p></p>
# MAGIC
# MAGIC * Initialize the demo raw dataset
# MAGIC * Ingest synthea data by starting a Delta Live Tables run
# MAGIC * Secure the tables, lineage, audit logs
# MAGIC * Create our Patient cohorts
# MAGIC * Refresh the cohort dashboard
# MAGIC * Retrain our Patient readmission ML model:
# MAGIC   * Update our features
# MAGIC   * Start the autoML run and deploy it
# MAGIC   * Run inferences in a batch to score our entire dataset
# MAGIC
# MAGIC <a dbdemos-workflow-id="init-job" href="/#job/104444623965854">Click here to access your Workflow job</a>, it was setup when you installed your demo.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/lakehouse-hls-readmission-job-4.png?raw=true" />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating your workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/lakehouse-hls-readmission-job-3.png?raw=true" />
# MAGIC
# MAGIC A Databricks Workflow is composed of Tasks.
# MAGIC
# MAGIC Each task can trigger a specific job:
# MAGIC
# MAGIC * Delta Live Tables
# MAGIC * SQL query / dashboard
# MAGIC * Model retraining / inference
# MAGIC * Notebooks
# MAGIC * dbt
# MAGIC * ...
# MAGIC
# MAGIC In this example, can see our 3 tasks:
# MAGIC
# MAGIC * Start the DLT pipeline to ingest new data and refresh our tables
# MAGIC * Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC * Retrain our Churn model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Monitoring your runs
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/lakehouse-hls-readmission-job-0.png?raw=true" />
# MAGIC
# MAGIC Once your workflow is created, we can access historical runs and receive alerts if something goes wrong!
# MAGIC
# MAGIC In the screenshot, we can see that our workflow had multiple errors, with different runtimes, and ultimately got fixed.
# MAGIC
# MAGIC Workflow monitoring includes errors, abnormal job duration and more advanced control!
