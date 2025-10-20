# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Deploying and orchestrating the full workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="800px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dashboard-1.png?raw=trueg" />
# MAGIC
# MAGIC
# MAGIC All our assets are ready. We now need to define when we want our overall workflow/pipeline to kick in and refresh the tables.
# MAGIC
# MAGIC Our Data Pipeline can be run in 2 mode:
# MAGIC - continuous mode to have a streaming pipeline, providing near-realtime insight (ingesting new claims in real-time).
# MAGIC - In a batch mode, waking the pipeline every X hours, ingest the new data (incremental) and shut down all your compute. This is a simple configuration offering a tradeoff between uptime and ingestion latencies.
# MAGIC
# MAGIC In our case, we decided that the best tradoff is to ingest new data every hours.
# MAGIC
# MAGIC To do so, we'll need an orchestrator to schedule the data ingestion (trigger the SDP pipeline) every hour, and then call our model to infere the image severity. 
# MAGIC
# MAGIC Ultimately, we could even refresh our DBSQL Dashboard!
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=04-Workflow-Orchestration-Smart-Claims&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Orchestrating our Smart Claims pipeline with Databricks Workflows
# MAGIC
# MAGIC With Databricks Lakehouse, no need for external orchestrator. We can use [Workflows](/#job/list) (available on the left menu) to orchestrate our Smart Claims within a few clicks.
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
# MAGIC ## A workflow was created for you as part of the demo!
# MAGIC
# MAGIC Open the <a dbdemos-workflow-id="init-job" href="#job/629612721485383/tasks" target="_blank">FSI Smart Claims workflow</a> to start exploring Databricks orchestration capabilities!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating your workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dashboard-full.png?raw=trueg" />
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
# MAGIC In this example, can see our several tasks, one of which is SDP flow:
# MAGIC
# MAGIC * Start the workflow to ingest new data and refresh our tables
# MAGIC * Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC * Retrain our model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Monitoring your runs
# MAGIC
# MAGIC <img style="float: left; margin-right: 20px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-workflow-monitoring.png" />
# MAGIC
# MAGIC Once your workflow is created, we can access historical runs and receive alerts if something goes wrong!
# MAGIC
# MAGIC In the screenshot we can see that our workflow had multiple errors, with different runtime, and ultimately got fixed.
# MAGIC
# MAGIC Workflow monitoring includes errors, abnormal job duration and more advanced control!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC You're now ready to orchestrate your data pipeline using Databricks Workflow!
# MAGIC
# MAGIC Return to [00-Smart-Claims-Introduction]($../00-Smart-Claims-Introduction)
