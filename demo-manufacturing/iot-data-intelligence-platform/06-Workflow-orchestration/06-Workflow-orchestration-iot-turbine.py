# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Deploying and orchestrating the full workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-10-03%20at%2014.16.23.png?raw=true" />
# MAGIC
# MAGIC All our assets are ready. We now need to define when we want our DLT pipeline to kick in and refresh the tables.
# MAGIC
# MAGIC One option is to switch DLT pipeline in continuous mode to have a streaming pipeline, providing near-realtime insight.
# MAGIC
# MAGIC An alternative is to wakeup the DLT pipeline every X hours, ingest the new data (incremental) and shut down all your compute. 
# MAGIC
# MAGIC This is a simple configuration offering a tradeoff between uptime and ingestion latencies.
# MAGIC
# MAGIC In our case, we decided that the best tradoff is to ingest new data every hours:
# MAGIC
# MAGIC - Start the DLT pipeline to ingest new data and refresh our tables
# MAGIC - Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC - Retrain our model to include the lastest date and capture potential behavior change
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05-Workflow-orchestration-iot-turbine&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Orchestrating our Churn pipeline with Databricks Workflows
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-workflow.png?raw=true" />
# MAGIC
# MAGIC With Databricks Data Intelligence Platform, no need for external orchestrator. We can use [Workflows](/#job/list) (available on the left menu) to orchestrate our Churn pipeline within a few click.
# MAGIC
# MAGIC
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
# MAGIC A workflow was created as part of this demo. Open the <a dbdemos-workflow-id="init-job" href="#job/629612721485383/tasks" target="_blank">IOT Wind Turbine Workflow</a> to start exploring Databricks orchestration capabilities!

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
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-workflow-monitoring.png" />
# MAGIC
# MAGIC Once your workflow is created, we can access historical runs and receive alerts if something goes wrong!
# MAGIC
# MAGIC In the screenshot we can see that our workflow had multiple errors, with different runtime, and ultimately got fixed.
# MAGIC
# MAGIC Workflow monitoring includes errors, abnormal job duration and more advanced control!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Not only Datatabricks Data Intelligence Platform let you ingest, analyze and infer churn, it also provides a best-in-class orchestrator to offer your business fresh insight making sure everything works as expected!
# MAGIC
# MAGIC [Go back to introduction]($../00-IOT-wind-turbine-introduction-DI-platform)
