# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Orchestrating the End-to-End Workflow: From Data to Action
# MAGIC
# MAGIC
# MAGIC ## Finding Your Rhythm: Real-time vs. Scheduled
# MAGIC
# MAGIC Every business has its own pulse. We need to match our technical architecture to this natural cadence:
# MAGIC
# MAGIC **Option 1: Always-On** ⚡  
# MAGIC Configure DLT pipelines in continuous mode for streaming insights that capture customer signals as they happen.
# MAGIC
# MAGIC **Option 2: Efficient Cycles** ⏱️  
# MAGIC Wake your compute on a scheduled basis, process new data incrementally, and then release resources.
# MAGIC
# MAGIC ## Our Blueprint: The Hourly Retention Cycle
# MAGIC
# MAGIC For our retail business, an hourly cadence hits the sweet spot:
# MAGIC
# MAGIC 1. **Data Refresh** 📊 - DLT pipeline activates to ingest the latest customer data
# MAGIC 2. **Dashboard Update** 📈 - DBSQL dashboards automatically refresh
# MAGIC 3. **Model Retraining** 🧠 - ML models adapt to evolving customer behaviors
# MAGIC 4. **Triggered Interventions** 🎯 - GenAI deploys personalized retention campaigns
# MAGIC
# MAGIC This creates a closed-loop system where insights drive actions, actions generate new data, and new data refines future insights—all automatically.
# MAGIC
# MAGIC <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 20px;">
# MAGIC <strong>💡 Pro Tip:</strong> During peak seasons or special promotions, consider shifting to continuous mode for real-time responsiveness, then return to scheduled intervals during normal operations.
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05-Workflow-orchestration-churn&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Orchestrating our Churn pipeline with Databricks Workflows
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cross_demo_assets/dbdemos-workflow-churn-1.png?raw=true" />
# MAGIC
# MAGIC With Databricks Lakehouse, no need for external orchestrator. We can use [Workflows](/#job/list) (available on the left menu) to orchestrate our Churn pipeline within a few click.
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

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating your workflow
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cross_demo_assets/dbdemos-workflow-churn-3.png?raw=true" />
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

# MAGIC %md
# MAGIC A workflow was created as part of this demo. Open the <a dbdemos-workflow-id="init-job" href="#job/629612721485383/tasks" target="_blank">C360 Churn Workflow</a> to start exploring Databricks orchestration capabilities!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Monitoring your runs
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cross_demo_assets/dbdemos-workflow-tasks.png?raw=true" />
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
# MAGIC Not only Datatabricks Lakehouse let you ingest, analyze and infer churn, it also provides a best-in-class orchestrator to offer your business fresh insight making sure everything works as expected!
# MAGIC
# MAGIC [Go back to introduction]($../00-churn-introduction-lakehouse)
