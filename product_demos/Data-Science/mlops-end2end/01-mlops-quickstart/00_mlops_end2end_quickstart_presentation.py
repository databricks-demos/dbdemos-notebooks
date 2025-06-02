# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End MLOps demo with MLFlow, Auto ML, and Models in Unity Catalog
# MAGIC
# MAGIC ## Challenges moving ML project into production
# MAGIC
# MAGIC Moving an ML project from a standalone notebook to a production-grade data pipeline is complex and requires multiple competencies.
# MAGIC
# MAGIC Having a model up and running in a notebook isn't enough. We need to cover the end to end ML Project life cycle and solve the following challenges:
# MAGIC
# MAGIC * Update data over time (production-grade ingestion pipeline)
# MAGIC * How to save, share, and re-use ML features in the organization
# MAGIC * How to ensure a new model version that respects quality standards and won't break the pipeline
# MAGIC * Model governance: what is deployed, how is it trained, by whom, and which data?
# MAGIC * How to monitor and re-train the model...
# MAGIC
# MAGIC In addition, these projects typically involve multiple teams, creating friction and potential silos
# MAGIC
# MAGIC * Data Engineers in charge of ingesting, preparing, and exposing the data
# MAGIC * Data Scientist, expert in data analysis, building ML model
# MAGIC * ML engineers, setup the ML infrastructure pipelines (similar to DevOps)
# MAGIC
# MAGIC This has a real impact on the business, slowing down projects and preventing them from being deployed in production and bringing ROI.
# MAGIC
# MAGIC ## What's MLOps?
# MAGIC
# MAGIC MLOps is a set of standards, tools, processes, and methodology that aims to optimize time, efficiency, and quality while ensuring governance in ML projects.
# MAGIC
# MAGIC MLOps orchestrate a project life-cycle between the project and the teams to implement such ML pipelines smoothly.
# MAGIC
# MAGIC Databricks is uniquely positioned to solve this challenge with the Lakehouse pattern. Not only do we bring Data Engineers, Data Scientists, and ML Engineers together in a unique platform, but we also provide tools to orchestrate ML projects and accelerate the go to production.
# MAGIC
# MAGIC ## MLOps process walkthrough
# MAGIC
# MAGIC In this quickstart demo, we'll go through a few common steps in the MLOps process. The result of this process is a model used to power a dashboard for downstream business stakeholders, which is:
# MAGIC * preparing features
# MAGIC * training a model for deployment
# MAGIC * registering the model for its use to be governed
# MAGIC * validating the model in a champion-challenger analysis
# MAGIC * invoking a trained ML model as a pySpark UDF
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-0.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or disable the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00_mlops_end2end_quickstart_presentation&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC In this first quickstart, we'll cover the foundation of MLOps.
# MAGIC
# MAGIC The advanced section will go into more detail, including:
# MAGIC - Model Serving
# MAGIC - Realtime Feature serving with Online Tables
# MAGIC - A/B testing 
# MAGIC - Automated re-training
# MAGIC - Infra setup and hooks with Databricks MLOps Stack
# MAGIC - ...

# COMMAND ----------

# MAGIC %pip install mlflow==2.22.0

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer churn detection
# MAGIC
# MAGIC To explore MLOps, we'll implement a customer churn model.
# MAGIC
# MAGIC Our marketing team asked us to create a Dashboard tracking Churn risk evolution. In addition, we need to provide our renewal team with a daily list of customers at Churn risk to increase our final revenue.
# MAGIC
# MAGIC Our Data Engineer team provided us with a dataset collecting information on our customer base, including churn information. That's where our implementation starts.
# MAGIC
# MAGIC Let's see how we can implement such a model and provide our marketing and renewal team with Dashboards to track and analyze our Churn prediction.
# MAGIC
# MAGIC Ultimately, you'll be able to build a complete DBSQL Churn Dashboard containing all our customer & churn information but also start a Genie space to ask any question using plain English!

# COMMAND ----------

# DBTITLE 1,Exploring our customer dataset
telcoDF = spark.table("mlops_churn_bronze_customers")
display(telcoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC Our first job is to analyze the data and prepare a set of features.
# MAGIC
# MAGIC
# MAGIC Next: [Analyze the data and prepare features]($./01_feature_engineering)
