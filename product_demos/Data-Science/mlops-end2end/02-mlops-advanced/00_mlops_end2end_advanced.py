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
# MAGIC * ML Engineers, setup the ML infrastructure pipelines (similar to DevOps)
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
# MAGIC In the first [quickstart notebooks]($../01-mlops-quickstart/00_mlops_end2end_quickstart_presentation), we have covered a few common steps in the MLOps process. Now that you have mastered the foundation of MLOps, we'll dive into a more complete, end-to-end workflow. The end result of this process is a model used to power a dashboard for downstream business stakeholders, as well as a REST API endpoint that can make predictions in real-time.
# MAGIC
# MAGIC This end-to-end MLOps workflow involes:
# MAGIC
# MAGIC * preparing features, functions computing and persisting to an offline feature store
# MAGIC * training a model for deployment
# MAGIC * registering the model for its use to be goverened
# MAGIC * validating the model in a champion-challenger analysis
# MAGIC * invoking a trained ML model as part of a batch inference job
# MAGIC * deploying features for real-time feature lookup
# MAGIC * deploying the model to a real-time serving endpoint
# MAGIC * monitoring data and model drift
# MAGIC * detecting drift and retraining the model
# MAGIC
# MAGIC Run this demo on a __DBR 15.4 ML LTS__ cluster. A demo cluster has been created for you.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-0.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00_mlops_end2end_quickstart_presentation&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# MAGIC  %pip install mlflow==2.22.0

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false $adv_mlops=true 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer churn detection
# MAGIC
# MAGIC Just as we did in the Quickstart example, we'll be implementing a customer churn model to illustrate advanced concepts in MLOps.
# MAGIC
# MAGIC Our marketing team asked us to create a Dashboard tracking Churn risk evolution. In addition, we need to provide our renewal team with a daily list of customers at Churn risk to increase our final revenue. We also need to use the model in an online application to identify likely churners and take action to retain them.
# MAGIC
# MAGIC Our Data Engineer team provided us a dataset collecting information on our customer base, including churn information. That's where our implementation starts.
# MAGIC
# MAGIC Let's see how we can implement such a model, but also provide our marketing and renewal team with Dashboards to track and analyze our Churn prediction.
# MAGIC
# MAGIC Ultimately, you'll build able to build a complete DBSQL Churn Dashboard containing all our customer & churn information, but also start a Genie space to ask any question using plain english!

# COMMAND ----------

telcoDF = spark.table("advanced_churn_bronze_customers")
display(telcoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering
# MAGIC Our first job is to analyze the data, and prepare a set of features.
# MAGIC
# MAGIC
# MAGIC Next: [Analyze the data and prepare features]($./01_feature_engineering)
