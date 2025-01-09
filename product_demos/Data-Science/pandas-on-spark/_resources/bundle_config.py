# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "pandas-on-spark",
  "category": "data-science",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_schema": "dbdemos_pandas_on_spark",
  "default_catalog": "main",
  "title": "Pandas API with spark backend (Koalas)",
  "description": "Let you Data Science team scale to TB of data while working with Pandas API, without having to learn & move to another framework.",
  "fullDescription": "Despite being one of the most popular framework for data analysis, Pandas isn't distributed and can't process TB of data. Databricks solves this issues by allowing users to leverage pandas API while processing the data with spark distributed engine. This demo show you how to process big data using Pandas API (previously known as Koalas).",
  "usecase": "Data Science & AI",
  "products": ["Pandas", "Koalas", "Pandas on Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Pandas On Spark benchmark", "url": "https://www.databricks.com/blog/2021/04/07/benchmark-koalas-pyspark-and-dask.html"}],
  "recommended_items": ["llm-dolly-chatbot", "feature-store", "mlops-end2end"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"ds": "Data Science"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "_resources/01-load-data",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "01-pyspark-pandas-api-koalas", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Pandas on Spark (Koalas)", 
      "description": "Scale & accelerate your Pandas transformations"
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]"
    },
    "spark_version": "14.3.x-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }  
}
