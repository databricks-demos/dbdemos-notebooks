# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo
# MAGIC
# MAGIC Use the churn dashboard, so loads the DBSQL dashboard data from this demo.

# COMMAND ----------

{
  "name": "mlops-end2end",
  "category": "data-science",
  "title": "MLOps - End 2 end pipeline",
  "description": "Automate your model deployment with MLFlow and UC, end 2 end!",
  "fullDescription": "This demo covers a full MLOPs pipeline. We'll show you how Databricks Lakehouse can be leverage to orchestrate and deploy model in production while ensuring governance, security and robustness.<ul></li>Ingest data and save them as feature store</li><li>Build ML model with Databricks AutoML</li><li>Setup MLFlow hook to automatically test our models</li><li>Create the model test job</li><li>Automatically move model in production once the test are validated</li><li>Periodically retrain our model to prevent from drift</li></ul><br/><br/>Note that this is a fairly advanced demo. If you're new to Databricks and just want to learn about ML, we recommend starting with a ML demo or one of the Lakehouse demos.",
    "usecase": "Data Science & AI",
  "products": ["LLM", "Dolly", "AI"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Free Dolly", "url": "https://www.databricks.com/blog/2023/04/12/dolly-first-open-commercially-viable-instruction-tuned-llm"}],
  "recommended_items": ["sql-ai-functions", "feature-store", "llm-dolly-chatbot"],
  "demo_assets": [
      {"title": "Databricks SQL Dashboard: Customer Churn prediction", "url": "https://www.dbdemos.ai/assets/img/dbdemos/mlops-end2end-dashboard-0.png"}],
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
      "path": "01-mlops-quickstart/00_mlops_end2end_quickstart_presentation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "MLOps end2end presentation", 
      "description": "Understand MLOps and the flow we'll implement for Customer Churn detection."
    },
    {
      "path": "01-mlops-quickstart/01_feature_engineering", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature engineering & Feature store for Auto-ML", 
      "description": "Create and save your features to Feature store.",
      "parameters": {"force_refresh_automl": "true"}
    },
    {
      "path": "01-mlops-quickstart/02_automl_best_run", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Train ML model using AutoML best run", 
      "description": "Leverage Auto-ML generated notebook to build the best model out of the bpox."
    },
    {
      "path": "01-mlops-quickstart/03_from_notebook_to_models_in_uc", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Register your best run with UC", 
      "description": "Leverage MLFlow to find your best training run and save as Challenger"
    },
    {
      "path": "01-mlops-quickstart/04_challenger_validation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Validate your Challenger model", 
      "description": "Test your challenger model and move it as Champion."
    },
    {
      "path": "01-mlops-quickstart/05_batch_inference", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Run inference", 
      "description": "Leverage your ML model within inference pipelines."
    },
    {
      "path": "02-mlops-advanced/00_mlops_end2end_advanced", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Advanced MLOps pipeline", 
      "description": "Complete MLOps flow - intro."
    }
  ],
  "cluster": {
      "spark_version": "15.3.x-cpu-ml-scala2.12",
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER",
    "num_workers": 0
  }
}
