# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-retail-c360",
  "category": "lakehouse",
  "title": "Lakehouse for C360: Reducing Customer Churn",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_retail_c360",
  "env_version": 3,
  "description": "Centralize customer data and reduce churn: Ingestion (SDP), BI, Predictive Maintenance (ML), Governance (UC), Orchestration.",
  "fullDescription": "The Databricks Lakehouse Platform is an open architecture that combines the best elements of data lakes and data warehouses. In this demo, we'll show you how to build a customer 360 solution on the lakehouse, delivering data and insights that would typically take months of effort on legacy platforms. <br/><br/>This demo covers the end-to-end lakehouse platform: <ul><li>Ingest data from external system (such as EPR/Salesforce) and then transform it using Spark Declarative Pipelines (SDP), a declarative ETL framework for building reliable, maintainable and testable data processing pipelines </li><li>Secure your ingested data to ensure governance and security on top of PII data</li><li>Leverage Databricks SQL and the warehouse endpoints to build a dashboard to analyze the ingested data and understand the existing churn</li><li>Build a machine learning model with Databricks AutoML to understand and predict future churn</li><li>Orchestrate all these steps with Databricks Workflows</li></ul>",
  "usecase": "Lakehouse Platform",
  "products": ["Spark Declarative Pipelines", "Databricks SQL", "MLFLow", "Auto ML", "Unity Catalog", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Solution Accelerator: Subscriber Churn Prediction", "url": "https://www.databricks.com/solutions/accelerators/survivorship-and-churn"}],
  "recommended_items": ["lakehouse-fsi-credit", "lakehouse-fsi-fraud", "lakehouse-iot-platform"],
  "demo_assets": [
      {"title": "Spark Declarative Pipelines pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dlt-0.png"},
      {"title": "Databricks SQL Dashboard: Churn Analysis", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dashboard-1.png"},
      {"title": "Databricks SQL Dashboard: SDP Data Quality Stats", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dashboard-0.png"},
      {"title": "Databricks SQL Dashboard: Customer Churn Prediction", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dashboard-2.png"}], 
  "bundle": True,
  "tags": [{"dlt": "Spark Declarative Pipelines"},  {"ds": "Data Science"}, {"uc": "Unity Catalog"}, {"dbsql": "BI/DW/DBSQL"}],
  "notebooks": [
    {
      "path": "_resources/00-prep-data-db-sql", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Dbsql data", 
      "description": "Prep data for dbsql dashboard."
    },
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Dbsql data", 
      "description": "Prep data for dbsql dashboard."
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Dbsql data", 
      "description": "Prep data for dbsql dashboard."
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Configuration file", 
      "description": "Define the database and volume folder."
    },
    {
      "path": "00-churn-introduction-lakehouse", 
      "pre_run": False,
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Lakehouse - churn introduction", 
      "description": "Start here to explore the Lakehouse."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/01.1-SDP-churn-SQL",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Main notebook",
      "description": "SQL SDP pipeline to ingest data & build clean tables."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/explorations/sample_exploration",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Sample exploration",
      "description": "Sample exploration notebook for SQL pipeline."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/transformations/01-bronze.sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Bronze transformations",
      "description": "Bronze layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/transformations/02-silver.sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Silver transformations",
      "description": "Silver layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/transformations/03-gold.sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Gold transformations",
      "description": "Gold layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/transformations/04-churn-UDF.py",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - UDF",
      "description": "Churn prediction UDF for ML model."
    },
    {
      "path": "01-Data-ingestion/01.2-SDP-python/01.1-SDP-churn-Python",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP Python - Main notebook",
      "description": "Python SDP pipeline to ingest data & build clean tables."
    },
    {
      "path": "01-Data-ingestion/01.2-SDP-python/explorations/sample_exploration",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP Python - Sample exploration",
      "description": "Sample exploration notebook for Python pipeline."
    },
    {
      "path": "01-Data-ingestion/01.2-SDP-python/transformations/01-bronze.py",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP Python - Bronze transformations",
      "description": "Bronze layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.2-SDP-python/transformations/02-silver.py",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP Python - Silver transformations",
      "description": "Silver layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.2-SDP-python/transformations/03-gold.py",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP Python - Gold transformations",
      "description": "Gold layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.1-SDP-SQL/01.2-SDP-churn-expectation-dashboard-data-prep",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Dashboard data prep",
      "description": "Prepare data for SDP quality dashboard (SQL version)."
    },
    {
      "path": "01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-churn", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Alternative: Ingest data with Spark+Delta", 
      "description": "Build a complete ingestion pipeline using spark API (alternative to SDP)"
    },
    {
      "path": "02-Data-governance/02.1-UC-data-governance-security-churn",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Governance with Unity Catalog", 
      "description": "Secure your tables, lineage, auditlog..."
    },
    {
      "path": "02-Data-governance/02.2-UC-metric-views",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title":  "Semantic Layer with Metric Views",
      "description": "Metric views in Unity Catalog simplify access to key business KPIs."
    },
    {
      "path": "02-Data-governance/churn_users_mteric_view.yaml",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Metric view YAML for churn_users table",
      "description": "Metric view definition as a YAML for main.dbdemos_retail_c360.churn_users."
    },
    {
      "path": "04-Data-Science-ML/04.1-automl-churn-prediction", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Build churn prediction model (AutoML)", 
      "description": "Leverage Databricks AutoML to create a churn model in a few clicks"
    },
    {
      "path": "04-Data-Science-ML/04.2-automl-generated-notebook",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Explore churn Prediction generated model", 
      "description": "Explore the best churn model generated by AutoML and deploy it in production.",
      "parameters": {"shap_enabled": "true"}
    },
    {
      "path": "04-Data-Science-ML/04.3-running-inference", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Infer churn on batch or realtime serverless", 
      "description": "Once your model is deployed, run low latency inferences."
    },
    {
      "path": "03-AI-BI-data-warehousing/03.1-AI-BI-Datawarehousing", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Datawarehousing & BI / Dashboarding", 
      "description": "Run interactive queries on top of your data"
    },
    {
      "path": "03-AI-BI-data-warehousing/03.2-AI-BI-Genie", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Datawarehousing & BI / Dashboarding", 
      "description": "Run interactive queries on top of your data"
    },
    {
      "path": "05-Generative-AI/05.1-Agent-Functions-Creation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Define GenAI Functions for reducing customer churn", 
      "description": "Define the Unity Catalog functions to reduce churn, including a churn predictor, order retriever, and marketing copy generator."
    },    
    {
      "path": "05-Generative-AI/05.2-Agent-Creation-Guide", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Create an agent", 
      "description": "Define an AI agent with the functions you defined in notebook 05.1"
    },    
    {
      "path": "06-Workflow-orchestration/06-Workflow-orchestration-churn", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Orchestrate churn prevention with Workflow", 
      "description": "Orchestrate all tasks in a job and schedule your data/model refresh"
    }
  ],
  "init_job": {
    "settings": {
        "name": "dbdemos_lakehouse_retail_c360_init_{{CATALOG}}_{{SCHEMA}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "load_dataset",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/01-load-data",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "start_sdp_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_SDP_ID_sdp-churn}}",
                    "full_refresh": false
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "load_dataset"
                    }
                ]
            },
            {
                "task_key": "init_dashboard_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/00-prep-data-db-sql",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "start_sdp_pipeline"
                    }
                ]
            },
            {
                "task_key": "create_feature_and_automl_run",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.1-automl-churn-prediction",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "init_dashboard_data"
                      }
                  ]
            },
            {
                "task_key": "register_churn_model",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.2-automl-generated-notebook",
                    "source": "WORKSPACE"
                },
                "base_parameters": {"shap_enabled": "false"},
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "create_feature_and_automl_run"
                      }
                  ]
            },
            {
                "task_key": "running_inference",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.3-running-inference",
                    "source": "WORKSPACE"
                },
                "base_parameters": {"shap_enabled": "false"},
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "register_churn_model"
                      }
                  ]
            },            
            {
                "task_key": "create_ai_functions",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/05-Generative-AI/05.1-Agent-Functions-Creation",
                    "source": "WORKSPACE"
                },
                "base_parameters": {"shap_enabled": "false"},
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "running_inference"
                      }
                  ]
            }            
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "spark_version": "16.4.x-cpu-ml-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
    }
  },
  "cluster": {
      "spark_version": "16.4.x-cpu-ml-scala2.12",
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
  }, 
  "pipelines": [
    {
      "id": "sdp-churn",
      "run_after_creation": False,
      "definition": {
        "clusters": [
            {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 2,
                    "mode": "LEGACY"
                }
            }
        ],
        "name": "dbdemos_sdp_lakehouse_churn_{{CATALOG}}_{{SCHEMA}}",
        "libraries": [
          {
            "glob": {
              "include": "{{DEMO_FOLDER}}/01-Data-ingestion/01.1-SDP-SQL/transformations/**"
            }
          }
        ],
        "schema": "{{SCHEMA}}",
        "continuous": False,
        "development": True,
        "edition": "ADVANCED",
        "photon": False,
        "channel": "PREVIEW",
        "catalog": "{{CATALOG}}",
        "root_path": "{{DEMO_FOLDER}}/01-Data-ingestion/01.1-SDP-SQL",
        "event_log": {
              "catalog": "{{CATALOG}}",
              "schema": "{{SCHEMA}}",
              "name": "dbdemos_retail_c360_event_logs"
        },
        "environment": {
          "dependencies": ["mlflow==3.1.0"]
        }
      }
    }
  ],
  "dashboards": [{"name": "[dbdemos] Retail Churn Prediction Dashboard",       "id": "churn-prediction"},
                 {"name": "[dbdemos] Retail - Customer Churn - Universal",     "id": "churn-universal"},
                 {"name": "[dbdemos] Retail SDP - Retail Data Quality Stats",  "id": "sdp-quality-stat"}]
}
