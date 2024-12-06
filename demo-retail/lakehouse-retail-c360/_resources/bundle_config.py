# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-retail-c360",
  "category": "lakehouse",
  "title": "Lakehouse for C360: Reducing Customer Churn",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_retail_c360",
  "description": "Centralize customer data and reduce churn: Ingestion (DLT), BI, Predictive Maintenance (ML), Governance (UC), Orchestration.",
  "fullDescription": "The Databricks Lakehouse Platform is an open architecture that combines the best elements of data lakes and data warehouses. In this demo, we'll show you how to build a customer 360 solution on the lakehouse, delivering data and insights that would typically take months of effort on legacy platforms. <br/><br/>This demo covers the end-to-end lakehouse platform: <ul><li>Ingest data from external system (such as EPR/Salesforce) and then transform it using Delta Live Tables (DLT), a declarative ETL framework for building reliable, maintainable and testable data processing pipelines </li><li>Secure your ingested data to ensure governance and security on top of PII data</li><li>Leverage Databricks SQL and the warehouse endpoints to build a dashboard to analyze the ingested data and understand the existing churn</li><li>Build a machine learning model with Databricks AutoML to understand and predict future churn</li><li>Orchestrate all these steps with Databricks Workflows</li></ul>",
  "usecase": "Lakehouse Platform",
  "products": ["Delta Live Tables", "Databricks SQL", "MLFLow", "Auto ML", "Unity Catalog", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Solution Accelerator: Subscriber Churn Prediction", "url": "https://www.databricks.com/solutions/accelerators/survivorship-and-churn"}],
  "recommended_items": ["lakehouse-fsi-credit", "lakehouse-fsi-fraud", "lakehouse-iot-platform"],
  "demo_assets": [
      {"title": "Delta Live Table pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dlt-0.png"},
      {"title": "Databricks SQL Dashboard: Churn Analysis", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dashboard-1.png"},
      {"title": "Databricks SQL Dashboard: DLT Data Quality Stats", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dashboard-0.png"},
      {"title": "Databricks SQL Dashboard: Customer Churn Prediction", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-retail-c360-dashboard-2.png"}], 
  "bundle": True,
  "tags": [{"dlt": "Delta Live Table"},  {"ds": "Data Science"}, {"uc": "Unity Catalog"}, {"dbsql": "BI/DW/DBSQL"}],
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
      "path": "01-Data-ingestion/01.1-DLT-churn-SQL", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Ingest data with Delta Live Table", 
      "description": "SQL DLT pipeline to ingest data & build clean tables."
    },
    {
      "path": "01-Data-ingestion/01.2-DLT-churn-Python-UDF", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Ingest data with DLT-companion UDF", 
      "description": "Loads ML model as UDF in python."
    },
    {
      "path": "01-Data-ingestion/01.3-DLT-churn-python", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Alternative: Ingest data with Delta Live Table", 
      "description": "Python DLT pipeline to ingest data & build clean tables."
    },
    {
      "path": "01-Data-ingestion/01.4-DLT-churn-expectation-dashboard-data-prep", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Alternative: Ingest data with Delta Live Table", 
      "description": "Python DLT pipeline to ingest data & build clean tables."
    },
    {
      "path": "01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-churn", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Alternative: Ingest data with Spark+Delta", 
      "description": "Build a complete ingestion pipeline using spark API (alternative to DLT)"
    },
    {
      "path": "02-Data-governance/02-UC-data-governance-security-churn", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Governance with Unity Catalog", 
      "description": "Secure your tables, lineage, auditlog..."
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
      "path": "04-Data-Science-ML/04.4-GenAI-for-churn", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "GenAI on Databricks", 
      "description": "Reduce churn with GenAI capabilities."
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
      "path": "05-Workflow-orchestration/05-Workflow-orchestration-churn", 
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
                "task_key": "start_dlt_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_DLT_ID_dlt-churn}}",
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
                        "task_key": "start_dlt_pipeline"
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
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "spark_version": "15.4.x-cpu-ml-scala2.12",
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
      "spark_version": "15.4.x-cpu-ml-scala2.12",
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
      "id": "dlt-churn",
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
        "development": True,
        "continuous": False,
        "channel": "PREVIEW",
        "edition": "ADVANCED",
        "photon": False,
        "libraries": [
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-Data-ingestion/01.1-DLT-churn-SQL"
                }
            },
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-Data-ingestion/01.2-DLT-churn-Python-UDF"
                }
            }
        ],
        "name": "dbdemos_dlt_lakehouse_churn_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "target": "{{SCHEMA}}"
      }
    }
  ],
  "dashboards": [{"name": "[dbdemos] Retail Churn Prediction Dashboard",       "id": "churn-prediction"},
                 {"name": "[dbdemos] Retail - Customer Churn - Universal",     "id": "churn-universal"},
                 {"name": "[dbdemos] Retail DLT - Retail Data Quality Stats",  "id": "dlt-quality-stat"}]
}
