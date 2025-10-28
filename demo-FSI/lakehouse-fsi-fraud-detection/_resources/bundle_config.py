# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-fsi-fraud",
  "category": "lakehouse",
  "title": "Retail Banking - Fraud Detection",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_fsi_fraud_detection",
  "description": "Build your Banking platform and detect Fraud in real-time. End 2 End demo, with Model Serving & realtime fraud inference A/B testing.",
  "fullDescription": "The Databricks Lakehouse Platform is an open architecture that combines the best elements of data lakes and data warehouses. In this demo, we'll show you how to build a Real-time Fraud detection system for banking transactionn, delivering data and insights that would typically take months of effort on legacy platforms. <br/><br/>This demo covers the end to end lakehouse platform: <ul><li>Ingest data from external systems (EPR/Salesforce...) and then transform it using Spark Declarative Pipelines (SDP), a declarative ETL framework for building reliable, maintainable, and testable data processing pipelines. </li><li>Secure your ingested data to ensure governance and security on top of PII data</li><li>Leverage Databricks DBSQL and the warehouse endpoints to build dashboards to analyze the ingested data and understand the existing Fraud</li><li>Build a Machine Learning model with Databricks AutoML to flag transactions at risk</li><li>Leverage Databricks Model Serving to deploy a REST API serving real-time inferences in milliseconds with model A/B testing.</li><li>Orchestrate all these steps with Databricks Workflow</li></ul>",
  "bundle": True,
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Prep data", 
      "description": "Helpers & setup."
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Demo setup", 
      "description": "Setup schema and database name."
    },
    {
      "path": "00-FSI-fraud-detection-introduction-lakehouse", 
      "pre_run": False,
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Lakehouse - Fraud introduction", 
      "description": "Start here to explore the Lakehouse."
    },
    {
      "path": "01-Data-ingestion/01.1-sdp-sql/01-SDP-fraud-detection-SQL",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Main notebook",
      "description": "SQL SDP pipeline to ingest data & build clean tables."
    },
    {
      "path": "01-Data-ingestion/01.1-sdp-sql/explorations/sample_exploration",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Sample exploration",
      "description": "Sample exploration notebook for pipeline."
    },
    {
      "path": "01-Data-ingestion/01.1-sdp-sql/transformations/01-bronze.sql",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Bronze transformations",
      "description": "Bronze layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.1-sdp-sql/transformations/02-silver.sql",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Silver transformations",
      "description": "Silver layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.1-sdp-sql/transformations/03-gold.sql",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Gold transformations",
      "description": "Gold layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.2-sdp-python/transformations/01-bronze.py",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP python - Bronze transformations",
      "description": "Bronze layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.2-sdp-python/transformations/02-silver.py",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP python - Silver transformations",
      "description": "Silver layer transformations."
    },
    {
      "path": "01-Data-ingestion/01.2-sdp-python/transformations/03-gold.py",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP python - Gold transformations",
      "description": "Gold layer transformations."
    },
    {
      "path": "02-Data-governance/02-UC-data-governance-ACL-fsi-fraud", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Governance with Unity Catalog", 
      "description": "Secure your tables, lineage, auditlog..."
    },
    {
      "path": "03-BI-data-warehousing/03-BI-Datawarehousing-fraud", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Datawarehousing & BI / Dashboarding", 
      "description": "Run interactive queries on top of your data"
    },
    {
      "path": "04-Data-Science-ML/04.1-AutoML-FSI-fraud", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Build Fraud prediction model (AutoML)", 
      "description": "Leverage Databricks AutoML to create a Fraud model in a few clicks"
    },
    {
      "path": "04-Data-Science-ML/04.2-automl-generated-notebook-fraud",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Explore Fraud Prediction generated model", 
      "description": "Explore the best Fraud model generated by AutoML and deploy it in production.",
      "parameters": {"shap_enabled": "true"}
    },
    {
      "path": "04-Data-Science-ML/04.3-Model-serving-realtime-inference-fraud", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Infer Fraud in realtime - serverless API", 
      "description": "Once your model is deployed, run low latency inferences."
    },
    {
      "path": "04-Data-Science-ML/04.4-Upgrade-to-imbalance-and-xgboost-model-fraud", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Upgrade our model to XGboost", 
      "description": "Improve AutoML model to handle imbalanced data."
    },
    {
      "path": "04-Data-Science-ML/04.5-AB-testing-model-serving-fraud", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Roll-out our new model with A/B testing.", 
      "description": "Deploy the new model comparing its performance with the previous one."
    },
    {
      "path": "05-Generative-AI/05.1-AI-Functions-Creation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "GenAI Functions", 
      "description": "Utilize Databricks AI functions to generate automated fraud report generation."
    },
    {
      "path": "05-Generative-AI/05.2-Agent-Creation-Guide", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Create an agent", 
      "description": "Define an AI agent with the functions you defined in notebook 04.1"
    },     
    {
      "path": "06-Workflow-orchestration/06-Workflow-orchestration-fsi-fraud", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Orchestrate churn prevention with Workflow", 
      "description": "Orchestrate all tasks in a job and schedule your data/model refresh"
    }
  ],
  "init_job": {
    "settings": {
        "name": "dbdemos_lakehouse_fsi_fraud_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/00-setup",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "start_sdp_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_SDP_ID_sdp-fsi-fraud}}",
                    "full_refresh": false
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "init_data"
                    }
                ]
            },
            {
                "task_key": "create_feature_and_automl_run",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.1-AutoML-FSI-fraud",
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
                "task_key": "register_model",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.2-automl-generated-notebook-fraud",
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
                "task_key": "create_model_serving_endpoint",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.3-Model-serving-realtime-inference-fraud",
                    "source": "WORKSPACE"
                },
                "base_parameters": {"shap_enabled": "false"},
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "register_model"
                      }
                  ]
            },
            {
                "task_key": "create_ai_functions",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/05-Generative-AI/05.1-AI-Functions-Creation",
                    "source": "WORKSPACE"
                },
                "base_parameters": {"shap_enabled": "false"},
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "create_model_serving_endpoint"
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
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "spark_version": "16.4.x-cpu-ml-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER",
    "num_workers": 0
  }, 
  "pipelines": [
    {
      "id": "sdp-fsi-fraud",
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
                "glob": {
                    "include": "{{DEMO_FOLDER}}/01-Data-ingestion/01.1-sdp-sql/transformations/**"
                }
            }
        ],
        "name": "dbdemos_sdp_lakehouse_fraud_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "schema": "{{SCHEMA}}",
        "event_log": {
              "catalog": "{{CATALOG}}",
              "schema": "{{SCHEMA}}",
              "name": "dbdemos_fraud_event_logs"
        },
        "root_path": "{{DEMO_FOLDER}}/01-Data-ingestion/01.1-sdp-sql"
      }
    }
  ],
  "dashboards": [{"name": "[dbdemos] FSI Fraud analysis Analysis",       "id": "fraud-detection"}]
}
