# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-fsi-smart-claims",
  "category": "lakehouse",
  "title": "Data Intelligence Platform: Smart Claims Processing for Insurance",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_fsi_smart_claims",
  "description": "Build your smart claims platform on the Lakehouse",
  "fullDescription": "The Databricks Lakehouse Platform is an open architecture that combines the best elements of data lakes and data warehouses. In this demo, we'll show you how to build an end-to-end claims automation for car accident claims, delivering data and insights that would typically take months of effort on legacy platforms. <br/><br/>This demo covers the end to end lakehouse platform: <ul><li>Ingest both policy and claims data, and then transform them using Delta Live Tables (DLT), a declarative ETL framework for building reliable, maintainable, and testable data processing pipelines. </li><li>Ingest telematics data as an external streaming source to understand customer behavior</li><li>Build a Machine Learning model with Databricks and HuggingFace to identify the seviarity of the accident and car damage</li><li>Leverage Databricks DBSQL and the warehouse endpoints to build dashboard to analyze the policy holder and claims overview and also serve the the analytics outcome to claims investigators</li><li>Orchestrate all these steps with Databricks Workflow</li></ul>",
  "usecase": "Lakehouse Platform",
  "products": ["Delta Live Tables", "Databricks SQL", "MLFLow", "Auto ML", "Unity Catalog", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks for Financial Services", "url": "https://www.databricks.com/solutions/industries/financial-services"}],
  "recommended_items": ["lakehouse-fsi-credit", "lakehouse-fsi-fraud", "lakehouse-iot-platform"],
  "demo_assets": [],   
  "bundle": True,
  "tags": [{"dlt": "Delta Live Table"},  {"ds": "Data Science"}, {"uc": "Unity Catalog"}, {"dbsql": "BI/DW/DBSQL"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Prep data", 
      "description": "Helpers & setup."
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Config", 
      "description": "Config file."
    },
    {
      "path": "00-Smart-Claims-Introduction", 
      "pre_run": False,
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Lakehouse - Smart Claims", 
      "description": "Start here to explore the Lakehouse."
    },
    {
      "path": "01-Data-Ingestion/01.1-DLT-Ingest-Policy-Claims", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Ingest claim and policy data with Delta Live Table", 
      "description": "Python DLT pipeline to ingest claim and policy data."
    },
    {
      "path": "01-Data-Ingestion/01.2-Policy-Location", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Ingest Accidents data", 
      "description": "Ingest the accident images"
    },
    {
      "path": "02-Data-Science-ML/02.1-Model-Training", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Batch Scoring", 
      "description": "Train image processing model to assess accident severity"
    },
    {
      "path": "02-Data-Science-ML/02.2-Batch-Scoring", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Batch Scoring", 
      "description": "Batch scoring to generate damage severity assessment"
    },
    {
      "path": "02-Data-Science-ML/02.3-Dynamic-Rule-Engine", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Ingests Accident data", 
      "description": "Apply dynamic rule engine to generate insights"
    },
    {
      "path": "03-BI-Data-Warehousing/03-BI-Data-Warehousing-Smart-Claims", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Datawarehousing & BI / Dashboarding", 
      "description": "Generate dashboards to summarize claims and generate analytics for claims investigation"
    },
    {
      "path": "04-Generative-AI/04.1-AI-Functions-Creation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "GenAI Functions", 
      "description": "Utilize Databricks AI functions to generate automated claims summarizations."
    },
    {
      "path": "04-Generative-AI/04.2-Agent-Creation-Guide", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Create an agent", 
      "description": "Define an AI agent with the functions you defined in notebook 04.1"
    },     
    {
      "path": "05-Workflow-Orchestration/05-Workflow-Orchestration-Smart-Claims", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Orchestrate dashboard generation with Workflow", 
      "description": "Orchestrate all tasks in a job and schedule your data/model refresh"
    }
  ],
  "init_job": {
    "settings": {
        "name": "dbdemos_lakehouse_fsi_smart_claims_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
            },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": 
            [
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
                "task_key": "claim_policy_dlt_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_DLT_ID_dlt-fsi-smart-claims}}",
                    "full_refresh": True
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
                "task_key": "train_model",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/02-Data-Science-ML/02.1-Model-Training",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "init_data"
                      }
                  ]
            },
            {
                "task_key": "predict_accident_severity",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/02-Data-Science-ML/02.2-Batch-Scoring",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "claim_policy_dlt_pipeline"
                      },
                      {
                          "task_key": "train_model"
                      }
                  ]
            },
            {
                "task_key": "rule_engine",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/02-Data-Science-ML/02.3-Dynamic-Rule-Engine",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "predict_accident_severity"
                      }
                  ]
            },
            {
                "task_key": "create_ai_functions",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Generative-AI/04.1-AI-Functions-Creation",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "rule_engine"
                      }
                  ]
            }            
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 10
                    },
                    "spark_version": "16.4.x-cpu-ml-scala2.12",
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
        "autoscale": {
            "min_workers": 2,
            "max_workers": 10
        },
        "spark_version": "16.4.x-cpu-ml-scala2.12",
        "single_user_name": "{{CURRENT_USER}}",
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD"
  }, 
  "pipelines": [
    {
      "id": "dlt-fsi-smart-claims",
      "run_after_creation": False,
      "definition": {
        "clusters": [
            {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 5,
                    "mode": "ENHANCED"
                }
            }
        ],
        "development": True,
        "continuous": False,
        "channel": "PREVIEW",
        "edition": "ADVANCED",
        "photon": True,
        "libraries": [
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-Data-Ingestion/01.1-DLT-Ingest-Policy-Claims"
                }
            },
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-Data-Ingestion/01.2-Policy-Location"
                }
            }
        ],
        "name": "dbdemos_claims_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "target": "{{SCHEMA}}"
      }
    }
  ],
  "workflows": [],
  "dashboards": [{"name": "[dbdemos] FSI - Smart Claims Investigation",   "id": "claims-report"},
                 {"name": "[dbdemos] FSI - Smart Claims Summary Report",  "id": "claims-investigation"}]

}
