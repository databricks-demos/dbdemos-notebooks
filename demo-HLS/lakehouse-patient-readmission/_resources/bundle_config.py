# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-hls-readmission",
  "category": "lakehouse",
  "title": "Lakehouse for HLS: Patient readmission",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_hls_readmission",
  "description": "Build your data platform and personalized health care to reduce readmission risk",
  "fullDescription": "The Databricks Lakehouse Platform is an open architecture that combines the best elements of data lakes and data warehouses. In this demo, we’ll show you how to build an end-to-end Health Car data platform to ingest patient and encounter informations. <br/>We will focus on predicting and explaining patient readmission risk to improve care quality. <br/><br/>This demo covers the end to end lakehouse platform: <ul><li>Ingest health care data (from Synthea), and then transform them to the OMOP data model using Delta Live Tables (DLT), a declarative ETL framework for building reliable, maintainable, and testable data processing pipelines. </li><li>Secure our ingested data to ensure governance and security on top of PII data</li><li>Build patient Cohorts and Leverage Databricks SQL and the warehouse endpoints to visualize our population.</li><li>Build a Machine Learning model with Databricks AutoML to predict 30 days patient readmission risk</li><li>Orchestrate all these steps with Databricks Workflow</li></ul>",
  "usecase": "Lakehouse Platform",
  "products": ["Delta Live Tables", "Databricks SQL", "MLFLow", "Auto ML", "Unity Catalog", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks for Financial Services", "url": "https://www.databricks.com/solutions/industries/financial-services"}],
  "recommended_items": ["lakehouse-iot-platform", "lakehouse-fsi-fraud", "lakehouse-retail-c360"],
  "demo_assets": [
      {"title": "Delta Live Table pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-fsi-credit-dlt-0.png"},
      {"title": "Databricks SQL Dashboard: Credit Decisioning", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-fsi-credit-dashboard-0.png"}],   "bundle": True,
  "tags": [{"dlt": "Delta Live Table"},  {"ds": "Data Science"}, {"uc": "Unity Catalog"}, {"dbsql": "BI/DW/DBSQL"}],
  "notebooks": [
    {
      "path": "_resources/00-generate-synthea-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Generate data", 
      "description": "Generate synthea dataset."
    },
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Prep data", 
      "description": "Helpers & setup."
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Load data", 
      "description": "Load data for demo."
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
      "path": "00-patient-readmission-introduction", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Patient Readmission Intro", 
      "description": "Introduction notebook, start here to implement your HLS Lakehouse."
    },
    {
      "path": "01-Data-Ingestion/01.1-DLT-patient-readmission-SQL", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Ingest patient & encounter data with Delta Live Table", 
      "description": "SQL DLT pipeline to ingest patient data & build clean tables."
    },
    {
      "path": "02-Data-Governance/02-Data-Governance-patient-readmission", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Governance with Unity Catalog", 
      "description": "Secure your tables, lineage, auditlog..."
    },
    {
      "path": "03-Data-Analysis-BI-Warehousing/03-Data-Analysis-BI-Warehousing-patient-readmission", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Feature engineering", 
      "description": "Feature engineering for credit decisioning"
    },
    {
      "path": "04-Data-Science-ML/04.1-Feature-Engineering-patient-readmission", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Feature Engineering", 
      "description": "Prepare our features and labls"
    },
    {
      "path": "04-Data-Science-ML/04.2-AutoML-patient-admission-risk", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "AutoML", 
      "description": "ML model training using AutoML"
    },
    {
      "path": "04-Data-Science-ML/04.3-Batch-Scoring-patient-readmission", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Batch Scoring", 
      "description": "Batch scoring using the best model generated by AutoML"
    },
    {
      "path": "04-Data-Science-ML/04.4-Model-Serving-patient-readmission", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Real-time Serving", 
      "description": "Create a real-time serving endpoint to enable live recommendation"
    },
    {
      "path": "04-Data-Science-ML/04.5-Explainability-patient-readmission", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Explainability and Fairness", 
      "description": "Explain model outputs using Shapley values and personalize care"
    },
    {
      "path": "04-Data-Science-ML/04.6-EXTRA-Feature-Store-ML-patient-readmission", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Explainability and Fairness", 
      "description": "Discover Databricks Feature Store benefits"
    },
    {
      "path": "05-Workflow-Orchestration/05-Workflow-Orchestration-patient-readmission", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Orchestrate the data pipeline with Databricks Workflows", 
      "description": "Orchestrate all tasks in a job and schedule your data/model refresh"
    }
  ],
  "init_job": {
    "settings": {
        "name": "dbdemos_lakehouse_hls_patient_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/01-load-data",
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "reset_all_data": False
                    }
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }, 
            {
                "task_key": "start_dlt_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_DLT_ID_dlt-patient-readmission}}",
                    "full_refresh": false
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [{"task_key": "init_data"}]
            },
            {
                "task_key": "build_cohorts",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/03-Data-Analysis-BI-Warehousing/03-Data-Analysis-BI-Warehousing-patient-readmission",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [{"task_key": "start_dlt_pipeline"}]
            },
            {
                "task_key": "feature_engineering",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.1-Feature-Engineering-patient-readmission",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [{"task_key": "build_cohorts"}]
            },
            {
                "task_key": "auto_ml",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.2-AutoML-patient-admission-risk",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [{"task_key": "feature_engineering"}]
            },
            {
                "task_key": "batch_scoring",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.3-Batch-Scoring-patient-readmission",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [{"task_key": "auto_ml"}]
            },
            {
                "task_key": "readmission_explainability",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/04-Data-Science-ML/04.5-Explainability-patient-readmission",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [{"task_key": "auto_ml"}]
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
                    "spark_version": "15.3.x-cpu-ml-scala2.12",
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
        "spark_version": "15.3.x-cpu-ml-scala2.12",
        "single_user_name": "{{CURRENT_USER}}",
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD"
  }, 
  "pipelines": [
    {
      "id": "dlt-patient-readmission",
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
        "photon": False,
        "libraries": [
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/_resources/01-load-data"
                },
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-Data-Ingestion/01.1-DLT-patient-readmission-SQL"
                }
            }
        ],
        "name": "dbdemos-hls-readmission_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "target": "{{SCHEMA}}"
      }
    }
  ],
  "dashboards": [{"name": "[demos] HLS - Patient Summary",  "id": "patient-summary"}]
}
