# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "dlt-loans",
  "category": "data-engineering",
  "title": "Full Delta Live Tables Pipeline - Loan",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_dlt_loan",
  "description": "Ingest loan data and implement a DLT pipeline with quarantine.",
  "fullDescription": "This demo is an introduction to Delta Live Tables, an ETL frameworks making Data Engineering accessible for all. Simply declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you:<ul><li><strong>Accelerate ETL development</strong>: Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance</li><li><strong>Remove operational complexity</strong>: By automating complex administrative tasks and gaining broader visibility into pipeline operations</li><li><strong>Trust your data</strong>With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML</li><li><strong>Simplify batch and streaming</strong>: With self-optimization and auto-scaling data pipelines for batch or streaming processing</li></ul>In this demo, we will be using as input a raw dataset containing information on our customers' loan and historical transactions. Our goal is to ingest this data in near real time and build tables for our Analyst team while ensuring data quality.",
  "usecase": "Data Engineering",
  "products": ["Delta Live Tables", "Delta Lake"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Delta Live Tables: 1B records for under $1", "url": "https://www.databricks.com/blog/2023/04/14/how-we-performed-etl-one-billion-records-under-1-delta-live-tables.html"}],
  "recommended_items": ["dlt-unit-test", "dlt-cdc", "delta-lake"],
  "demo_assets": [
      {"title": "Delta Live Tables pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/dlt-loans-dlt-0.png"},
      {"title": "Databricks SQL Dashboard: DLT Data Quality Stats", "url": "https://www.dbdemos.ai/assets/img/dbdemos/dlt-loans-dashboard-0.png"}
  ],
  "bundle": True,
  "tags": [{"dlt": "Delta Live Tables"}],
  "notebooks": [
    {
      "path": "_resources/00-Loan-Data-Generator", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Loan data generator", 
      "description": "Generate data for the pipeline."
    },
    {
      "path": "_resources/01-load-data-quality-dashboard", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Data quality expectation load", 
      "description": "Creates data from expectation for DBSQL dashboard."
    },
    {
      "path": "01-DLT-Loan-pipeline-SQL", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (SQL)", 
      "description": "Loan ingestion with DLT & quarantine"
    },
    {
      "path": "02-DLT-Loan-pipeline-PYTHON", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (Python)", 
      "description": "Loan ingestion with DLT & quarantine"
    },
    {
      "path": "03-Log-Analysis", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Pipeline expectation monitoring", 
      "description": "Extract data from expectation for DBSQL dashboard.",
      "parameters": {"storage_path": "/demos/dlt/loans/quentin_ambard"}
    }
  ],
  "init_job": {
    "settings": {
        "name": "demos_dlt_loans_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/00-Loan-Data-Generator",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "start_dlt_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_DLT_ID_dlt-loans}}",
                    "full_refresh": true
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
                "task_key": "load_data_quality_dashboard",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/01-load-data-quality-dashboard",
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
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "spark_version": "15.3.x-cpu-ml-scala2.12",
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
    "num_workers": 1,
    "spark_version": "14.3.x-scala2.12",
    "spark_conf": {},
    "data_security_mode": "USER_ISOLATION",
    "runtime_engine": "STANDARD"
  },
  "pipelines": [
    {
      "id": "dlt-loans",
      "run_after_creation": True,
      "definition": {
        "clusters": [
            {
                "label": "default",
                "num_workers": 1
            }
        ],
        "development": True,
        "continuous": False,
        "channel": "CURRENT",
        "edition": "ADVANCED",
        "photon": False,
        "libraries": [
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-DLT-Loan-pipeline-SQL"
                }
            }
        ],
        "name": "dbdemos_dlt_loan_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "target": "{{SCHEMA}}"
      }
    }
  ],
  "dashboards": [{"name": "[dbdemos] Delta Lake - Data Quality Stats",  "id": "dlt-expectations"}]
}
