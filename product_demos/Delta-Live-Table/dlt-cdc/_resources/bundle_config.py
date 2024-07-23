# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "dlt-cdc",
  "category": "data-engineering",
  "title": "CDC pipeline with Delta Live Table.",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_dlt_cdc",
  "description": "Ingest Change Data Capture flow with APPLY INTO and simplify SCDT2 implementation.",
  "fullDescription": "This demo highlight how Delta Live Table simplify CDC (Change Data Capture).<br/> CDC is typically done ingesting changes from external system (ERP, SQL databases) with tools like fivetran, debezium etc. <br/> In this demo, we'll show you how to re-create your table consuming CDC information. <br/>We'll also implement a SCD2 (Slowly Changing Dimention table of type 2). While this can be really tricky to implement when data arrives out of order, DLT makes this super simple with one simple keyword.<br/><br/>Ultimately, we'll show you how to programatically scan multiple incoming folder and trigger N stream (1 for each CDC table), leveraging DLT with python.",
    "usecase": "Data Engineering",
  "products": ["Delta Live Table", "Delta Lake", "Spark", "CDC"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks Delta Lake CDC", "url": "https://www.databricks.com/blog/2022/04/25/simplifying-change-data-capture-with-databricks-delta-live-tables.html"}],
  "recommended_items": ["dlt-unit-test", "dlt-loans", "cdc-pipeline"],
  "demo_assets": [
      {"title": "Delta Live Table pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/dlt-cdc-dlt-0.png"}],
  "bundle": True,
  "tags": [{"dlt": "Delta Live Table"}],
  "notebooks": [
    {
      "path": "_resources/00-Data_CDC_Generator", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "CDC data generator", 
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
      "path": "01-Retail_DLT_CDC_SQL", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (SQL)", 
      "description": "CDC flow in SQL with Delta Live Table"
    },
    {
      "path": "02-Retail_DLT_CDC_Python", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (Python)", 
      "description": "CDC flow in Python with Delta Live Table"
    },
    {
      "path": "03-Retail_DLT_CDC_Monitoring", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Pipeline expectation monitoring", 
      "description": "Extract data from expectation for DBSQL dashboard.",
      "parameters": {"storage_path": "/demos/dlt/cdc/quentin_ambard"}
    },
    {
      "path": "04-Retail_DLT_CDC_Full", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Programatically handle multiple CDC flow", 
      "description": "Use python to create a dynamic CDC pipelines with N tables."
    }
  ],
  "init_job": {
    "settings": {
        "name": "demos_dlt_cdc_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/01-load-data-quality-dashboard",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "start_dlt_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_DLT_ID_dlt-cdc}}",
                    "full_refresh": true
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                    {
                        "task_key": "init_data"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "spark_version": "14.3.x-cpu-ml-scala2.12",
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
      "id": "dlt-cdc",
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
                    "path": "{{DEMO_FOLDER}}/01-Retail_DLT_CDC_SQL"
                }
            }
        ],
        "name": "dbdemos_dlt_cdc_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "target": "{{SCHEMA}}"
      }
    }
  ],
  "dashboards": [{"name": "[dbdemos] Delta Lake - Data Quality Stats",  "id": "dlt-expectations"}]
}
