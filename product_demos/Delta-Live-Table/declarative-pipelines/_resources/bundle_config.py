# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "pipeline-bike",
    "category": "data-engineering",
    "title": "Full Declarative Pipeline - Bike",
    "serverless_supported": True,
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_pipeline_bike",
    "description": "Ingest bike rental data and implement a Lakeflow Declarative Pipeline, with expectation and monitoring.",
    "bundle": True,
    "notebooks": [
      {
        "path": "_resources/01-Bike-Data-generator", 
        "pre_run": False, 
        "publish_on_website": False, 
        "add_cluster_setup_cell": False,
        "title":  "Bike data generator", 
        "description": "Generate data for the pipeline."
      },
      {
        "path": "00-Lakeflow-Declarative-Pipeline-Introduction", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Lakeflow Declarative Pipeline: Introduction", 
        "description": "Start here to learn about your Pipeline"
      },
      {
        "path": "explorations/01-Exploring-the-Data", 
        "pre_run": True, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Data Exploration", 
        "description": "Run interactive queries to better understand your data"
      },
      {
        "path": "explorations/02-Pipeline-event-monitoring", 
        "pre_run": True, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Pipeline data monitoring", 
        "description": "Interactive queries to learn about your Declarative Pipeline metadata."
      },
      {
        "path": "transformations/00-pipeline-tutorial", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Declarative Pipeline introduction", 
        "description": "Learn about Streaming table, Materialized view and more."
      },
      {
        "path": "transformations/01-bronze.sql", 
        "pre_run": False, 
        "publish_on_website": False, 
        "add_cluster_setup_cell": False,
        "title":  "Bronze SQL tables", 
        "description": "Ingest the raw data."
      },
      {
        "path": "transformations/02-silver.sql", 
        "pre_run": False, 
        "publish_on_website": False, 
        "add_cluster_setup_cell": False,
        "title":  "Silver SQL tables", 
        "description": "Clean and prepare your data."
      },
      {
        "path": "transformations/03-gold.sql", 
        "pre_run": False, 
        "publish_on_website": False, 
        "add_cluster_setup_cell": False,
        "title":  "Gold SQL tables", 
        "description": "Final aggregation layer, for ML and BI usage."
      }
    ],
    "init_job": {
      "settings": {
          "name": "dbdemos_job_bike_init_{{CURRENT_USER_NAME}}",
          "email_notifications": {
              "no_alert_for_skipped_runs": False
          },
          "timeout_seconds": 0,
          "max_concurrent_runs": 1,
          "tasks": [
              {
                  "task_key": "init_data",
                  "notebook_task": {
                      "notebook_path": "{{DEMO_FOLDER}}/_resources/01-Bike-Data-generator",
                      "source": "WORKSPACE"
                  },
                  "job_cluster_key": "Shared_job_cluster",
                  "timeout_seconds": 0,
                  "email_notifications": {}
              },
              {
                  "task_key": "start_dlt_pipeline",
                  "pipeline_task": {
                      "pipeline_id": "{{DYNAMIC_DLT_ID_pipeline-bike}}",
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
                      "spark_version": "16.4.x-scala2.12",
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
      "spark_version": "16.4.x-scala2.12",
      "spark_conf": {},
      "data_security_mode": "USER_ISOLATION",
      "runtime_engine": "STANDARD"
    },
    "pipelines": [
      {
        "id": "pipeline-bike",
        "run_after_creation": False,
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
          "root_path": "{{DEMO_FOLDER}}",
          "libraries": [
              {"glob": {"include": "{{DEMO_FOLDER}}/transformations/01-bronze.sql"}},
              {"glob": {"include": "{{DEMO_FOLDER}}/transformations/02-silver.sql"}},
              {"glob": {"include": "{{DEMO_FOLDER}}/transformations/03-gold.sql"}}
          ],
          "name": "dbdemos_pipeline_bike_{{CATALOG}}_{{SCHEMA}}",
          "catalog": "{{CATALOG}}",
          "schema": "{{SCHEMA}}",
          "event_log": {
              "catalog": "{{CATALOG}}",
              "schema": "{{SCHEMA}}",
              "name": "pipeline_bike_event_logs"
          },
          "configuration": {
            "catalog": "{{CATALOG}}",
            "schema": "{{SCHEMA}}"
          }
        }
      }
    ],
    "dashboards": [{"name": "[dbdemos] LDP - Bike Rental Business Insights",  "id": "bike-rental"},
                   {"name": "[dbdemos] LDP - Bike Operational insights",  "id": "operational"},
                   {"name": "[dbdemos] LDP - Bike Data Monitoring",  "id": "data-quality"}]
  }
