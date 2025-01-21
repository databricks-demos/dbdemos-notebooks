# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "dbt-on-databricks",
  "category": "data-engineering",
  "title": "Orchestrate and run your dbt jobs",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_dbt_retail",
  "description": "Launch your dbt pipelines in production using a SQL Warehouse. Leverage Databricks Workflow (orchestration) and add a dbt task in your transformation pipeline.",
  "fullDescription": "dbt is a popular data framework to transform and load data into your Lakehouse. Databricks makes it very easy to launch production-grade dbt pipeline using your Databricks SQL warehouse.<br/>In this dbt + Databricks demo, we'll cover: <ul><li>How to build a dbt pipeline to ingest our customer datasets. (We'll be building the same pipeline as the one available in the 'lakehouse-retail-c360' demo.)</li><li>How to start your dbt pipeline from your IDEA</li><li>And ultimately how Databricks Workflow can start dbt tasks to orchestrate your production run.</li></ul><br/>Note: this demo will clone for you the repo <a href='https://github.com/databricks-demos/dbt-databricks-c360'>https://github.com/databricks-demos/dbt-databricks-c360</a> in your repo personal folder.",
  "usecase": "Data Engineering",
  "products": ["dbt", "Workflows"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks and dbt Cloud", "url": "https://www.databricks.com/blog/2022/11/17/introducing-native-high-performance-integration-dbt-cloud.html"}],
  "recommended_items": ["delta-lake", "dlt-loans", "dlt-cdc"],
  "demo_assets": [],     
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "00-DBT-on-databricks", 
      "pre_run": False,
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DBT on Databricks", 
      "description": "Start here to explore DBT."
    }    
  ],
  "repos": [{
            "id": "dbt-databricks-c360",
            "path": "{{DEMO_FOLDER}}/dbdemos-dbt-databricks-c360",
            "url": "https://github.com/databricks-demos/dbt-databricks-c360",
            "provider": "gitHub",
            "branch": "main"}],
  "workflows": [
      {
       "start_on_install": true,
       "id": "dbt" ,
       "definition": {
        "run_as_owner": true,
        "settings": {
            "name": "dbdemos-dbt-{{CURRENT_USER_NAME}}",
            "email_notifications": {
                "no_alert_for_skipped_runs": false
            },
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "01-autoloader-data-ingestion",
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/dbdemos-dbt-databricks-c360/01-load-raw-data/01-load-data",
                        "source": "WORKSPACE",
                        "base_parameters": {
                            "catalog": "{{CATALOG}}",
                            "schema": "{{SCHEMA}}"
                        }
                    },
                    "job_cluster_key": "dbdemos-dbt-workflow-cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "02-dbt-data-transformation",
                    "depends_on": [
                        {
                            "task_key": "01-autoloader-data-ingestion"
                        }
                    ],
                    "dbt_task": {
                        "project_directory": "{{DEMO_FOLDER}}/dbdemos-dbt-databricks-c360",
                        "commands": [
                            "dbt run",
                            "dbt test --store-failures --vars '{\"catalog\": \"{{CATALOG}}\", \"schema\":\"{{SCHEMA}}\"}'"
                        ],
                        "warehouse_id": "{{SHARED_WAREHOUSE_ID}}",
                        "schema": "{{SCHEMA}}",
                        "catalog": "{{CATALOG}}",
                        "source": "WORKSPACE"
                    },
                    "libraries": [
                        {
                            "pypi": {
                                "package": "dbt-databricks==1.8.7"                            }
                        }
                    ],
                    "job_cluster_key": "dbdemos-dbt-workflow-cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "03-ml-churn-prediction",
                    "depends_on": [
                        {
                            "task_key": "02-dbt-data-transformation"
                        }
                    ],
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/dbdemos-dbt-databricks-c360/03-ml-predict-churn/03-churn-prediction",
                        "source": "WORKSPACE",
                        "base_parameters": {
                            "catalog": "{{CATALOG}}",
                            "schema": "{{SCHEMA}}"
                        }
                    },
                    "libraries": [
                        {
                            "pypi": {
                                "package": "databricks-sdk==0.41.0"
                            }
                        }
                    ],
                    "job_cluster_key": "dbdemos-dbt-workflow-cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {}
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "dbdemos-dbt-workflow-cluster",
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
                        "data_security_mode": "SINGLE_USER",
                        "runtime_engine": "STANDARD",
                        "num_workers": 0
                    }
                }
            ],
            "format": "MULTI_TASK"
        }
      }
    }
  ]
}
