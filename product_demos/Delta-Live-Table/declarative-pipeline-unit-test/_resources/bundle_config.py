# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "declarative-pipeline-unit-test",
  "category": "data-engineering",
  "title": "Unit Testing Declarative Pipeline for production-grade pipelines",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_ldp_unit_test",
  "description": "Deploy robust pipelines with unit tests leveraging expectation.",
  "fullDescription": "Production-grade pipeline requires Unit Test to garantee their robustness. Spark Declarative Pipelines let you track your pipeline data quality with expectation in your table. <br/> These expectations can also be leverage to write integration tests, making robust pipeline. <br/> In this demo, we'll show you how to test your SDP pipeline and make it composable, easily switching input data with your test data.",
  "bundle": True,
  "notebooks": [
    {
      "path": "SDP-pipeline-to-test", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "SDP to test", 
      "description": "Definition of the pipeline we want to test."
    },
    {
      "path": "ingestion_profile/SDP-ingest_prod", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Prod ingestion source", 
      "description": "Define the production data source (ex: kafka)"
    },
    {
      "path": "ingestion_profile/SDP-ingest_test", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Test ingestion source", 
      "description": "Define the test data source (ex: csv file crafted to validate our tests)"
    },
    {
      "path": "test/SDP-Test-Dataset-setup", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Test dataset creation", 
      "description": "Craft the data required for the tests (used by 'SDP-ingest_test')"
    },
    {
      "path": "test/SDP-Tests", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Unit test definition", 
      "description": "Main notebook containing the unit tests."
    }
  ],
  "init_job": {
    "settings": {
        "name": "field_demos_ldp_unit_test_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/test/SDP-Test-Dataset-setup",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "start_ldp_pipeline",
                "pipeline_task": {
                    "pipeline_id": "{{DYNAMIC_SDP_ID_dlt-test}}",
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
      "id": "sdp-test",
      "run_after_creation": False,
      "definition": {
        "clusters": [
            {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 5,
                    "mode": "LEGACY"
                }
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
                    "path": "{{DEMO_FOLDER}}/SDP-pipeline-to-test"
                }
            },
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/ingestion_profile/SDP-ingest_test"
                }
            },
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/test/SDP-Tests"
                }
            }
        ],
        "name": "dbdemos_ldp_unit_test_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "target": "{{SCHEMA}}"
      }
    }
  ]
}
