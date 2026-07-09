# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "dbsql-for-dim-etl",
    "category": "DBSQL",
    "title": "DBSQL: Create & Populate Type 2 Patient Dimension",
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_sql_etl",
    "description": "The demo will illustrate the data architecture and data workflow that creates and populates a dimension in a Star Schema using Databricks SQL. This will utilize a Patient dimension in the Healthcare domain. The demo will illustrate all facets of an end-to-end ETL to transform, validate, and load an SCD2 dimension.",
    "bundle": True,
    "notebooks": [
      {
        "path": "00-get-started-with-SQL", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Get Started with Databricks SQL", 
        "description": "Start here to explore the demo."
      },
      {
        "path": "01-patient-dimension-ETL-introduction", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Patient Dimension ETL Introduction", 
        "description": "Start here to explore the demo."
      },
      {
        "path": "sql-centric-capabilities-examples", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "SQL Centric Capabilities Examples", 
        "description": "Start here to explore the SQL Scripting example."
      },
      {
        "path": "01-Setup/01.1-initialize", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Configure and Initialize", 
        "description": "Configure demo catalog, schema, and initialize global variables."
      },
      {
        "path": "01-Setup/01.2-setup", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create demo Catalog/Schema/Volume", 
        "description": "Create demo Catalog/Schema/Volume."
      },
      {
        "path": "02-Create/02.1-create-code-table", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create Code Table", 
        "description": "Create the code master table and initialize with sample data."
      },
      {
        "path": "02-Create/02.2-create-ETL-log-table", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create ETL Log Table", 
        "description": "Create the ETL log table to log metadata on each ETL run."
      },
      {
        "path": "02-Create/02.3-create-patient-tables", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create Patient Tables", 
        "description": "Create the patient staging, patient integration, and patient dimension tables."
      },
      {
        "path": "03-Populate/03.1-patient-dimension-ETL", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Populate Patient Dimension", 
        "description": "Populate the patient staging, patient integration, and patient dimension tables."
      },
      {
        "path": "_resource/browse-load", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Browse Load", 
        "description": "Browse the data populated in the patient tables and log table."
      },
      {
        "path": "_resource/stage-source-file-init", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Stage Source Data File - Initial Load", 
        "description": "Stage the source CSV file for initial load onto the staging volume and folder."
      },
      {
        "path": "_resource/stage-source-file-incr", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Stage Source Data File - Incremental Load", 
        "description": "Stage the source CSV file for incremental load onto the staging volume and folder."
      }
    ],
    "init_job": {
        "settings": {
          "name": "dbdemos_patient_dimension_etl_{{CATALOG}}_{{SCHEMA}}",
          "email_notifications": {
            "no_alert_for_skipped_runs": False
          },
          "webhook_notifications": {},
          "timeout_seconds": 0,
          "max_concurrent_runs": 1,
          "tasks": [
            {
              "task_key": "INITIALIZE_AND_SETUP_SCHEMA",
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/01-Setup/01.2-setup",
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "CREATE_CODE_TABLE",
              "depends_on": [
                {
                  "task_key": "INITIALIZE_AND_SETUP_SCHEMA"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/02-Create/02.1-create-code-table",
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "CREATE_LOG_TABLE",
              "depends_on": [
                {
                  "task_key": "INITIALIZE_AND_SETUP_SCHEMA"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/02-Create/02.2-create-ETL-log-table",
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "CREATE_PATIENT_TABLES",
              "depends_on": [
                {
                  "task_key": "INITIALIZE_AND_SETUP_SCHEMA"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/02-Create/02.3-create-patient-tables",
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "demo_StgSrcFileInit",
              "depends_on": [
                {
                  "task_key": "INITIALIZE_AND_SETUP_SCHEMA"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/_resource/stage-source-file-init",
                "source": "WORKSPACE"
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "INITIAL_LOAD_PATIENT",
              "depends_on": [
                {
                  "task_key": "CREATE_PATIENT_TABLES"
                },
                {
                  "task_key": "CREATE_CODE_TABLE"
                },
                {
                  "task_key": "CREATE_LOG_TABLE"
                },
                {
                  "task_key": "demo_StgSrcFileInit"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/03-Populate/03.1-patient-dimension-ETL",
                "base_parameters": {
                  "p_process_id": "{{job.id}}-{{job.run_id}}"
                },
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {},
              "description": "Initial load of Patient Tables"
            },
            {
              "task_key": "demo_BrowseResultInit",
              "depends_on": [
                {
                  "task_key": "INITIAL_LOAD_PATIENT"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/_resource/browse-load",
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {},
              "description": "Browse results of the initial load"
            },
            {
              "task_key": "demo_StgSrcFileIncr",
              "depends_on": [
                {
                  "task_key": "INITIAL_LOAD_PATIENT"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/_resource/stage-source-file-incr",
                "source": "WORKSPACE"
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "INCREMENTAL_LOAD_PATIENT",
              "depends_on": [
                {
                  "task_key": "demo_StgSrcFileIncr"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/03-Populate/03.1-patient-dimension-ETL",
                "base_parameters": {
                  "p_process_id": "{{job.id}}-{{job.run_id}}"
                },
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            },
            {
              "task_key": "demo_BrowseResultIncr",
              "depends_on": [
                {
                  "task_key": "INCREMENTAL_LOAD_PATIENT"
                }
              ],
              "run_if": "ALL_SUCCESS",
              "notebook_task": {
                "notebook_path": "{{DEMO_FOLDER}}/_resource/browse-load",
                "source": "WORKSPACE",
                "warehouse_id": ""
              },
              "timeout_seconds": 0,
              "email_notifications": {}
            }
          ],
          "format": "MULTI_TASK",
          "queue": {
            "enabled": True
          }
        },
    },
    "serverless_supported": True,
    "cluster": {},
    "pipelines": [],
}
