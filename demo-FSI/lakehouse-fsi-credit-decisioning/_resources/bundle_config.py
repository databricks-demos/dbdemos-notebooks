# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-fsi-credit",
  "category": "lakehouse",
  "title": "Lakehouse for Retail Banking: Credit Decisioning",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_fsi_credit_decisioning",
  "description": "Build your banking data platform and identify credit worthy customers",
  "fullDescription": "The Databricks Lakehouse Platform is an open architecture that combines the best elements of data lakes and data warehouses. In this demo, we'll show you how to build an end-to-end credit decisioning system for underbanked customers, delivering data and insights that would typically take months of effort on legacy platforms. <br/><br/>This demo covers the end to end lakehouse platform: <ul><li>Ingest both internal and partner data, and then transform them using Spark Declarative Pipelines (SDP), a declarative ETL framework for building reliable, maintainable, and testable data processing pipelines. </li><li>Secure our ingested data to ensure governance and security on top of PII data</li><li>Build a Machine Learning model with Databricks AutoML to identify credit worthy customers</li><li>Leverage Databricks DBSQL and the warehouse endpoints to build dashboard to analyze the ingested data and explain the machine learning model outputs</li><li>Orchestrate all these steps with Databricks Workflow</li></ul>",
  "usecase": "Lakehouse Platform",
  "products": ["Spark Declarative Pipelines", "Databricks SQL", "MLFLow", "Auto ML", "Unity Catalog", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks for Financial Services", "url": "https://www.databricks.com/solutions/industries/financial-services"}],
  "recommended_items": ["lakehouse-iot-platform", "lakehouse-fsi-fraud", "lakehouse-retail-c360"],
  "demo_assets": [
      {"title": "Spark Declarative Pipelines pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-fsi-credit-dlt-0.png"},
      {"title": "Databricks SQL Dashboard: Credit Decisioning", "url": "https://www.dbdemos.ai/assets/img/dbdemos/lakehouse-fsi-credit-dashboard-0.png"}],   "bundle": True,
  "tags": [{"dlt": "Spark Declarative Pipelines"},  {"ds": "Data Science"}, {"uc": "Unity Catalog"}, {"dbsql": "BI/DW/DBSQL"}],
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
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Prep data", 
      "description": "Prep data for demo."
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "FSI Credit Decisioning setup", 
      "description": "Setup schema and database name."
    },
    {
      "path": "00-Credit-Decisioning", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "FSI Credit Decisioning Intro", 
      "description": "Introduction notebook, start here to implement your FSI Lakehouse."
    },
    {
      "path": "01-Data-Ingestion/01-SDP-Internal-Banking-Data-SQL",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Main notebook",
      "description": "SQL SDP pipeline to ingest internal banking data & build clean tables."
    },
    {
      "path": "01-Data-Ingestion/01.1-sdp-sql/explorations/sample_exploration",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Sample exploration",
      "description": "Sample exploration notebook for pipeline."
    },
    {
      "path": "01-Data-Ingestion/01.1-sdp-sql/transformations/01-bronze.sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Bronze transformations",
      "description": "Bronze layer transformations."
    },
    {
      "path": "01-Data-Ingestion/01.1-sdp-sql/transformations/02-silver.sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Silver transformations",
      "description": "Silver layer transformations."
    },
    {
      "path": "01-Data-Ingestion/01.1-sdp-sql/transformations/03-gold.sql",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "SDP SQL - Gold transformations",
      "description": "Gold layer transformations."
    },
    {
      "path": "02-Data-Governance/02-Data-Governance-credit-decisioning", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Governance with Unity Catalog", 
      "description": "Secure your tables, lineage, auditlog..."
    },
    {
      "path": "03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Feature engineering", 
      "description": "Feature engineering for credit decisioning"
    },
    {
      "path": "03-Data-Science-ML/03.2-AutoML-credit-decisioning", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "AutoML", 
      "description": "ML model training using AutoML"
    },
    {
      "path": "03-Data-Science-ML/03.3-Batch-Scoring-credit-decisioning", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Batch Scoring", 
      "description": "Batch scoring using the best model generated by AutoML"
    },
    {
      "path": "03-Data-Science-ML/03.4-model-serving-BNPL-credit-decisioning", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Real-time Serving", 
      "description": "Create a real-time serving endpoint to enable Buy Now, Pay Later"
    },
    {
      "path": "03-Data-Science-ML/03.5-Explainability-and-Fairness-credit-decisioning", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Explainability and Fairness", 
      "description": "Expalain model outputs using Shapley values and evaluate the model fairness"
    },
    {
      "path": "04-BI-Data-Warehousing/04-BI-Data-Warehousing-credit-decisioning", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Datawarehousing & BI / Dashboarding", 
      "description": "Run interactive queries on top of your data"
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
      "path": "06-Workflow-Orchestration/06-Workflow-Orchestration-credit-decisioning", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Orchestrate the credit scoring with Databricks Workflows", 
      "description": "Orchestrate all tasks in a job and schedule your data/model refresh"
    }
  ],
  "init_job": {
    "settings": {
        "name": "dbdemos_lakehouse_fsi_credit_init__{{CATALOG}}_{{SCHEMA}}",
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
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "catalog": "{{CATALOG}}",
                        "db": "{{SCHEMA}}",
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
                    "pipeline_id": "{{DYNAMIC_SDP_ID_dlt-fsi-credit-decisioning}}",
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
                "task_key": "feature_engineering",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning",
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
                "task_key": "automl_model",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.2-AutoML-credit-decisioning",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "feature_engineering"
                      }
                  ]
            },
            {
                "task_key": "batch_scoring",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.3-Batch-Scoring-credit-decisioning",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "automl_model"
                      }
                  ]
            },
            {
                "task_key": "explainability_and_fairness",
                "depends_on": [
                    {
                        "task_key": "batch_scoring"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.5-Explainability-and-Fairness-credit-decisioning",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": false,
                    "no_alert_for_canceled_runs": false,
                    "alert_on_last_attempt": false
                }
            },
            {
                "task_key": "create_ai_functions",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/05-Generative-AI/05.1-AI-Functions-Creation",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "depends_on": [
                      {
                          "task_key": "explainability_and_fairness"
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
      "id": "sdp-fsi-credit-decisioning",
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
                }
            },
            {
                "glob": {
                    "include": "{{DEMO_FOLDER}}/01-Data-Ingestion/01.1-sdp-sql/transformations/**"
                }
            }
        ],
        "name": "dbdemos_sdp_lakehouse_credit_decisioning_{{CATALOG}}_{{SCHEMA}}",
        "catalog": "{{CATALOG}}",
        "schema": "{{SCHEMA}}",
        "root_path": "{{DEMO_FOLDER}}/01-Data-Ingestion"
      }
    }
  ],
  "workflows": [{
    "start_on_install": False,
    "id": "credit-job",
    "definition": {
        "settings": {
            "name": "dbdemos_lakehouse_fsi_credit_{{CURRENT_USER_NAME}}",
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
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Shared_job_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {}
                }, 
                {
                    "task_key": "start_dlt_pipeline",
                    "pipeline_task": {
                        "pipeline_id": "{{DYNAMIC_SDP_ID_dlt-fsi-credit-decisioning}}",
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
                    "task_key": "feature_engineering",
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning",
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
                    "task_key": "automl_model",
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.2-AutoML-credit-decisioning",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Shared_job_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "depends_on": [
                          {
                              "task_key": "feature_engineering"
                          }
                      ]
                },
                {
                    "task_key": "batch_scoring",
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.3-Batch-Scoring-credit-decisioning",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Shared_job_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "depends_on": [
                          {
                              "task_key": "automl_model"
                          }
                      ]
                },
                {
                    "task_key": "explainability_and_fairness",
                    "depends_on": [
                        {
                            "task_key": "batch_scoring"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.5-Explainability-and-Fairness-credit-decisioning",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "Shared_job_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {
                        "no_alert_for_skipped_runs": false,
                        "no_alert_for_canceled_runs": false,
                        "alert_on_last_attempt": false
                    }
                },
                {
                    "task_key": "real_time_serving",
                    "depends_on": [
                        {
                            "task_key": "automl_model"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "{{DEMO_FOLDER}}/03-Data-Science-ML/03.5-Explainability-and-Fairness-credit-decisioning",
                        "source": "WORKSPACE"
                    },


                    "job_cluster_key": "Shared_job_cluster",

                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "notification_settings": {}
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
      }
    }
  ],
  "dashboards": [{"name": "[dbdemos] FSI Credit Decisioning Analysis",       "id": "credit-decisioning"}],
  "genie_rooms": [
      {
          "id": "fsi-credit-decisioning",
          "display_name": "DBDemos - FSI - Credit Decisioning",
          "description": "",
          "table_identifiers": [
              "{{CATALOG}}.{{SCHEMA}}.customer_gold",
              "{{CATALOG}}.{{SCHEMA}}.credit_bureau_gold",
              "{{CATALOG}}.{{SCHEMA}}.shap_explanation",
              "{{CATALOG}}.{{SCHEMA}}.feature_definitions",
              "{{CATALOG}}.{{SCHEMA}}.funds_trans_silver"
          ],
          "sql_instructions": [
              {
                  "title": "monthly trend on fund transfers",
                  "content": "SELECT month, total_amount, ROUND(total_amount - LAG(total_amount) OVER (ORDER BY month), 2) AS diff, ROUND((total_amount - LAG(total_amount) OVER (ORDER BY month)) / LAG(total_amount) OVER (ORDER BY month) * 100, 2) AS change_pct FROM (SELECT month, SUM(txn_amt) AS total_amount FROM (SELECT DISTINCT payer_acc_id, payee_cust_id, txn_amt, date_trunc('month', datetime) AS month FROM {{CATALOG}}.{{SCHEMA}}.fund_trans_silver) GROUP BY month ORDER BY month)"
              }
          ],
          "curated_questions": [
              "Display important factors for credit worthiness and aggregated stats on 6-month totals for the underbanked population",
              "Show credit decisions and distribution of approved customers vs unapproved customers.",
              "Show the trend on fund transfers over time",
              "Summarize my customers"
          ]
      }
  ]
}
