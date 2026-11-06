# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "data-vault-sdp",
    "category": "data-engineering",
    "title": "Data Vault 2.0 on Spark Declarative Pipelines",
    "serverless_supported": True,
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_data_vault_sdp",
    "description": "Build a complete Data Vault 2.0 model on Spark Declarative Pipelines, from raw ingestion through business vault to a queryable information mart.",
    "bundle": True,
    "tags": ["data-vault", "sdp", "spark-declarative-pipelines", "data-engineering", "dimensional-modeling"],
    "notebooks": [
      {
        "path": "_resources/00-setup",
        "pre_run": True,
        "publish_on_website": False,
        "add_cluster_setup_cell": False,
        "title": "Setup and Data Generation",
        "description": "Create catalog/schema, seed bronze tables from samples.tpch, and define helper functions."
      },
      {
        "path": "00-data-vault-intro",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "Data Vault 2.0 Introduction",
        "description": "What is Data Vault, when to choose it, and the honest trade-offs. Includes architecture diagram."
      },
      {
        "path": "01-raw-vault-hubs-links-sats",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "Raw Vault: Hubs, Links, and Satellites",
        "description": "Build the foundational Data Vault layers using SDP: hubs, links, satellites including multi-active satellites."
      },
      {
        "path": "02-business-vault-and-pit",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "Business Vault and Point-in-Time Tables",
        "description": "Computed satellites, PIT tables for fast reassembly, and the trade-offs between query cost and query speed."
      },
      {
        "path": "03-information-mart-star-schema",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "Information Mart: Star Schema",
        "description": "Dimensional model and fact table over the vault. Compare query cost between vault and mart."
      },
      {
        "path": "04-schema-evolution-and-history",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "Schema Evolution and SCD2 History",
        "description": "Simulate source changes, re-run the pipeline, and see how satellites track version history with valid-from/to dates."
      }
    ],
    "cluster": {
      "num_workers": 2,
      "spark_version": "17.3.x-scala2.13",
      "spark_conf": {},
      "data_security_mode": "USER_ISOLATION",
      "runtime_engine": "STANDARD"
    },
    "pipelines": [
      {
        "id": "data-vault-sdp",
        "run_after_creation": False,
        "definition": {
          "clusters": [
              {
                  "label": "default",
                  "num_workers": 2
              }
          ],
          "development": True,
          "continuous": False,
          "channel": "CURRENT",
          "edition": "ADVANCED",
          "photon": True,
          "root_path": "{{DEMO_FOLDER}}",
          "libraries": [
              {"notebook": {"path": "{{DEMO_FOLDER}}/01-raw-vault-hubs-links-sats"}},
              {"notebook": {"path": "{{DEMO_FOLDER}}/02-business-vault-and-pit"}},
              {"notebook": {"path": "{{DEMO_FOLDER}}/03-information-mart-star-schema"}}
          ],
          "name": "dbdemos_data_vault_{{CATALOG}}_{{SCHEMA}}",
          "catalog": "{{CATALOG}}",
          "schema": "{{SCHEMA}}",
          "event_log": {
              "catalog": "{{CATALOG}}",
              "schema": "{{SCHEMA}}",
              "name": "data_vault_event_logs"
          },
          "configuration": {
            "catalog": "{{CATALOG}}",
            "schema": "{{SCHEMA}}"
          }
        }
      }
    ],
    "dashboards": [
      {"name": "[dbdemos] Data Vault Analytics", "id": "data-vault-analytics"}
    ]
}
