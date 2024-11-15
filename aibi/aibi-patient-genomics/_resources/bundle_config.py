# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "aibi-patient-genomics",
  "category": "AI-BI",
  "title": "AI/BI: Patient Genomics Review for Precision Oncology",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_aibi_hls_genomics",
  "description": "Optimize your patient genomics visibility and insights with AIBI Dashboards, and leverage Genie to ask questions about your data in natural language.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Patient-genomics",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title": "AI BI: Patient Genomics Review",
      "description": "Discover Databricks Intelligence Data Platform capabilities."
    }
  ],
  "init_job": {},
  "serverless_supported": True,
  "cluster": {},
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AIBI - Patient Genomics Review for Precision Oncology",
      "id": "patient-genomics"
    }
  ],
  "data_folders": [
    {
      "source_folder": "aibi/dbdemos_aibi_hls_genomics/demographics",
      "source_format": "parquet",
      "target_table_name": "demographics",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_hls_genomics/diagnoses",
      "source_format": "parquet",
      "target_table_name": "diagnoses",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_hls_genomics/exposures",
      "source_format": "parquet",
      "target_table_name": "exposures",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_hls_genomics/expression_profiles_umap",
      "source_format": "parquet",
      "target_table_name": "expression_profiles_umap",
      "target_format": "delta"
    }
  ],
  "genie_rooms": [
    {
      "id": "patient-genomics",
      "display_name": "DBDemos - AI/BI - Patient Genomics Data Review",
      "description": "Analyze your patient genomics data with this AIBI Dashboard. Deep dive into genomic insights and patient metrics.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.demographics",
        "{{CATALOG}}.{{SCHEMA}}.diagnoses",
        "{{CATALOG}}.{{SCHEMA}}.exposures",
        "{{CATALOG}}.{{SCHEMA}}.expression_profiles_umap"
      ],
      "sql_instructions": [
        {
          "title": "Which cancer types show the highest correlation with exposure to tobacco?",
          "content": "SELECT d.`primary_diagnosis`, AVG(e.`cigarettes_per_day`) AS avg_cigarettes_per_day, AVG(e.`years_smoked`) AS avg_years_smoked FROM {{CATALOG}}.{{SCHEMA}}.`diagnoses` d JOIN {{CATALOG}}.{{SCHEMA}}.`exposures` e ON d.`case_id` = e.`case_id` WHERE e.`cigarettes_per_day` IS NOT NULL AND e.`years_smoked` IS NOT NULL GROUP BY d.`primary_diagnosis` ORDER BY avg_cigarettes_per_day DESC, avg_years_smoked DESC"
        },
        {
          "title": "Which diagnoses are associated with the highest mortality rates within our patient population?",
          "content": "SELECT d.`primary_diagnosis`, COUNT(*) AS total_cases, SUM(CASE WHEN dem.`year_of_death` IS NOT NULL THEN 1 ELSE 0 END) AS total_deaths, (SUM(CASE WHEN dem.`year_of_death` IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS death_percentage FROM {{CATALOG}}.{{SCHEMA}}.`diagnoses` d JOIN {{CATALOG}}.{{SCHEMA}}.`demographics` dem ON d.`case_id` = dem.`case_id` GROUP BY d.`primary_diagnosis` ORDER BY death_percentage DESC"
        }
      ],
      "instructions": "- A patient is \"deceased\" or \"dead\" if their year_of_death is not null.",
      "curated_questions": [
        "Which cancer types show the highest correlation with exposure to tobacco?",
        "What is the most common organ or tissue of organ for patients with Malignant lymphoma?",
        "What are the top 10 cancer types in this dataset by number of cases?",
        "What organs or tissues act as origin sites for cancer most frequently among women?",
        "Which diagnoses are associated with the highest mortality rates within our patient population?"
      ]
    }
  ]
}

# COMMAND ----------


