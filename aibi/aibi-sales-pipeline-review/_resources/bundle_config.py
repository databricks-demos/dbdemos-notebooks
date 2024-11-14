# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "aibi-sales-pipeline-review",
    "category": "AI-BI",
    "title": "AI/BI: Sales Pipeline Review",
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_aibi_sales_pipeline_review",
    "description": "Optimize your sales pipeline visibility and insights with AI/BI Dashboards, and leverage Genie to ask questions about your data in natural language.",
    "bundle": True,
    "notebooks": [
      {
        "path": "AI-BI-Sales-pipeline",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "AI BI: Sales Pipeline Review",
        "description": "Discover Databricks Intelligence Data Platform capabilities."
      }
    ],
    "init_job": {},
    "cluster": {},
    "pipelines": [],
    "dashboards": [
      {
        "name": "[dbdemos] AIBI - Sales Pipeline Review",
        "id": "sales-pipeline"
      }
    ],
    "data_folders": [
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/accounts",
        "source_format": "parquet",
        "target_table_name": "accounts",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/opportunity",
        "source_format": "parquet",
        "target_table_name": "opportunity",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/user",
        "source_format": "parquet",
        "target_table_name": "user",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/opportunityhistory_cube",
        "source_format": "parquet",
        "target_table_name": "opportunityhistory_cube",
        "target_format": "delta"
      }
    ],
    "genie_rooms": [
      {
        "id": "sales-pipeline",
        "display_name": "DBDemos - AI/BI - Sales Pipeline Review",
        "description": "Analyze your Sales Pipeline performance leveraging this AI/BI Dashboard. Deep dive into your data and metrics.",
        "table_identifiers": [
          "{{CATALOG}}.{{SCHEMA}}.accounts",
          "{{CATALOG}}.{{SCHEMA}}.opportunity",
          "{{CATALOG}}.{{SCHEMA}}.opportunityhistory_cube",
          "{{CATALOG}}.{{SCHEMA}}.user"
        ],
        "sql_instructions": [
          {
            "title": "Average active opportunity size",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT AVG(`opportunity_amount`) AS `avg(opportunity_amount)` FROM q GROUP BY GROUPING SETS (())"
          },
          {
            "title": "Distribution of stages across all opportunities",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT COUNT(`stagename`) AS `count(stagename)`, `stagename` FROM q GROUP BY `stagename`"
          },
          {
            "title": "Pipeline Amount by Region",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT `region`, SUM(`pipeline_amount`) AS `sum(pipeline_amount)` FROM q GROUP BY `region`"
          },
          {
            "title": "Average days to close",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT AVG(`days_to_close`) AS `avg(days_to_close)` FROM q GROUP BY GROUPING SETS (())"
          }
        ],
        "instructions": "- Financial years run from February through to the end of January the following year.\n- For example deals created in FY24 are createddate >= '2024-02-01' AND createddate < '2025-02-01'\n- Deals closed in FY24 are closedate >= '2024-02-01'  AND closedate < '2025-02-01'\n\n- There are 4 financial quarters every fiscal year. Each quarter is 3 months long. Q1 starts in February and ends in April. Q2 starts in may and ends in July. This quarter is Q2. We are currently in Q2.\n\n- Always round metrics in results to 2 decimal places. For example use ROUND(foo,2) where foo is the metric getting returned.\n\n- When someone asks about europe that means EMEA, america and north america mean AMER.\n\n- When someone asks about top sales performance for sales reps respond with the person who generated the most pipeline across all stages.\n\n- a won sale is one with the stagename \"5. Closed Won\" a lost sale is one with the stagename \"X. Closed Lost\"\n\n- Remember to filter out nulls for forecastcategory\n\n- Remember to use pipeline_amount when asked about pipeline\n- When asked about pipeline start by breaking down totals by region\n\n- include user name when asked about sales reps\n\n \n\n- include account name when asked about a specific account\n",
        "curated_questions": [
          "How's my pipeline just in the americas and by segment?",
          "What customers churned in America and how much revenue were they paying us?",
          "How's my pipeline?",
          "Who are my biggest customers globally?",
          "What is my average active opportunity size?"
        ]
      }
    ]
  }

# COMMAND ----------


