# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "aibi-marketing-campaign",
  "category": "AI-BI",
  "title": "AI/BI: Marketing Campaign effectiveness",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_aibi_cme_marketing_campaign",
  "description": "Analyze your marketing campaign performance visually with AI/BI Dashboards. Then, utilize Genie to ask questions about your data in your natural language.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Marketing-campaign", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "AI BI: Campaign effectiveness", 
      "description": "Discover Databricks Intelligence Data Platform capabilities."
    }
  ],
  "init_job": {},
  "cluster": {}, 
  "pipelines": [],
  "dashboards": [{"name": "[dbdemos] AI/BI - Marketing Campaign",       "id": "web-marketing"}
                ],
  "data_folders":[
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/compaigns",              "source_format": "parquet", "target_table_name":"compaigns",              "target_format":"delta"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/contacts",               "source_format": "parquet", "target_table_name":"contacts",               "target_format":"delta"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/events",                 "source_format": "parquet", "target_table_name":"events",                 "target_format":"delta"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/feedbacks",              "source_format": "parquet", "target_table_name":"feedbacks",              "target_format":"delta"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/issues",                 "source_format": "parquet", "target_table_name":"issues",                 "target_format":"delta"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/metrics_daily_rolling",  "source_format": "parquet", "target_table_name":"metrics_daily_rolling",  "target_format":"delta"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/prospects",              "source_format": "parquet", "target_table_name":"prospects",              "target_format":"delta"}  ],
  "genie_rooms":[
    {
     "id": "marketing-campaign",
     "display_name": "DBDemos - AI/BI - Marketing Campaign",     
     "description": "Analyze your Marketing Campaign effectiveness leveraging AI/BI Dashboard. Deep dive into your data and metrics.",
     "table_identifiers": ["{{CATALOG}}.{{SCHEMA}}.compaigns",
                           "{{CATALOG}}.{{SCHEMA}}.contacts",
                           "{{CATALOG}}.{{SCHEMA}}.events",
                           "{{CATALOG}}.{{SCHEMA}}.feedbacks",
                           "{{CATALOG}}.{{SCHEMA}}.issues",
                           "{{CATALOG}}.{{SCHEMA}}.metrics_daily_rolling",
                           "{{CATALOG}}.{{SCHEMA}}.prospects"],
     "sql_instructions": [{"title": "Compute rolling metrics", "content": "select date, unique_clicks, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS clicks_t7d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS delivered_t7d, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 27 PRECEDING AND CURRENT ROW) AS clicks_t28d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 27 PRECEDING AND CURRENT ROW) AS delivered_t28d, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 90 PRECEDING AND CURRENT ROW) AS clicks_t91d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 90 PRECEDING AND CURRENT ROW) AS delivered_t91d, unique_clicks / total_delivered as ctr, total_delivered / total_sent AS delivery_rate, total_optouts / total_delivered AS optout_rate, total_spam / total_delivered AS spam_rate, clicks_t7d / delivered_t7d as ctr_t7d, clicks_t28d / delivered_t28d as ctr_t28d, clicks_t91d / delivered_t91d as ctr_t91d from {{CATALOG}}.{{SCHEMA}}.metrics_daily_rolling"}],
     "instructions": "If a customer ask a forecast, leverage the sql fonction ai_forecast",
     "curated_questions": [
       "What is the open rate?", 
       "What is the click-through rate (CTR)?", 
       "Were there any issues with email deliverability?", 
       "Are the email campaigns compliant with relevant regulations (e.g., GDPR, CAN-SPAM Act)?"
       ]
    }
  ]
}
