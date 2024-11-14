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
  "dashboards": [{"name": "[dbdemos] AIBI - Marketing Campaign",       "id": "web-marketing"}
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
     "sql_instructions": [
        {
            "title": "Compute rolling metrics",
            "content": "select date, unique_clicks, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS clicks_t7d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS delivered_t7d, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 27 PRECEDING AND CURRENT ROW) AS clicks_t28d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 27 PRECEDING AND CURRENT ROW) AS delivered_t28d, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 90 PRECEDING AND CURRENT ROW) AS clicks_t91d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 90 PRECEDING AND CURRENT ROW) AS delivered_t91d, unique_clicks / total_delivered as ctr, total_delivered / total_sent AS delivery_rate, total_optouts / total_delivered AS optout_rate, total_spam / total_delivered AS spam_rate, clicks_t7d / delivered_t7d as ctr_t7d, clicks_t28d / delivered_t28d as ctr_t28d, clicks_t91d / delivered_t91d as ctr_t91d from {{CATALOG}}.{{SCHEMA}}.metrics_daily_rolling"
        },
        {
            "title": "What are the campaigns with the highest click-through rates?",
            "content": "SELECT e.compaign_id, first(c.campaign_name) as campaign_name, first(c.compaign_description) as compaign_description, first(c.subject_line) as subject_line, first(c.template) as template, first(c.cost) as cost, first(c.start_date) as start_date, first(c.end_date) as end_date, to_timestamp(first(c.start_date)) as _start_date, to_timestamp(first(c.end_date)) as _end_date, SUM(CASE WHEN e.event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent, SUM(CASE WHEN e.event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered, SUM(CASE WHEN e.event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam, SUM(CASE WHEN e.event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens, SUM(CASE WHEN e.event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts, SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks, count(distinct case when e.event_type = 'click' then e.contact_id end) as unique_clicks, unique_clicks / total_delivered as ctr, format_number(ctr, '##.##%') as ctr_p, total_delivered / total_sent as delivery_rate, format_number(delivery_rate, '##.##%') as delivery_rate_p, total_optouts / total_delivered as optouts_rate, format_number(optouts_rate, '##.##%') as optouts_rate_p, total_spam / total_delivered as spam_rate, format_number(spam_rate, '##.##%') as spam_rate_p, sum(p.employees) as total_employees FROM {{CATALOG}}.{{SCHEMA}}.events e INNER JOIN {{CATALOG}}.{{SCHEMA}}.compaigns c on e.compaign_id = c.compaign_id INNER JOIN {{CATALOG}}.{{SCHEMA}}.contacts ct on e.contact_id = ct.contact_id INNER JOIN {{CATALOG}}.{{SCHEMA}}.prospects p on ct.prospect_id = p.prospect_id GROUP BY e.compaign_id HAVING _start_date >= :start_date AND _end_date <= :end_date ORDER BY ctr desc, cost ASC LIMIT 20"
        }
    ],
     "instructions": "If a customer ask a forecast, leverage the sql fonction ai_forecast",
     "curated_questions": [
        "How has the total number of emails sent, delivered, and the unique clicks evolved over the last six months?",
        "Which industries have shown the highest engagement rates with marketing campaigns?",
        "Which subject lines for my campaigns led to the most number of opens?",
        "Which campaigns had the strongest click-through rates (CTR)?"
       ]
    }
  ]
}
