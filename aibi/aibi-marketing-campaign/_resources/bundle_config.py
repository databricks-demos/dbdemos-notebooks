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
  "serverless_supported": True,
  "cluster": {}, 
  "pipelines": [],
  "dashboards": [{"name": "[dbdemos] AIBI - Marketing Campaign",       "id": "web-marketing"}
                ],
  "data_folders":[
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_data/raw_campaigns",              "source_format": "parquet", "target_volume_folder":"raw_campaigns",              "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_data/raw_contacts",               "source_format": "parquet", "target_volume_folder":"raw_contacts",               "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_data/raw_events",                 "source_format": "parquet", "target_volume_folder":"raw_events",                 "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_data/raw_feedbacks",              "source_format": "parquet", "target_volume_folder":"raw_feedbacks",              "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_data/raw_issues",                 "source_format": "parquet", "target_volume_folder":"raw_issues",                 "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_data/raw_prospects",              "source_format": "parquet", "target_volume_folder":"raw_prospects",              "target_format":"parquet"}  ],
  "sql_queries": [
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_campaigns TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for campaigns created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/raw_data/raw_campaigns', format => 'parquet', pathGlobFilter => '*.parquet')",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_contacts TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for contacts created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/raw_data/raw_contacts', format => 'parquet', pathGlobFilter => '*.parquet')",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_events TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for events created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/raw_data/raw_events', format => 'parquet', pathGlobFilter => '*.parquet')",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_feedbacks TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for feedbacks created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/raw_data/raw_feedbacks', format => 'parquet', pathGlobFilter => '*.parquet')",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_issues TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for issues created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/raw_data/raw_issues', format => 'parquet', pathGlobFilter => '*.parquet')",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_prospects TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for prospects created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/raw_data/raw_prospects', format => 'parquet', pathGlobFilter => '*.parquet')",

      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_campaigns COMMENT 'Cleaned version of the raw campaigns table' AS SELECT CampaignId, CampaignName, CampaignDescription, SubjectLine, Template, Cost, StartDate, EndDate, MailingList FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_campaigns WHERE CampaignId IS NOT NULL AND CampaignName IS NOT NULL AND CampaignDescription IS NOT NULL AND SubjectLine IS NOT NULL AND Template IS NOT NULL AND Cost IS NOT NULL AND StartDate IS NOT NULL AND EndDate IS NOT NULL AND MailingList IS NOT NULL",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_contacts COMMENT 'Cleaned version of the raw contacts table' AS SELECT ContactId, ProspectId, Department, JobTitle, Source, Device, OptedOut FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_contacts WHERE ContactId IS NOT NULL AND ProspectId IS NOT NULL AND Department IS NOT NULL AND JobTitle IS NOT NULL AND Source IS NOT NULL AND Device IS NOT NULL AND OptedOut IS NOT NULL",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_events COMMENT 'Cleaned version of the raw events table' AS SELECT EventId, CampaignId, ContactId, EventType, EventDate, Metadata FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_events WHERE EventId IS NOT NULL AND CampaignId IS NOT NULL AND ContactId IS NOT NULL AND EventType IS NOT NULL AND EventDate IS NOT NULL AND Metadata IS NOT NULL",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_feedbacks COMMENT 'Cleaned version of the raw feedbacks table' AS SELECT CampaignId, ContactId, Feedbacks FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_feedbacks WHERE CampaignId IS NOT NULL AND ContactId IS NOT NULL AND Feedbacks IS NOT NULL",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_issues COMMENT 'Cleaned version of the raw issues table' AS SELECT CampaignId, ComplaintType, ContactId FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_issues WHERE CampaignId IS NOT NULL AND ComplaintType IS NOT NULL AND ContactId IS NOT NULL",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prospects COMMENT 'Cleaned version of the raw prospects table' AS SELECT ProspectId, ProspectName, AnnualRevenue, Employees, Industry, Country, City, Postcode FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_prospects WHERE ProspectId IS NOT NULL AND ProspectName IS NOT NULL AND AnnualRevenue IS NOT NULL AND Employees > 0 AND Industry IS NOT NULL AND Country IS NOT NULL AND City IS NOT NULL AND Postcode IS NOT NULL",

      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns AS SELECT CampaignId AS campaign_id, CampaignName AS campaign_name, CampaignDescription AS campaign_description, SubjectLine AS subject_line, Template AS template, Cost AS cost, CAST(StartDate AS date) AS start_date, CAST(EndDate AS date) AS end_date, collect_list(DISTINCT MailingList) AS mailing_list FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_campaigns GROUP BY CampaignId, CampaignName, CampaignDescription, SubjectLine, Template, Cost, StartDate, EndDate",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.contacts AS SELECT ContactId AS contact_id, ProspectId AS prospect_id, Department AS department, JobTitle AS job_title, Source AS source, Device As device, OptedOut AS opted_out FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_contacts",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events AS SELECT EventId AS event_id, CampaignId AS campaign_id, ContactId AS contact_id, EventType AS event_type, EventDate AS event_date, Metadata AS metadata FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_events",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks AS SELECT CampaignId AS campaign_id, Feedbacks AS feedbacks, ContactId AS contact_id FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_feedbacks",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.issues AS SELECT CampaignId AS campaign_id, ComplaintType AS complaint_type, ContactId AS contact_id FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_issues",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects AS SELECT ProspectId AS prospect_id, ProspectName AS name, AnnualRevenue AS annual_revenue, Employees AS employees, Industry AS industry, Country AS country, City AS city, Postcode AS postcode FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prospects",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.metrics_daily_rolling AS SELECT CAST(event_date AS date) AS date, count(distinct case when event_type = 'click' then contact_id end) as unique_clicks, SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered, SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent, SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens, SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks, SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts, SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam FROM `{{CATALOG}}`.`{{SCHEMA}}`.events GROUP BY date"
  ],
  "genie_rooms":[
    {
     "id": "marketing-campaign",
     "display_name": "DBDemos - AI/BI - Marketing Campaign",     
     "description": "Analyze your Marketing Campaign effectiveness leveraging AI/BI Dashboard. Deep dive into your data and metrics.",
     "table_identifiers": ["{{CATALOG}}.{{SCHEMA}}.campaigns",
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
            "content": "SELECT e.campaign_id, first(c.campaign_name) as campaign_name, first(c.campaign_description) as campaign_description, first(c.subject_line) as subject_line, first(c.template) as template, first(c.cost) as cost, first(c.start_date) as start_date, first(c.end_date) as end_date, to_timestamp(first(c.start_date)) as _start_date, to_timestamp(first(c.end_date)) as _end_date, SUM(CASE WHEN e.event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent, SUM(CASE WHEN e.event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered, SUM(CASE WHEN e.event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam, SUM(CASE WHEN e.event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens, SUM(CASE WHEN e.event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts, SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks, count(distinct case when e.event_type = 'click' then e.contact_id end) as unique_clicks, unique_clicks / total_delivered as ctr, format_number(ctr, '##.##%') as ctr_p, total_delivered / total_sent as delivery_rate, format_number(delivery_rate, '##.##%') as delivery_rate_p, total_optouts / total_delivered as optouts_rate, format_number(optouts_rate, '##.##%') as optouts_rate_p, total_spam / total_delivered as spam_rate, format_number(spam_rate, '##.##%') as spam_rate_p, sum(p.employees) as total_employees FROM {{CATALOG}}.{{SCHEMA}}.events e INNER JOIN {{CATALOG}}.{{SCHEMA}}.campaigns c on e.campaign_id = c.campaign_id INNER JOIN {{CATALOG}}.{{SCHEMA}}.contacts ct on e.contact_id = ct.contact_id INNER JOIN {{CATALOG}}.{{SCHEMA}}.prospects p on ct.prospect_id = p.prospect_id GROUP BY e.campaign_id HAVING _start_date >= :start_date AND _end_date <= :end_date ORDER BY ctr desc, cost ASC LIMIT 20"
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
