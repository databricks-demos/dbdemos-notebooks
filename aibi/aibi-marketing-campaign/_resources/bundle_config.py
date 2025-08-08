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
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_campaigns",              "source_format": "parquet", "target_volume_folder":"raw_campaigns",              "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_contacts",               "source_format": "parquet", "target_volume_folder":"raw_contacts",               "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_events",                 "source_format": "parquet", "target_volume_folder":"raw_events",                 "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_feedbacks",              "source_format": "parquet", "target_volume_folder":"raw_feedbacks",              "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_issues",                 "source_format": "parquet", "target_volume_folder":"raw_issues",                 "target_format":"parquet"},
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_prospects",              "source_format": "parquet", "target_volume_folder":"raw_prospects",              "target_format":"parquet"}  ],
  "sql_queries": [
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_campaigns TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for campaigns created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_campaigns', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_contacts TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for contacts created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_contacts', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_events TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for events created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_events', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_feedbacks TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for feedbacks created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_feedbacks', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_issues TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for issues created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_issues', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_prospects TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for prospects created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_prospects', format => 'parquet', pathGlobFilter => '*.parquet')"
      ],
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_campaigns COMMENT 'Cleaned version of the raw campaigns table' AS SELECT CampaignId, CampaignName, CampaignDescription, SubjectLine, Template, Cost, StartDate, EndDate, MailingList FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_campaigns WHERE CampaignId IS NOT NULL AND CampaignName IS NOT NULL AND CampaignDescription IS NOT NULL AND SubjectLine IS NOT NULL AND Template IS NOT NULL AND Cost IS NOT NULL AND StartDate IS NOT NULL AND EndDate IS NOT NULL AND MailingList IS NOT NULL",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_contacts COMMENT 'Cleaned version of the raw contacts table' AS SELECT ContactId, ProspectId, Department, JobTitle, Source, Device, OptedOut FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_contacts WHERE ContactId IS NOT NULL AND ProspectId IS NOT NULL AND Department IS NOT NULL AND JobTitle IS NOT NULL AND Source IS NOT NULL AND Device IS NOT NULL AND OptedOut IS NOT NULL",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_events COMMENT 'Cleaned version of the raw events table' AS SELECT EventId, CampaignId, ContactId, EventType, EventDate, Metadata FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_events WHERE EventId IS NOT NULL AND CampaignId IS NOT NULL AND ContactId IS NOT NULL AND EventType IS NOT NULL AND EventDate IS NOT NULL AND Metadata IS NOT NULL",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_feedbacks COMMENT 'Cleaned version of the raw feedbacks table' AS SELECT CampaignId, ContactId, Feedbacks FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_feedbacks WHERE CampaignId IS NOT NULL AND ContactId IS NOT NULL AND Feedbacks IS NOT NULL",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_issues COMMENT 'Cleaned version of the raw issues table' AS SELECT CampaignId, ComplaintType, ContactId FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_issues WHERE CampaignId IS NOT NULL AND ComplaintType IS NOT NULL AND ContactId IS NOT NULL",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prospects COMMENT 'Cleaned version of the raw prospects table' AS SELECT ProspectId, ProspectName, AnnualRevenue, Employees, Industry, Country, City, Postcode FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_prospects WHERE ProspectId IS NOT NULL AND ProspectName IS NOT NULL AND AnnualRevenue IS NOT NULL AND Employees > 0 AND Industry IS NOT NULL AND Country IS NOT NULL AND City IS NOT NULL AND Postcode IS NOT NULL"
      ],
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns ( campaign_id BIGINT COMMENT 'Unique identifier for each campaign', campaign_name STRING COMMENT 'Name of the marketing campaign', campaign_description STRING COMMENT 'Description of the campaign', subject_line STRING COMMENT 'Subject line used in campaign emails', template STRING COMMENT 'Email template used for the campaign', cost DOUBLE COMMENT 'Total cost of the campaign', start_date DATE COMMENT 'Start date of the campaign', end_date DATE COMMENT 'End date of the campaign', mailing_list ARRAY<BIGINT> COMMENT 'List of contact_id that are targets of the campaign' ) COMMENT 'The table contains data related to marketing campaigns. It includes details such as campaign identifiers, names, descriptions, and the email subject lines used. Additionally, it tracks the cost of each campaign, the duration (start and end dates), and the mailing lists associated with them. This data can be used for analyzing campaign performance, budgeting, and understanding audience engagement.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.contacts ( contact_id BIGINT COMMENT 'Unique identifier for each contact', prospect_id BIGINT COMMENT 'Identifier for the associated prospect', department STRING COMMENT 'Department of the contact', job_title STRING COMMENT 'Job title of the contact', source STRING COMMENT 'Source from which the contact was obtained', device STRING COMMENT 'Device used by the contact', opted_out BOOLEAN COMMENT 'Flag indicating if the contact has opted out of communications' ) COMMENT 'The table contains information about contacts associated with prospects. It includes details such as the contacts department, job title, and the source of their information. This data can be used for managing relationships with prospects, analyzing communication preferences, and understanding the demographics of contacts. Additionally, the opted-out flag helps in compliance with marketing regulations.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events ( event_id STRING COMMENT 'Unique identifier for the event', campaign_id BIGINT COMMENT 'Identifier for the associated campaign', contact_id BIGINT COMMENT 'Identifier for the contact involved in the event', event_type STRING COMMENT 'Type of event (e.g., open, click)', event_date TIMESTAMP COMMENT 'Timestamp of when the event occurred', metadata MAP<STRING, STRING> COMMENT 'Additional metadata related to the event' ) COMMENT 'The table captures data related to marketing events associated with campaigns. It includes details such as the unique event identifier, the campaign and contact involved, the type of event (like opens or clicks), and the date of the event. This data can be used for analyzing campaign performance, understanding user engagement, and optimizing marketing strategies.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks ( campaign_id BIGINT COMMENT 'Identifier for the associated campaign', feedbacks STRING COMMENT 'Feedback provided by the contact', contact_id BIGINT COMMENT 'Identifier for the contact providing feedback' ) COMMENT 'The table contains feedback data related to marketing campaigns. It includes information about the campaign ID, the contact who provided the feedback, and the feedback itself. This data can be used to analyze customer sentiments regarding specific campaigns, track the effectiveness of marketing efforts, and identify areas for improvement based on customer input.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.issues ( campaign_id BIGINT COMMENT 'Identifier for the associated campaign', complaint_type ARRAY<STRING> COMMENT 'Types of complaints received (e.g., GDPR, CAN-SPAM Act)', contact_id BIGINT COMMENT 'Identifier for the contact submitting the issue' ) COMMENT 'The table contains data related to customer complaints associated with marketing campaigns. It includes information on the campaign ID, the types of complaints received (such as GDPR or CAN-SPAM Act), and the contact ID of the individual submitting the issue. This data can be used to analyze complaint trends, assess compliance with regulations, and improve campaign strategies.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects ( prospect_id BIGINT COMMENT 'Unique identifier for each prospect', name STRING COMMENT 'Name of the business prospect', annual_revenue DOUBLE COMMENT 'Annual revenue of the prospect', employees INT COMMENT 'Number of employees at the prospect', industry STRING COMMENT 'Industry sector of the prospect', country STRING COMMENT 'Country where the prospect is located', city STRING COMMENT 'City where the prospect is located', postcode STRING COMMENT 'Postal code of the prospect' ) COMMENT 'The table contains information about business prospects, including their unique identifiers, names, annual revenues, employee counts, and industry sectors. It also includes geographical details such as country, city, and postal code. This data can be used for market analysis, lead generation, and understanding the potential customer base.'"
      ],
      [
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.campaigns SELECT CampaignId AS campaign_id, CampaignName AS campaign_name, CampaignDescription AS campaign_description, SubjectLine AS subject_line, Template AS template, Cost AS cost, CAST(StartDate AS date) AS start_date, CAST(EndDate AS date) AS end_date, collect_list(DISTINCT MailingList) AS mailing_list FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_campaigns GROUP BY CampaignId, CampaignName, CampaignDescription, SubjectLine, Template, Cost, StartDate, EndDate",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.contacts SELECT ContactId AS contact_id, ProspectId AS prospect_id, Department AS department, JobTitle AS job_title, Source AS source, Device AS device, OptedOut AS opted_out FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_contacts",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.events SELECT EventId AS event_id, CampaignId AS campaign_id, ContactId AS contact_id, EventType AS event_type, EventDate AS event_date, Metadata AS metadata FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_events",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks SELECT CampaignId AS campaign_id, Feedbacks AS feedbacks, ContactId AS contact_id FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_feedbacks",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.issues SELECT CampaignId AS campaign_id, ComplaintType AS complaint_type, ContactId AS contact_id FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_issues",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.prospects SELECT ProspectId AS prospect_id, ProspectName AS name, AnnualRevenue AS annual_revenue, Employees AS employees, Industry AS industry, Country AS country, City AS city, Postcode AS postcode FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prospects"
      ]
      ,
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.metrics_daily_rolling AS SELECT CAST(event_date AS date) AS date, count(distinct case when event_type = 'click' then contact_id end) as unique_clicks, SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered, SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent, SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens, SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks, SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts, SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam FROM `{{CATALOG}}`.`{{SCHEMA}}`.events GROUP BY date"
      ],
      [
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns ALTER COLUMN campaign_id SET NOT NULL",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.contacts ALTER COLUMN contact_id SET NOT NULL",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events ALTER COLUMN event_id SET NOT NULL",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects ALTER COLUMN prospect_id SET NOT NULL"
      ],
      [
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.issues SET TAGS ('system.Certified')",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns SET TAGS ('system.Certified')",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events SET TAGS ('system.Certified')",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.contacts SET TAGS ('system.Certified')",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks SET TAGS ('system.Certified')",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects SET TAGS ('system.Certified')",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.metrics_daily_rolling SET TAGS ('system.Certified')"
      ],
      [
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns ADD CONSTRAINT campaigns_pk PRIMARY KEY(campaign_id)",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.contacts ADD CONSTRAINT contact_pk PRIMARY KEY(contact_id)",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events ADD CONSTRAINT event_pk PRIMARY KEY(event_id)",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects ADD CONSTRAINT prospect_pk PRIMARY KEY(prospect_id)"
      ],
      [
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.contacts ADD CONSTRAINT contact_prospect_fk FOREIGN KEY(prospect_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.prospects NOT ENFORCED RELY",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events ADD CONSTRAINT event_campaign_fK FOREIGN KEY(campaign_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.campaigns NOT ENFORCED RELY",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks ADD CONSTRAINT feedback_campaign_fk FOREIGN KEY(campaign_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.campaigns NOT ENFORCED RELY",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.issues ADD CONSTRAINT issue_campaign_fk FOREIGN KEY(campaign_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.campaigns NOT ENFORCED RELY"
      ],
      [
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.events ADD CONSTRAINT event_contact_fk FOREIGN KEY(contact_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.contacts NOT ENFORCED RELY",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks ADD CONSTRAINT feedback_contact_fk FOREIGN KEY(contact_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.contacts NOT ENFORCED RELY",
          "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.issues ADD CONSTRAINT issue_contact_fk FOREIGN KEY(contact_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.contacts NOT ENFORCED RELY"
      ],
      [
        "CREATE OR REPLACE FUNCTION `{{CATALOG}}`.`{{SCHEMA}}`.get_highest_ctr() RETURNS TABLE(campaign_id INT, campaign_name STRING, ctr DOUBLE) COMMENT 'Function that extracts the campaign with the highest click through rate ever' RETURN SELECT campaign_id, campaign_name, ctr FROM (SELECT e.campaign_id, c.campaign_name, try_divide(SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END), SUM(CASE WHEN e.event_type = 'delivered' THEN 1 ELSE 0 END)) AS ctr FROM `{{CATALOG}}`.`{{SCHEMA}}`.`events` e INNER JOIN `{{CATALOG}}`.`{{SCHEMA}}`.`campaigns` c ON e.campaign_id = c.campaign_id WHERE e.event_type IN ('delivered', 'click') GROUP BY e.campaign_id, c.campaign_name ORDER BY ctr DESC LIMIT 1)"
      ] 
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
     "instructions": 
       [
         "If a customer ask a forecast, leverage the sql fonction ai_forecast",
         "The mailing_list column in the campaigns table contains all the contact_ids of the contacts to whom the campaign was sent.",
         "When you do joins between tables consider the foreign keys references."
       ],
     "curated_questions": [
        "How has the total number of emails sent, delivered, and the unique clicks evolved over the last six months?",
        "Which industries have shown the highest engagement rates with marketing campaigns?",
        "Which subject lines for my campaigns led to the most number of opens?",
        "Which campaigns had the strongest click-through rates (CTR)?"
       ],
     "benchmarks": [
        {
            "question_text": "Which is the campaign with the highest click through rate?",
            "answer_text": "SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.`get_highest_ctr`()"
        },
        {
            "question_text": "Which campaign had the highest total number of clicks?",
            "answer_text": "SELECT e.`campaign_id`, c.`campaign_name`, COUNT(*) as total_clicks FROM `{{CATALOG}}`.`{{SCHEMA}}`.`events` e INNER JOIN `{{CATALOG}}`.`{{SCHEMA}}`.`campaigns` c ON e.`campaign_id` = c.`campaign_id` WHERE e.`event_type` = 'click' GROUP BY e.`campaign_id`, c.`campaign_name` ORDER BY total_clicks DESC LIMIT 1"
        },
        {
          "question_text":"What is the total number of opens for each campaign? Order by campaign id",
          "answer_text": "SELECT e.`campaign_id`, c.`campaign_name`, COUNT(*) as total_opens FROM `{{CATALOG}}`.`{{SCHEMA}}`.`events` e INNER JOIN `{{CATALOG}}`.`{{SCHEMA}}`.`campaigns` c ON e.`campaign_id` = c.`campaign_id` WHERE e.`event_type` = 'html_open' GROUP BY e.`campaign_id`, c.`campaign_name` ORDER BY e.`campaign_id`"
        },
        {
          "question_text":"Which campaign had the max total number of opens? Give me the top 1",
          "answer_text": "SELECT e.`campaign_id`, c.`campaign_name`, COUNT(*) as total_opens FROM `{{CATALOG}}`.`{{SCHEMA}}`.`events` e INNER JOIN `{{CATALOG}}`.`{{SCHEMA}}`.`campaigns` c ON e.`campaign_id` = c.`campaign_id` WHERE e.`event_type` = 'html_open' GROUP BY e.`campaign_id`, c.`campaign_name` ORDER BY total_opens DESC LIMIT 1"
        },
        {
          "question_text":"What is the total number of clicks for each campaign? Order by campaign id",
          "answer_text": "SELECT e.`campaign_id`, c.`campaign_name`, COUNT(*) as total_clicks FROM `{{CATALOG}}`.`{{SCHEMA}}`.`events` e INNER JOIN `{{CATALOG}}`.`{{SCHEMA}}`.`campaigns` c ON e.`campaign_id` = c.`campaign_id` WHERE e.`event_type` = 'click' GROUP BY e.`campaign_id`, c.`campaign_name` ORDER BY e.`campaign_id`"
        }
    ]
    }
  ]
}
