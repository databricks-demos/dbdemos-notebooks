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
  "dashboards": [{"name": "[dbdemos] AIBI - Marketing Campaign",       "id": "web-marketing",        "genie_room_id": "marketing-campaign"}
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
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks ( campaign_id BIGINT COMMENT 'Identifier for the associated campaign', feedbacks STRING COMMENT 'Feedback provided by the contact', contact_id BIGINT COMMENT 'Identifier for the contact providing feedback', sentiment STRING COMMENT 'The sentiment of the feedback' ) COMMENT 'The table contains feedback data related to marketing campaigns. It includes information about the campaign ID, the contact who provided the feedback, and the feedback itself. This data can be used to analyze customer sentiments regarding specific campaigns, track the effectiveness of marketing efforts, and identify areas for improvement based on customer input.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.issues ( campaign_id BIGINT COMMENT 'Identifier for the associated campaign', complaint_type ARRAY<STRING> COMMENT 'Types of complaints received (e.g., GDPR, CAN-SPAM Act)', contact_id BIGINT COMMENT 'Identifier for the contact submitting the issue' ) COMMENT 'The table contains data related to customer complaints associated with marketing campaigns. It includes information on the campaign ID, the types of complaints received (such as GDPR or CAN-SPAM Act), and the contact ID of the individual submitting the issue. This data can be used to analyze complaint trends, assess compliance with regulations, and improve campaign strategies.'",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects ( prospect_id BIGINT COMMENT 'Unique identifier for each prospect', name STRING COMMENT 'Name of the business prospect', annual_revenue DOUBLE COMMENT 'Annual revenue of the prospect', employees INT COMMENT 'Number of employees at the prospect', industry STRING COMMENT 'Industry sector of the prospect', country STRING COMMENT 'Country where the prospect is located', city STRING COMMENT 'City where the prospect is located', postcode STRING COMMENT 'Postal code of the prospect' ) COMMENT 'The table contains information about business prospects, including their unique identifiers, names, annual revenues, employee counts, and industry sectors. It also includes geographical details such as country, city, and postal code. This data can be used for market analysis, lead generation, and understanding the potential customer base.'"
      ],
      [
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.campaigns SELECT CampaignId AS campaign_id, CampaignName AS campaign_name, CampaignDescription AS campaign_description, SubjectLine AS subject_line, Template AS template, Cost AS cost, CAST(StartDate AS date) AS start_date, CAST(EndDate AS date) AS end_date, collect_list(DISTINCT MailingList) AS mailing_list FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_campaigns GROUP BY CampaignId, CampaignName, CampaignDescription, SubjectLine, Template, Cost, StartDate, EndDate",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.contacts SELECT ContactId AS contact_id, ProspectId AS prospect_id, Department AS department, JobTitle AS job_title, Source AS source, Device AS device, OptedOut AS opted_out FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_contacts",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.events SELECT EventId AS event_id, CampaignId AS campaign_id, ContactId AS contact_id, EventType AS event_type, EventDate AS event_date, Metadata AS metadata FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_events",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.feedbacks SELECT CampaignId AS campaign_id, Feedbacks AS feedbacks, ContactId AS contact_id, ai_analyze_sentiment(Feedbacks) AS sentiment FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_feedbacks",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.issues SELECT CampaignId AS campaign_id, ComplaintType AS complaint_type, ContactId AS contact_id FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_issues",
        "INSERT INTO `{{CATALOG}}`.`{{SCHEMA}}`.prospects SELECT ProspectId AS prospect_id, ProspectName AS name, AnnualRevenue AS annual_revenue, Employees AS employees, Industry AS industry, Country AS country, City AS city, Postcode AS postcode FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prospects"
      ]
      ,
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
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prospects SET TAGS ('system.Certified')"
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
          """
          CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_events
          WITH METRICS
          LANGUAGE YAML
          AS $$
          version: 1.1

          source: {{CATALOG}}.{{SCHEMA}}.events

          joins:
            - name: campaigns
              source: {{CATALOG}}.{{SCHEMA}}.campaigns
              using:
                - campaign_id
            - name: contacts
              source: {{CATALOG}}.{{SCHEMA}}.contacts
              using:
                - contact_id
              joins:
                - name: prospects
                  source: {{CATALOG}}.{{SCHEMA}}.prospects
                  "on": contacts.prospect_id = prospects.prospect_id

          filter: campaigns.start_date >= DATE('2024-01-01') AND campaigns.start_date <= CURRENT_DATE

          dimensions:
            - name: event_date
              expr: "TO_DATE(source.event_date, 'yyyy-MM-dd HH:mm:ss')"
              comment: Date and time when the event occurred.
              display_name: Event Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - date of event
                - event timestamp
            - name: event_type
              expr: source.event_type
              comment: "Type of event (e.g., html_open, click, sent, delivered, spam, optout_click)."
              display_name: Event Type
              synonyms:
                - type of event
                - event category
            - name: campaign_id
              expr: campaigns.campaign_id
              comment: Unique Identifier of the campaign associated with the event.
              display_name: Campaign ID
            - name: campaign_name
              expr: campaigns.campaign_name
              comment: Name of the campaign associated with the event.
              display_name: Campaign Name
              synonyms:
                - name of campaign
                - marketing campaign name
            - name: campaign_description
              expr: campaigns.campaign_description
              comment: Description of the campaign.
              display_name: Campaign Description
              synonyms:
                - description of campaign
                - campaign details
            - name: cost
              expr: campaigns.cost
              comment: Total cost of the campaign.
              display_name: Campaign Cost
              format:
                type: currency
                currency_code: USD
                decimal_places:
                  type: exact
                  places: 2
                abbreviation: compact
              synonyms:
                - cost of campaign
                - marketing spend
            - name: campaign_template
              expr: campaigns.template
              comment: Email template used for the campaign.
              display_name: Campaign Template
              synonyms:
                - email template
                - template used
            - name: contact_id
              expr: contacts.contact_id
              comment: Unique identifier for the contact.
              display_name: Contact ID
              synonyms:
                - contact identifier
                - contact id
            - name: prospect_nr_of_employees
              expr: contacts.prospects.employees
              comment: Number of employees working for the prospect.
              display_name: Prospect Number of Employees
              format:
                type: number
                abbreviation: compact
              synonyms:
                - number of employees
                - prospect employees
            - name: prospect_country
              expr: contacts.prospects.country
              comment: Country where the prospect is located.
              display_name: Prospect Country
              synonyms:
                - country of prospect
                - prospect location country
            - name: prospect_city
              expr: contacts.prospects.city
              comment: City where the prospect is located.
              display_name: Prospect City
              synonyms:
                - city of prospect
                - prospect location city
            - name: prospect_industry
              expr: contacts.prospects.industry
              comment: Industry sector the prospect operates in.
              display_name: Prospect Industry
              synonyms:
                - industry of prospect
                - prospect sector
            - name: contact_department
              expr: contacts.department
              comment: The department where the contact is employed.
              display_name: Contact Department
              synonyms:
                - department of contact
                - contact's department
            - name: contact_source
              expr: contacts.source
              comment: The origin source of the contact information.
              display_name: Contact Source
              synonyms:
                - source of contact
                - contact origin
            - name: contact_device
              expr: contacts.device
              comment: The primary device type used by the contact for communication.
              display_name: Contact Device
              synonyms:
                - device of contact
                - contact's device type
            - name: start_date
              expr: campaigns.start_date
              comment: Start date of the campaign.
              display_name: Campaign Start Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - start date
                - campaign start
            - name: end_date
              expr: campaigns.end_date
              comment: End date of the campaign.
              display_name: Campaign End Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - end date
                - campaign end

          measures:
            - name: cost_metric
              expr: FIRST(campaigns.cost)
              comment: First recorded cost value for the campaign.
              display_name: First Campaign Cost
              format:
                type: currency
                currency_code: USD
                decimal_places:
                  type: exact
                  places: 2
                abbreviation: compact
              synonyms:
                - first cost
                - initial campaign cost
            - name: prospect_employees
              expr: sum(contacts.prospects.employees)
              comment: Total number of employees across all prospects.
              display_name: Total Prospect Employees
              format:
                type: number
                abbreviation: compact
              synonyms:
                - sum of employees
                - total employees
            - name: total_sent
              expr: SUM(CASE WHEN source.event_type = 'sent' THEN 1 ELSE 0 END)
              comment: Total number of 'sent' events.
              display_name: Total Sent
              synonyms:
                - sent count
                - number sent
            - name: total_delivered
              expr: SUM(CASE WHEN source.event_type = 'delivered' THEN 1 ELSE 0 END)
              comment: Total number of 'delivered' events.
              display_name: Total Delivered
              synonyms:
                - delivered count
                - number delivered
            - name: total_spam
              expr: SUM(CASE WHEN source.event_type = 'spam' THEN 1 ELSE 0 END)
              comment: Total number of 'spam' events.
              display_name: Total Spam
              synonyms:
                - spam count
                - number spam
            - name: total_opens
              expr: SUM(CASE WHEN source.event_type = 'html_open' THEN 1 ELSE 0 END)
              comment: Total number of 'html_open' events.
              display_name: Total Opens
              synonyms:
                - opens count
                - number opened
            - name: total_optouts
              expr: SUM(CASE WHEN source.event_type = 'optout_click' THEN 1 ELSE 0 END)
              comment: Total number of 'optout_click' events.
              display_name: Total Optouts
              synonyms:
                - optout count
                - number optouts
            - name: total_clicks
              expr: SUM(CASE WHEN source.event_type = 'click' THEN 1 ELSE 0 END)
              comment: Total number of 'click' events.
              display_name: Total Clicks
              synonyms:
                - clicks count
                - number clicked
            - name: unique_clicks
              expr: count(distinct case when source.event_type = 'click' then source.contact_id
                end)
              comment: Number of unique contacts who clicked.
              display_name: Unique Clicks
              synonyms:
                - distinct clicks
                - unique contacts clicked
            - name: nr_events
              expr: count(distinct source.event_id)
              comment: Number of distinct events.
              display_name: Number of Events
              synonyms:
                - event count
                - distinct events
            - name: ctr
              expr: unique_clicks / total_delivered
              comment: "Click-through rate: unique clicks divided by total delivered."
              display_name: Click-Through Rate
              format:
                type: percentage
                decimal_places:
                  type: exact
                  places: 2
              synonyms:
                - CTR
                - click rate
            - name: ctr_t7d
              expr: MEASURE(ctr)
              window:
                - order: event_date
                  semiadditive: last
                  range: trailing 7 day
              comment: Trailing 7 day click-through rate (CTR) using the event date.
              display_name: Trailing 7 Day CTR
              format:
                type: percentage
                decimal_places:
                  type: exact
                  places: 2
              synonyms:
                - trailing 7 day CTR
                - moving CTR
                - rolling CTR
            - name: delivery_rate
              expr: total_delivered / total_sent
              comment: "Delivery rate: total delivered divided by total sent."
              display_name: Delivery Rate
              format:
                type: percentage
                decimal_places:
                  type: exact
                  places: 2
              synonyms:
                - delivered rate
                - rate of delivery
            - name: optouts_rate
              expr: total_optouts / total_delivered
              comment: "Optouts rate: total optouts divided by total delivered."
              display_name: Optouts Rate
              format:
                type: percentage
                decimal_places:
                  type: exact
                  places: 2
              synonyms:
                - optout rate
                - rate of optouts
            - name: spam_rate
              expr: total_spam / total_delivered
              comment: "Spam rate: total spam divided by total delivered."
              display_name: Spam Rate
              format:
                type: percentage
                decimal_places:
                  type: exact
                  places: 2
              synonyms:
                - spam rate
                - rate of spam
            - name: opens_rate
              expr: total_opens / total_sent
              comment: "Opens rate: total opens divided by total sent."
              display_name: Opens Rate
              format:
                type: percentage
                decimal_places:
                  type: exact
                  places: 2
              synonyms:
                - open rate
                - rate of opens
            $$
          """,
          """
          CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_feedback
          WITH METRICS
          LANGUAGE YAML
          AS $$
          version: 1.1

          source: {{CATALOG}}.{{SCHEMA}}.feedbacks

          joins:
            - name: campaigns
              source: {{CATALOG}}.{{SCHEMA}}.campaigns
              using:
                - campaign_id
            - name: contacts
              source: {{CATALOG}}.{{SCHEMA}}.contacts
              using:
                - contact_id
              joins:
                - name: prospects
                  source: {{CATALOG}}.{{SCHEMA}}.prospects
                  "on": contacts.prospect_id = prospects.prospect_id

          filter: campaigns.start_date >= DATE('2024-01-01') AND campaigns.start_date <= CURRENT_DATE

          dimensions:
            - name: feedback
              expr: source.feedbacks
              comment: Feedback provided by the contact
              display_name: Feedback
              synonyms:
                - feedback
                - contact feedback
                - response
            - name: sentiment
              expr: source.sentiment
              comment: The sentiment of the feedback
              display_name: Sentiment
              synonyms:
                - sentiment
                - feedback sentiment
                - response sentiment
            - name: campaign_name
              expr: campaigns.campaign_name
              comment: Name of the campaign
              display_name: Campaign Name
              synonyms:
                - campaign name
                - name of campaign
            - name: campaign_description
              expr: campaigns.campaign_description
              comment: Description of the campaign
              display_name: Campaign Description
              synonyms:
                - campaign description
                - description of campaign
            - name: cost
              expr: campaigns.cost
              comment: Total cost of the campaign
              display_name: Campaign Cost
              format:
                type: currency
                currency_code: USD
                decimal_places:
                  type: exact
                  places: 2
                abbreviation: compact
              synonyms:
                - cost
                - campaign cost
                - total cost
            - name: campaign_template
              expr: campaigns.template
              comment: Email template used for the campaign
              display_name: Campaign Template
              synonyms:
                - template
                - email template
                - campaign template
            - name: prospect_nr_of_employees
              expr: contacts.prospects.employees
              comment: Number of employees working for the prospect
              display_name: Prospect Number of Employees
              format:
                type: number
                decimal_places:
                  type: exact
                  places: 0
                abbreviation: none
              synonyms:
                - number of employees
                - prospect employees
                - employee count
            - name: prospect_country
              expr: contacts.prospects.country
              comment: Country where the prospect is located
              display_name: Prospect Country
              synonyms:
                - country
                - prospect country
                - location country
            - name: prospect_city
              expr: contacts.prospects.city
              comment: City where the prospect is located
              display_name: Prospect City
              synonyms:
                - city
                - prospect city
                - location city
            - name: prospect_industry
              expr: contacts.prospects.industry
              comment: Industry sector the prospect operates in
              display_name: Prospect Industry
              synonyms:
                - industry
                - prospect industry
                - sector
            - name: contact_department
              expr: contacts.department
              comment: The department where the contact is employed
              display_name: Contact Department
              synonyms:
                - department
                - contact department
                - employee department
            - name: contact_source
              expr: contacts.source
              comment: The origin source of the contact information
              display_name: Contact Source
              synonyms:
                - source
                - contact source
                - origin source
            - name: contact_device
              expr: contacts.device
              comment: The primary device type used by the contact for communication
              display_name: Contact Device
              synonyms:
                - device
                - contact device
                - communication device
            - name: start_date
              expr: campaigns.start_date
              comment: Start date of the campaign
              display_name: Campaign Start Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - start date
                - campaign start
                - begin date
            - name: end_date
              expr: campaigns.end_date
              comment: End date of the campaign
              display_name: Campaign End Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - end date
                - campaign end
                - finish date

          measures:
            - name: count_feedbacks
              expr: count(source.feedbacks)
              comment: Total number of feedbacks
              display_name: Count of Feedbacks
              synonyms:
                - feedback count
                - number of feedbacks
                - total feedbacks
            - name: count_negative_feedbacks
              expr: count(source.feedbacks) filter(where source.sentiment = 'negative')
              comment: Number of feedbacks with negative sentiment
              display_name: Count of Negative Feedbacks
              synonyms:
                - negative feedback count
                - number of negative feedbacks
            - name: count_mixed_feedbacks
              expr: count(source.feedbacks) filter(where source.sentiment = 'mixed')
              comment: Number of feedbacks with mixed sentiment
              display_name: Count of Mixed Feedbacks
              synonyms:
                - mixed feedback count
                - number of mixed feedbacks
            - name: count_positive_feedbacks
              expr: count(source.feedbacks) filter(where source.sentiment = 'positive')
              comment: Number of feedbacks with positive sentiment
              display_name: Count of Positive Feedbacks
              synonyms:
                - positive feedback count
                - number of positive feedbacks
            $$
          """,
          """
          CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_issues
          WITH METRICS
          LANGUAGE YAML
          AS $$
          version: 1.1

          source: {{CATALOG}}.{{SCHEMA}}.issues

          joins:
            - name: campaigns
              source: {{CATALOG}}.{{SCHEMA}}.campaigns
              using:
                - campaign_id
            - name: contacts
              source: {{CATALOG}}.{{SCHEMA}}.contacts
              using:
                - contact_id
              joins:
                - name: prospects
                  source: {{CATALOG}}.{{SCHEMA}}.prospects
                  "on": contacts.prospect_id = prospects.prospect_id

          dimensions:
            - name: complaint_type
              expr: "source.complaint_type[0]"
              comment: "Type of complaint (e.g., GDPR, CAN-SPAM Act, etc.)"
              display_name: Complaint Type
              synonyms:
                - complaint
                - issue type
                - regulatory complaint
            - name: campaign_name
              expr: campaigns.campaign_name
              comment: Name of the marketing campaign
              display_name: Campaign Name
              synonyms:
                - name of campaign
                - marketing campaign name
            - name: campaign_description
              expr: campaigns.campaign_description
              comment: Description of the campaign
              display_name: Campaign Description
              synonyms:
                - description
                - campaign details
            - name: cost
              expr: campaigns.cost
              comment: Total cost of the campaign in USD
              display_name: Campaign Cost
              format:
                type: currency
                currency_code: USD
                decimal_places:
                  type: exact
                  places: 2
                abbreviation: compact
              synonyms:
                - cost
                - total cost
                - campaign expense
            - name: campaign_template
              expr: campaigns.template
              comment: Email template used for the campaign
              display_name: Campaign Template
              synonyms:
                - template
                - email template
            - name: prospect_nr_of_employees
              expr: contacts.prospects.employees
              comment: Number of employees working for the prospect
              display_name: Prospect Number of Employees
              format:
                type: number
                decimal_places:
                  type: exact
                  places: 0
                abbreviation: none
              synonyms:
                - number of employees
                - prospect employees
                - employee count
            - name: prospect_country
              expr: contacts.prospects.country
              comment: Country where the prospect is located
              display_name: Prospect Country
              synonyms:
                - country
                - prospect location country
            - name: prospect_city
              expr: contacts.prospects.city
              comment: City where the prospect is located
              display_name: Prospect City
              synonyms:
                - city
                - prospect location city
            - name: prospect_industry
              expr: contacts.prospects.industry
              comment: Industry sector the prospect operates in
              display_name: Prospect Industry
              synonyms:
                - industry
                - prospect sector
            - name: contact_department
              expr: contacts.department
              comment: Department where the contact is employed
              display_name: Contact Department
              synonyms:
                - department
                - contact's department
            - name: contact_source
              expr: contacts.source
              comment: Origin source of the contact information
              display_name: Contact Source
              synonyms:
                - source
                - contact origin
            - name: contact_device
              expr: contacts.device
              comment: Primary device type used by the contact for communication
              display_name: Contact Device
              synonyms:
                - device
                - contact's device
                - communication device
            - name: start_date
              expr: campaigns.start_date
              comment: Start date of the campaign
              display_name: Campaign Start Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - start date
                - campaign start
            - name: end_date
              expr: campaigns.end_date
              comment: End date of the campaign
              display_name: Campaign End Date
              format:
                type: date
                date_format: year_month_day
                leading_zeros: false
              synonyms:
                - end date
                - campaign end

          measures:
            - name: count_issues
              expr: count(*)
              comment: Total number of issues reported
              display_name: Number of Issues
              format:
                type: number
                decimal_places:
                  type: exact
                  places: 0
                abbreviation: none
              synonyms:
                - issue count
                - total issues
                - complaint count
            - name: nr_impacted_campaigns
              expr: count(distinct source.campaign_id)
              comment: Number of distinct campaigns impacted by issues
              display_name: Number of Impacted Campaigns
              format:
                type: number
                decimal_places:
                  type: exact
                  places: 0
                abbreviation: none
              synonyms:
                - impacted campaigns
                - distinct campaigns
                - affected campaigns
            $$
          """,
          """
          CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_daily_rolling
          WITH METRICS
          LANGUAGE YAML
          AS $$
          version: 1.1

          source: |-
            SELECT
              CAST(event_date AS DATE) AS date,
              contact_id,
              event_type
              FROM {{CATALOG}}.{{SCHEMA}}.events
          comment: "Daily and 7-day trailing (t7d_) email engagement metrics, sliced by event_type."

          dimensions:
            - name: Date
              expr: date
            - name: Event Type
              expr: event_type

          measures:
            - name: unique_clicks
              expr: COUNT(DISTINCT CASE WHEN event_type = 'click' THEN contact_id END)
            - name: total_delivered
              expr: SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END)
            - name: total_sent
              expr: SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END)
            - name: total_opens
              expr: SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END)
            - name: total_clicks
              expr: SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)
            - name: total_optouts
              expr: SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END)
            - name: total_spam
              expr: SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END)
            - name: t7d_unique_clicks
              expr: COUNT(DISTINCT CASE WHEN event_type = 'click' THEN contact_id END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
            - name: t7d_total_delivered
              expr: SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
            - name: t7d_total_sent
              expr: SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
            - name: t7d_total_opens
              expr: SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
            - name: t7d_total_clicks
              expr: SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
            - name: t7d_total_optouts
              expr: SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
            - name: t7d_total_spam
              expr: SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END)
              window:
                - order: Date
                  semiadditive: last
                  range: trailing 7 day
          $$
          """
      ],
      [
        "CREATE OR REPLACE FUNCTION `{{CATALOG}}`.`{{SCHEMA}}`.get_highest_ctr() RETURNS TABLE(campaign_name STRING, ctr DOUBLE) COMMENT 'Returns the campaign with the highest click-through rate using metric views' RETURN SELECT campaign_name, MEASURE(ctr) AS ctr FROM `{{CATALOG}}`.`{{SCHEMA}}`.metrics_events GROUP BY campaign_name ORDER BY ctr DESC LIMIT 1"
      ] 
  ],
  "genie_rooms":[
    {
     "id": "marketing-campaign",
     "display_name": "DBDemos - AI/BI - Marketing Campaign",     
     "description": "Analyze your Marketing Campaign effectiveness leveraging AI/BI Dashboard. Deep dive into your data and metrics.",
     "table_identifiers": ["{{CATALOG}}.{{SCHEMA}}.metrics_events",
                           "{{CATALOG}}.{{SCHEMA}}.metrics_feedback",
                           "{{CATALOG}}.{{SCHEMA}}.metrics_issues"],
     "sql_instructions": [
        {
            "title": "Compute rolling metrics",
            "content": "SELECT\n    event_date,\n    MEASURE(unique_clicks) AS daily_unique_clicks,\n    MEASURE(ctr_t7d) AS t7d_unique_clicks\nFROM {{CATALOG}}.{{SCHEMA}}.metrics_events\nGROUP BY event_date\nORDER BY event_date"
        },
        {
            "title": "What are the campaigns with the highest click-through rates?",
            "content": "SELECT\n    campaign_id,\n    campaign_name,\n    cost,\n    start_date,\n    end_date,\n    MEASURE(total_sent) AS total_sent,\n    MEASURE(total_delivered) AS total_delivered,\n    MEASURE(total_clicks) AS total_clicks,\n    MEASURE(unique_clicks) AS unique_clicks,\n    MEASURE(ctr) AS ctr\nFROM {{CATALOG}}.{{SCHEMA}}.metrics_events\nWHERE start_date >= :start_date AND end_date <= :end_date\nGROUP BY ALL\nORDER BY ctr DESC, cost ASC\nLIMIT 20"
        }
    ],
     "instructions": "If a customer ask a forecast, leverage the sql fonction ai_forecast.\nThe mailing_list column in the campaigns table contains all the contact_ids of the contacts to whom the campaign was sent.\nUse the metric views as the primary semantic layer. Metrics already encapsulate joins and business logic, so avoid joining raw tables unless explicitly required.",
      
      "function_names": [
        "{{CATALOG}}.{{SCHEMA}}.get_highest_ctr"
      ],
     "curated_questions": [
        "How has the total number of emails sent, delivered, and the unique clicks evolved over the last six months?",
        "Which industries have shown the highest engagement rates with marketing campaigns?",
        "Which campaigns achieved the highest open rates?",
        "Which campaigns had the strongest click-through rates (CTR)?"
       ],
     "benchmarks": [
      {
        "question_text": "Which is the campaign with the highest click through rate?",
        "answer_text": "SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.`get_highest_ctr`()"
      },
      {
        "question_text": "Which campaign had the highest total number of clicks?",
        "answer_text": "SELECT campaign_id, campaign_name, MEASURE(total_clicks) AS total_clicks FROM {{CATALOG}}.{{SCHEMA}}.metrics_events GROUP BY campaign_id, campaign_name ORDER BY total_clicks DESC LIMIT 1"
      },
      {
        "question_text": "What is the total number of opens for each campaign?",
        "answer_text": "SELECT campaign_id,campaign_name, MEASURE(total_opens) AS total_opens FROM {{CATALOG}}.{{SCHEMA}}.metrics_events GROUP BY campaign_id, campaign_name ORDER BY total_opens DESC"
      },
      {
        "question_text": "Which campaign had the max total number of opens? Give me the top 1",
        "answer_text": "SELECT campaign_id, campaign_name, MEASURE(total_opens) AS total_opens FROM {{CATALOG}}.{{SCHEMA}}.metrics_events GROUP BY campaign_id, campaign_name ORDER BY total_opens DESC LIMIT 1"
      },
      {
        "question_text": "What is the total number of clicks for each campaign? Order by campaign id",
        "answer_text": "SELECT campaign_id, campaign_name, MEASURE(total_clicks) AS total_clicks FROM {{CATALOG}}.{{SCHEMA}}.metrics_events GROUP BY campaign_id, campaign_name ORDER BY campaign_id"
      }
    ]
    }
  ]
}
