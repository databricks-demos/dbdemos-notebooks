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
  "description": "Track multi-channel marketing performance with AI/BI Dashboards (TikTok, Instagram, Google Ads, Email). Spot a revenue drop, then use Genie to ask why in natural language and trace it to the failing campaign, creative and markets.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Marketing-campaign",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "AI BI: Campaign effectiveness",
      "description": "Discover Databricks Platform capabilities."
    }
  ],
  "init_job": {},
  "serverless_supported": True,
  "cluster": {},
  "pipelines": [],
  "dashboards": [{"name": "[dbdemos] AIBI - Marketing Campaign",       "id": "web-marketing",        "genie_room_id": "marketing-campaign"}
                ],
  "data_folders": [
    {"source_folder": "aibi/dbdemos_aibi_cme_marketing_campaign_v2/channels",             "source_format": "parquet", "target_volume_folder": "channels",             "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_cme_marketing_campaign_v2/campaigns",            "source_format": "parquet", "target_volume_folder": "campaigns",            "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_cme_marketing_campaign_v2/audiences",            "source_format": "parquet", "target_volume_folder": "audiences",            "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_cme_marketing_campaign_v2/regions",              "source_format": "parquet", "target_volume_folder": "regions",              "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_cme_marketing_campaign_v2/creatives",            "source_format": "parquet", "target_volume_folder": "creatives",            "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_cme_marketing_campaign_v2/campaign_performance", "source_format": "parquet", "target_volume_folder": "campaign_performance", "target_format": "parquet"}
  ],
  "sql_queries": [
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.channels_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw marketing channels' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/channels', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw marketing campaigns' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/campaigns', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.audiences_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw target audiences' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/audiences', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.regions_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw geographic markets' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/regions', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.creatives_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw ad creatives (root-cause table)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/creatives', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaign_performance_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw daily campaign performance' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/campaign_performance', format => 'parquet', pathGlobFilter => '*.parquet')"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.channels (
          channel_id BIGINT COMMENT 'Unique channel identifier.',
          channel_name STRING COMMENT 'Marketing channel (TikTok, Instagram, Google Ads, Email).',
          channel_type STRING COMMENT 'Channel type (Social, Search, Owned).',
          PRIMARY KEY (channel_id) RELY
      ) USING delta COMMENT 'Marketing channels campaigns run on.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns (
          campaign_id BIGINT COMMENT 'Unique campaign identifier.',
          campaign_name STRING COMMENT 'Campaign name.',
          objective STRING COMMENT 'Campaign objective (Awareness, Conversion, Lead Gen).',
          start_date DATE COMMENT 'Campaign start date.',
          end_date DATE COMMENT 'Campaign end date.',
          PRIMARY KEY (campaign_id) RELY
      ) USING delta COMMENT 'Marketing campaigns. The Q4 Growth Push campaign (starts 2025-09-01) hosts the underperforming creative.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.audiences (
          audience_id BIGINT COMMENT 'Unique audience identifier.',
          audience_name STRING COMMENT 'Audience segment name (Gen Z, Young Pros, Families, Established).',
          age_band STRING COMMENT 'Age band of the segment.',
          interest STRING COMMENT 'Primary interest of the segment.',
          PRIMARY KEY (audience_id) RELY
      ) USING delta COMMENT 'Target audience segments.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.regions (
          region_id BIGINT COMMENT 'Unique market identifier.',
          country STRING COMMENT 'Country name.',
          country_code STRING COMMENT 'ISO country code.',
          latitude DOUBLE COMMENT 'Country latitude (for maps).',
          longitude DOUBLE COMMENT 'Country longitude (for maps).',
          PRIMARY KEY (region_id) RELY
      ) USING delta COMMENT 'Geographic markets campaigns target.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.creatives (
          creative_id BIGINT COMMENT 'Unique creative identifier.',
          creative_name STRING COMMENT 'Ad creative name.',
          channel STRING COMMENT 'Channel name the creative runs on.',
          channel_id BIGINT COMMENT 'Channel the creative runs on.',
          message_theme STRING COMMENT 'Creative message theme (Brand Story, Aggressive Discount, ...).',
          format STRING COMMENT 'Creative format (Video, Carousel, Search Text, HTML Email, ...).',
          launch_date DATE COMMENT 'Date the creative launched.',
          target_market STRING COMMENT 'Markets the creative targets (Global, or a localized market).',
          status STRING COMMENT 'Creative status (active / underperforming).',
          PRIMARY KEY (creative_id) RELY
      ) USING delta COMMENT 'Ad creatives. This is the root-cause table: the localized Fall Sale - v2 (DE/FR) creative (launched 2025-09-01, status underperforming) is why revenue dropped.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaign_performance (
          perf_id BIGINT COMMENT 'Unique performance row identifier.',
          date DATE COMMENT 'Activity date.',
          campaign_id BIGINT COMMENT 'Campaign for this row.',
          channel_id BIGINT COMMENT 'Channel for this row.',
          platform STRING COMMENT 'Device platform (Mobile / Web).',
          audience_id BIGINT COMMENT 'Audience segment targeted.',
          region_id BIGINT COMMENT 'Market (country) for this row.',
          creative_id BIGINT COMMENT 'Ad creative served (FK to creatives).',
          impressions BIGINT COMMENT 'Impressions served.',
          clicks BIGINT COMMENT 'Clicks.',
          spend DOUBLE COMMENT 'Ad spend in USD.',
          conversions BIGINT COMMENT 'Conversions.',
          revenue DOUBLE COMMENT 'Revenue generated in USD.',
          PRIMARY KEY (perf_id) RELY,
          CONSTRAINT cp_campaign_fk FOREIGN KEY (campaign_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.campaigns(campaign_id),
          CONSTRAINT cp_channel_fk  FOREIGN KEY (channel_id)  REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.channels(channel_id),
          CONSTRAINT cp_audience_fk FOREIGN KEY (audience_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.audiences(audience_id),
          CONSTRAINT cp_region_fk   FOREIGN KEY (region_id)   REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.regions(region_id),
          CONSTRAINT cp_creative_fk FOREIGN KEY (creative_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.creatives(creative_id)
      ) USING delta COMMENT 'Daily campaign performance fact. Revenue & conversions collapse from 2025-09-01 on TikTok in Germany & France due to the underperforming creative, while spend stays flat.'"""
    ],
    [
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.channels SELECT CAST(channel_id AS BIGINT), channel_name, channel_type FROM `{{CATALOG}}`.`{{SCHEMA}}`.channels_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.campaigns SELECT CAST(campaign_id AS BIGINT), campaign_name, objective, CAST(start_date AS DATE), CAST(end_date AS DATE) FROM `{{CATALOG}}`.`{{SCHEMA}}`.campaigns_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.audiences SELECT CAST(audience_id AS BIGINT), audience_name, age_band, interest FROM `{{CATALOG}}`.`{{SCHEMA}}`.audiences_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.regions SELECT CAST(region_id AS BIGINT), country, country_code, latitude, longitude FROM `{{CATALOG}}`.`{{SCHEMA}}`.regions_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.creatives SELECT CAST(creative_id AS BIGINT), creative_name, channel, CAST(channel_id AS BIGINT), message_theme, format, CAST(launch_date AS DATE), target_market, status FROM `{{CATALOG}}`.`{{SCHEMA}}`.creatives_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.campaign_performance SELECT CAST(perf_id AS BIGINT), CAST(date AS DATE), CAST(campaign_id AS BIGINT), CAST(channel_id AS BIGINT), platform, CAST(audience_id AS BIGINT), CAST(region_id AS BIGINT), CAST(creative_id AS BIGINT), CAST(impressions AS BIGINT), CAST(clicks AS BIGINT), spend, CAST(conversions AS BIGINT), revenue FROM `{{CATALOG}}`.`{{SCHEMA}}`.campaign_performance_bronze"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.campaign_performance_enriched
         COMMENT 'Analysis-ready daily campaign performance joined with campaign, channel, audience, market and creative. Powers the AI/BI dashboard and Genie. Revenue & conversions drop from 2025-09-01 on TikTok in Germany & France because of the underperforming Fall Sale - v2 (DE/FR) creative; spend stays flat.'
         AS SELECT p.perf_id, p.date,
           c.channel_name, c.channel_type, p.platform,
           cp.campaign_name, cp.objective,
           a.audience_name, a.age_band, a.interest,
           r.country, r.country_code, r.latitude, r.longitude,
           cr.creative_name, cr.message_theme, cr.format AS creative_format, cr.launch_date AS creative_launch_date, cr.target_market, cr.status AS creative_status,
           p.impressions, p.clicks, p.spend, p.conversions, p.revenue
         FROM `{{CATALOG}}`.`{{SCHEMA}}`.campaign_performance p
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.channels   c  ON p.channel_id  = c.channel_id
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.campaigns  cp ON p.campaign_id = cp.campaign_id
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.audiences  a  ON p.audience_id = a.audience_id
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.regions    r  ON p.region_id   = r.region_id
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.creatives  cr ON p.creative_id = cr.creative_id"""
    ],
    [
      """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_campaign
         WITH METRICS LANGUAGE YAML AS $$
         version: 1.1
         source: {{CATALOG}}.{{SCHEMA}}.campaign_performance_enriched
         comment: "Governed multi-channel marketing KPIs. Revenue per Dollar = revenue / spend (a.k.a. ROAS). The root cause of the late-2025 drop lives in the creative_name / creative_status fields (the underperforming Fall Sale - v2 (DE/FR) creative)."
         dimensions:
           - name: Date
             expr: date
           - name: Channel
             expr: channel_name
           - name: Channel Type
             expr: channel_type
           - name: Platform
             expr: platform
           - name: Campaign
             expr: campaign_name
           - name: Objective
             expr: objective
           - name: Audience
             expr: audience_name
           - name: Age Band
             expr: age_band
           - name: Interest
             expr: interest
           - name: Country
             expr: country
           - name: Country Code
             expr: country_code
           - name: Latitude
             expr: latitude
           - name: Longitude
             expr: longitude
           - name: Creative
             expr: creative_name
           - name: Message Theme
             expr: message_theme
           - name: Creative Format
             expr: creative_format
           - name: Creative Status
             expr: creative_status
           - name: Target Market
             expr: target_market
         measures:
           - name: Revenue
             expr: SUM(revenue)
           - name: Total Spend
             expr: SUM(spend)
           - name: Conversions
             expr: SUM(conversions)
           - name: Impressions
             expr: SUM(impressions)
           - name: Clicks
             expr: SUM(clicks)
           - name: Revenue per Dollar
             expr: SUM(revenue) / NULLIF(SUM(spend),0)
           - name: Conversion Rate
             expr: SUM(conversions) / NULLIF(SUM(clicks),0)
           - name: CTR
             expr: SUM(clicks) / NULLIF(SUM(impressions),0)
           - name: Cost per Conversion
             expr: SUM(spend) / NULLIF(SUM(conversions),0)
         $$;"""
    ]
  ],
  "genie_rooms": [
    {
      "id": "marketing-campaign",
      "display_name": "DBDemos - AI/BI - Marketing Campaign effectiveness",
      "description": "Explore multi-channel marketing performance across TikTok, Instagram, Google Ads and Email. Ask why revenue and conversions dropped in late 2025 and Genie will trace it to the failing campaign (Q4 Growth Push), the underperforming creative (the localized Fall Sale - v2 (DE/FR) launched 2025-09-01) and the affected markets (Germany & France), while spend stayed flat.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.campaign_performance_enriched",
        "{{CATALOG}}.{{SCHEMA}}.metrics_campaign",
        "{{CATALOG}}.{{SCHEMA}}.campaigns",
        "{{CATALOG}}.{{SCHEMA}}.creatives"
      ],
      "sql_instructions": [
        {
          "title": "Monthly revenue, spend and conversions trend",
          "content": """SELECT DATE_TRUNC('MONTH', date) AS month,
       ROUND(SUM(revenue)) AS revenue,
       ROUND(SUM(spend)) AS spend,
       SUM(conversions) AS conversions,
       ROUND(SUM(revenue)/NULLIF(SUM(spend),0), 2) AS revenue_per_dollar
FROM {{CATALOG}}.{{SCHEMA}}.campaign_performance_enriched
GROUP BY 1 ORDER BY 1;"""
        },
        {
          "title": "Revenue per dollar by campaign since the drop (Sept 2025)",
          "content": """SELECT campaign_name,
       ROUND(SUM(revenue)/NULLIF(SUM(spend),0), 2) AS revenue_per_dollar,
       ROUND(SUM(spend)) AS spend
FROM {{CATALOG}}.{{SCHEMA}}.campaign_performance_enriched
WHERE date >= DATE'2025-09-01'
GROUP BY 1 ORDER BY revenue_per_dollar ASC;"""
        },
        {
          "title": "Creatives inside the failing campaign (Q4 Growth Push)",
          "content": """SELECT creative_name, message_theme, target_market, creative_status,
       ROUND(SUM(conversions)/NULLIF(SUM(clicks),0)*100, 2) AS conv_rate_pct,
       ROUND(SUM(revenue)/NULLIF(SUM(spend),0), 2) AS revenue_per_dollar
FROM {{CATALOG}}.{{SCHEMA}}.campaign_performance_enriched
WHERE campaign_name = 'Q4 Growth Push'
GROUP BY 1,2,3,4 ORDER BY revenue_per_dollar ASC;"""
        }
      ],
      "instructions": "This data tells the story of a multi-channel marketing team whose revenue and conversions dropped from September 2025 even though spend stayed flat. When asked WHY revenue, conversions or revenue-per-dollar dropped (or what changed in late 2025), trace it to: (1) the failing campaign Q4 Growth Push (lowest revenue_per_dollar since 2025-09-01); (2) inside it, the underperforming creative 'Fall Sale - v2 (DE/FR)' (creative_status = 'underperforming', launched 2025-09-01 on TikTok, a localized Germany & France batch) whose conversion rate is ~0.35% vs ~3% for healthy creatives; (3) the affected markets Germany & France (lowest revenue_per_dollar). Revenue per Dollar = revenue / spend (a.k.a. ROAS). If asked for a forecast, use the ai_forecast SQL function. metrics_campaign is a governed METRIC VIEW: query its measures with MEASURE(`Revenue`), MEASURE(`Total Spend`), MEASURE(`Conversions`), MEASURE(`Revenue per Dollar`), MEASURE(`Conversion Rate`) and group by its dimensions (e.g. `Channel`, `Campaign`, `Country`, `Creative`, `Platform`); prefer it for clean aggregated KPIs.",
      "curated_questions": [
        "Why did our marketing revenue and conversions drop in late 2025?",
        "Which campaign is underperforming since September 2025?",
        "Inside the Q4 Growth Push campaign, which creative is dragging performance down?",
        "Which markets have the lowest revenue per dollar?",
        "Show monthly revenue, spend and conversions over the last year"
      ]
    }
  ]
}
