# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "aibi-customer-support",
    "category": "AI-BI",
    "title": "AI/BI: Making Customer Support More Efficient with AI",
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_aibi_customer_support",
    "description": "Show how an AI Support Copilot makes customer support dramatically more efficient. Explore the impact with an AI/BI Dashboard, then ask Genie why resolution time dropped.",
    "bundle": True,
    "notebooks": [
      {
        "path": "AI-BI-Customer-support",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "AI BI: Customer Support Review",
        "description": "Discover Databricks Platform capabilities."
      }
    ],
    "init_job": {},
    "serverless_supported": True,
    "cluster": {},
    "pipelines": [],
    "dashboards": [
      {
        "name": "[dbdemos] AIBI - Customer Support: AI Efficiency",
        "id": "customer-support",
        "genie_room_id": "customer-support"
      }
    ],
    "data_folders": [
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/regions",               "source_format": "parquet", "target_volume_folder": "regions",               "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/cities",                "source_format": "parquet", "target_volume_folder": "cities",                "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/products",              "source_format": "parquet", "target_volume_folder": "products",              "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/customers",             "source_format": "parquet", "target_volume_folder": "customers",             "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/calendar",              "source_format": "parquet", "target_volume_folder": "calendar",              "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/support_cases",         "source_format": "parquet", "target_volume_folder": "support_cases",         "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/ai_assistant_releases", "source_format": "parquet", "target_volume_folder": "ai_assistant_releases", "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_customer_support_v2/ai_assistant_usage",    "source_format": "parquet", "target_volume_folder": "ai_assistant_usage",    "target_format": "parquet"}
    ],
    "sql_queries": [
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.regions_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw geographic regions' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/regions', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cities_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw destination cities' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/cities', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.products_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw travel products' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/products', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.customers_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw customers (travel agencies)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/customers', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.calendar_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw calendar with holidays' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/calendar', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.support_cases_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: raw support cases' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/support_cases', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_releases_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: AI Support Copilot release log' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/ai_assistant_releases', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_usage_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: daily AI Support Copilot usage' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/ai_assistant_usage', format => 'parquet', pathGlobFilter => '*.parquet')"""
      ],
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.regions (
            region_id BIGINT COMMENT 'Unique region identifier.',
            region_name STRING COMMENT 'Region name (NA, EMEA, APAC, LATAM).',
            primary_timezone STRING COMMENT 'Primary timezone of the region.',
            PRIMARY KEY (region_id) RELY
        ) USING delta COMMENT 'Geographic regions served by the travel support organization.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cities (
            city_id BIGINT COMMENT 'Unique city identifier.',
            city_name STRING COMMENT 'Destination city name.',
            country_name STRING COMMENT 'Country of the destination city.',
            region_id BIGINT COMMENT 'Region this city belongs to.',
            latitude DOUBLE COMMENT 'Latitude of the destination (for maps).',
            longitude DOUBLE COMMENT 'Longitude of the destination (for maps).',
            PRIMARY KEY (city_id) RELY
        ) USING delta COMMENT 'Destination cities customers travel to. Joined to support_cases via destination_city_id.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.products (
            product_id BIGINT COMMENT 'Unique product identifier.',
            product_name STRING COMMENT 'Travel bundle / product name.',
            package_tier STRING COMMENT 'Package tier (Standard / Plus / Premium).',
            market_focus STRING COMMENT 'Primary market focus for the product.',
            PRIMARY KEY (product_id) RELY
        ) USING delta COMMENT 'Travel bundles / products sold by the company.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.customers (
            customer_id BIGINT COMMENT 'Unique customer identifier.',
            customer_name STRING COMMENT 'Customer (travel agency) name.',
            customer_segment STRING COMMENT 'Segment (SMB / Mid-Market / Enterprise).',
            industry STRING COMMENT 'Customer industry.',
            contract_value_band STRING COMMENT 'Contract value band proxying account size.',
            region_id BIGINT COMMENT 'Customer home region.',
            PRIMARY KEY (customer_id) RELY
        ) USING delta COMMENT 'Customers (travel agencies / corporate-travel firms).'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.calendar (
            date DATE COMMENT 'Calendar date.',
            year INT COMMENT 'Year.', quarter INT COMMENT 'Quarter.', month INT COMMENT 'Month.', week INT COMMENT 'ISO week.',
            is_us_holiday BOOLEAN COMMENT 'Whether the date is a US holiday.',
            holiday_name STRING COMMENT 'Holiday name if applicable.',
            is_peak_season BOOLEAN COMMENT 'Whether the date falls in a peak travel season.',
            PRIMARY KEY (date) RELY
        ) USING delta COMMENT 'Date dimension with US holiday and peak-travel-season flags.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_releases (
            release_id BIGINT COMMENT 'Unique release identifier.',
            version STRING COMMENT 'Release version.',
            release_date DATE COMMENT 'Release date.',
            capability_summary STRING COMMENT 'Short summary of what the release does.',
            release_notes STRING COMMENT 'Full release notes describing the AI Support Copilot capabilities.',
            auto_resolved_categories STRING COMMENT 'Ticket categories the copilot auto-resolves at this release.',
            status STRING COMMENT 'Release status (Beta / GA).',
            PRIMARY KEY (release_id) RELY
        ) USING delta COMMENT 'Release log for the AI Support Copilot, built on Databricks with Agent Bricks. The v1.0 GA release (2025-06-02) auto-resolves How-To/Access/Billing and is the reason support resolution time dropped.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_usage (
            usage_date DATE COMMENT 'Day of AI assistant activity.',
            ai_queries BIGINT COMMENT 'Total AI assistant queries that day.',
            ai_deflections BIGINT COMMENT 'Cases fully auto-resolved by the AI that day.',
            assist_queries BIGINT COMMENT 'Copilot assist queries on human-handled cases.',
            ai_compute_cost DOUBLE COMMENT 'AI compute cost in USD for the day.',
            PRIMARY KEY (usage_date) RELY
        ) USING delta COMMENT 'Daily AI Support Copilot activity. ai_deflections jump at the GA release and track the resolution-time drop.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.support_cases (
            case_id BIGINT COMMENT 'Unique support case identifier.',
            opened_at TIMESTAMP COMMENT 'When the case was opened.',
            closed_at TIMESTAMP COMMENT 'When the case was closed.',
            opened_date DATE COMMENT 'Date the case was opened (for joins/trends).',
            category STRING COMMENT 'Case category (How-To, Access, Billing, Outage, Bug, Performance).',
            channel STRING COMMENT 'Channel (Email, Chat, In-App, Phone).',
            priority STRING COMMENT 'Priority (Low, Medium, High, Critical).',
            support_tier STRING COMMENT 'Support tier (Tier 1/2/3).',
            region_id BIGINT COMMENT 'Region the case belongs to.',
            customer_id BIGINT COMMENT 'Customer that opened the case.',
            product_id BIGINT COMMENT 'Product the case is about.',
            destination_city_id BIGINT COMMENT 'Travel destination city for the case.',
            ai_handled BOOLEAN COMMENT 'TRUE when auto-resolved by the AI Support Copilot.',
            resolution_hours DOUBLE COMMENT 'Hours taken to resolve the case.',
            satisfaction_score DOUBLE COMMENT 'Customer satisfaction (2-5, null if no survey).',
            reopened_flag BOOLEAN COMMENT 'Whether the case was reopened.',
            support_cost DOUBLE COMMENT 'Cost of handling the case in USD.',
            PRIMARY KEY (case_id) RELY,
            CONSTRAINT sc_customer_fk FOREIGN KEY (customer_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.customers(customer_id),
            CONSTRAINT sc_product_fk  FOREIGN KEY (product_id)  REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.products(product_id),
            CONSTRAINT sc_region_fk   FOREIGN KEY (region_id)   REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.regions(region_id),
            CONSTRAINT sc_city_fk     FOREIGN KEY (destination_city_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.cities(city_id)
        ) USING delta COMMENT 'Customer support cases for the travel company. resolution_hours and support_cost drop sharply after the AI Support Copilot GA release on 2025-06-02.'"""
      ],
      [
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.regions SELECT CAST(region_id AS BIGINT), region_name, primary_timezone FROM `{{CATALOG}}`.`{{SCHEMA}}`.regions_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.cities SELECT CAST(city_id AS BIGINT), city_name, country_name, CAST(region_id AS BIGINT), latitude, longitude FROM `{{CATALOG}}`.`{{SCHEMA}}`.cities_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.products SELECT CAST(product_id AS BIGINT), product_name, package_tier, market_focus FROM `{{CATALOG}}`.`{{SCHEMA}}`.products_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.customers SELECT CAST(customer_id AS BIGINT), customer_name, customer_segment, industry, contract_value_band, CAST(region_id AS BIGINT) FROM `{{CATALOG}}`.`{{SCHEMA}}`.customers_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.calendar SELECT CAST(date AS DATE), year, quarter, month, week, is_us_holiday, holiday_name, is_peak_season FROM `{{CATALOG}}`.`{{SCHEMA}}`.calendar_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_releases SELECT CAST(release_id AS BIGINT), version, CAST(release_date AS DATE), capability_summary, release_notes, auto_resolved_categories, status FROM `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_releases_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_usage SELECT CAST(usage_date AS DATE), CAST(ai_queries AS BIGINT), CAST(ai_deflections AS BIGINT), CAST(assist_queries AS BIGINT), ai_compute_cost FROM `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_usage_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.support_cases SELECT CAST(case_id AS BIGINT), opened_at, closed_at, CAST(opened_at AS DATE), category, channel, priority, support_tier, CAST(region_id AS BIGINT), CAST(customer_id AS BIGINT), CAST(product_id AS BIGINT), CAST(destination_city_id AS BIGINT), ai_handled, resolution_hours, satisfaction_score, reopened_flag, support_cost FROM `{{CATALOG}}`.`{{SCHEMA}}`.support_cases_bronze"""
      ],
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.support_cases_enriched
           COMMENT 'Analysis-ready support cases joined with customer, product, destination, region, calendar and the AI Support Copilot usage context for the case date. Powers the AI/BI dashboard and Genie. resolution_hours & support_cost drop after the AI GA release on 2025-06-02.'
           AS SELECT c.case_id, c.opened_at, c.closed_at, c.opened_date,
             c.category, c.channel, c.priority, c.support_tier,
             c.ai_handled, c.resolution_hours, c.satisfaction_score, c.reopened_flag, c.support_cost,
             (c.opened_date >= DATE'2025-06-02') AS ai_era,
             cu.customer_name, cu.customer_segment, cu.industry, cu.contract_value_band,
             p.product_name, p.package_tier, p.market_focus,
             r.region_name,
             ci.city_name AS destination_city, ci.country_name AS destination_country,
             ci.latitude AS destination_lat, ci.longitude AS destination_lon,
             cal.is_us_holiday, cal.holiday_name, cal.is_peak_season,
             u.ai_queries AS ai_queries_that_day, u.ai_deflections AS ai_deflections_that_day
           FROM `{{CATALOG}}`.`{{SCHEMA}}`.support_cases c
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.customers cu ON c.customer_id = cu.customer_id
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.products  p  ON c.product_id  = p.product_id
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.regions   r  ON c.region_id   = r.region_id
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.cities    ci ON c.destination_city_id = ci.city_id
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.calendar  cal ON c.opened_date = cal.date
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.ai_assistant_usage u ON c.opened_date = u.usage_date"""
      ],
      [
        """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.support_metrics
           WITH METRICS LANGUAGE YAML AS $$
           version: 1.1
           source: {{CATALOG}}.{{SCHEMA}}.support_cases_enriched
           comment: "Governed customer-support KPIs. Measures stay correct under any dimension grouping. The AI Support Copilot (Agent Bricks) GA on 2025-06-02 sharply improved resolution time, cost and satisfaction."
           dimensions:
             - name: Opened Date
               expr: opened_date
             - name: Opened Month
               expr: DATE_TRUNC('MONTH', opened_at)
             - name: AI Era
               expr: CASE WHEN ai_era THEN 'After AI Copilot' ELSE 'Before AI Copilot' END
             - name: Category
               expr: category
             - name: Channel
               expr: channel
             - name: Priority Level
               expr: CASE WHEN priority='Critical' THEN '1-Critical' WHEN priority='High' THEN '2-High' WHEN priority='Medium' THEN '3-Medium' ELSE '4-Low' END
             - name: Support Tier
               expr: support_tier
             - name: Region
               expr: region_name
             - name: Destination Country
               expr: destination_country
             - name: Destination City
               expr: destination_city
             - name: Destination Latitude
               expr: destination_lat
             - name: Destination Longitude
               expr: destination_lon
             - name: Product
               expr: product_name
             - name: Customer Segment
               expr: customer_segment
             - name: AI Handled
               expr: ai_handled
           measures:
             - name: Total Cases
               expr: COUNT(1)
             - name: Avg Resolution Hours
               expr: AVG(resolution_hours)
             - name: Avg Satisfaction
               expr: AVG(satisfaction_score)
             - name: Total Support Cost
               expr: SUM(support_cost)
             - name: Avg Cost per Case
               expr: AVG(support_cost)
             - name: Reopen Rate
               expr: SUM(CASE WHEN reopened_flag THEN 1 ELSE 0 END) / COUNT(1)
             - name: AI Resolved Rate
               expr: SUM(CASE WHEN ai_handled THEN 1 ELSE 0 END) / COUNT(1)
           $$;"""
      ]
    ],
    "genie_rooms": [
      {
        "id": "customer-support",
        "display_name": "DBDemos - AI/BI - Customer Support: AI Efficiency",
        "description": "Explore how an AI Support Copilot (built on Databricks with Agent Bricks) made customer support dramatically more efficient. Ask why average resolution time and cost dropped in 2025, and Genie will trace it to the AI Copilot GA release and its rising daily usage. Compare performance before vs after the launch, break it down by region, category, channel and product, and quantify the savings.",
        "table_identifiers": [
          "{{CATALOG}}.{{SCHEMA}}.support_cases_enriched",
          "{{CATALOG}}.{{SCHEMA}}.support_metrics",
          "{{CATALOG}}.{{SCHEMA}}.ai_assistant_releases",
          "{{CATALOG}}.{{SCHEMA}}.ai_assistant_usage"
        ],
        "sql_instructions": [
          {
            "title": "Average resolution time and AI deflection by month",
            "content": """SELECT DATE_TRUNC('MONTH', opened_at) AS month,
       ROUND(AVG(resolution_hours), 1) AS avg_resolution_hours,
       ROUND(AVG(CASE WHEN ai_handled THEN 1 ELSE 0 END) * 100, 0) AS pct_ai_resolved
FROM {{CATALOG}}.{{SCHEMA}}.support_cases_enriched
GROUP BY 1 ORDER BY 1;"""
          },
          {
            "title": "Support cost and satisfaction before vs after the AI Copilot",
            "content": """SELECT CASE WHEN ai_era THEN 'After AI Copilot' ELSE 'Before AI Copilot' END AS era,
       ROUND(AVG(resolution_hours), 1) AS avg_resolution_hours,
       ROUND(AVG(satisfaction_score), 2) AS avg_satisfaction,
       ROUND(AVG(support_cost), 0) AS avg_cost_per_case,
       COUNT(*) AS cases
FROM {{CATALOG}}.{{SCHEMA}}.support_cases_enriched
GROUP BY 1 ORDER BY 1;"""
          },
          {
            "title": "When did the AI Support Copilot launch and what does it do?",
            "content": """SELECT version, release_date, status, capability_summary, release_notes
FROM {{CATALOG}}.{{SCHEMA}}.ai_assistant_releases
ORDER BY release_date;"""
          }
        ],
        "instructions": "This data tells the story of a travel company deploying an AI Support Copilot (built on Databricks with Agent Bricks) that reached GA on 2025-06-02 and auto-resolves How-To, Access and Billing cases. When asked WHY resolution time, cost or reopen rate dropped (or what changed in 2025), explain it using the ai_assistant_releases table (the v1.0 GA release notes) and show that ai_assistant_usage (ai_deflections) jumped from 0 at that date while resolution_hours fell. Use the boolean column ai_era (TRUE when opened_date >= 2025-06-02) to compare before vs after. ai_handled = TRUE means the case was auto-resolved by the AI. support_cost is in USD; satisfaction_score is 2-5 (higher is better, ~26% null = no survey returned). support_metrics is a governed METRIC VIEW: query its measures with MEASURE(`Total Cases`), MEASURE(`Avg Resolution Hours`), MEASURE(`Avg Satisfaction`), MEASURE(`Avg Cost per Case`), MEASURE(`AI Resolved Rate`) and group by its dimensions (e.g. `AI Era`, `Region`, `Category`); prefer it for clean aggregated KPIs.",
        "curated_questions": [
          "Why did our average support resolution time drop in 2025?",
          "How much did support cost per case fall after the AI Copilot launched?",
          "What percentage of cases does the AI assistant auto-resolve, and for which categories?",
          "Show average resolution time and customer satisfaction by month",
          "Which region has the slowest resolution time?"
        ]
      }
    ]
  }

# COMMAND ----------


