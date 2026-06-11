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
    "description": "Track sales performance against target with AI/BI Dashboards: unify CRM, ERP, Finance and product data, forecast where the quarter lands, and use Genie to ask why in natural language.",
    "bundle": True,
    "notebooks": [
      {
        "path": "AI-BI-Sales-pipeline",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "AI BI: Sales Pipeline Review",
        "description": "Discover Databricks Platform capabilities."
      }
    ],
    "init_job": {},
    "serverless_supported": True,
    "cluster": {},
    "pipelines": [],
    "dashboards": [
      {
        "name": "[dbdemos] AIBI - Sales Pipeline Review",
        "id": "sales-pipeline",
        "genie_room_id": "sales-pipeline"
      }
    ],
    "data_folders": [
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/products",          "source_format": "parquet", "target_volume_folder": "products",          "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/product_launches",  "source_format": "parquet", "target_volume_folder": "product_launches",  "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/sales_targets",     "source_format": "parquet", "target_volume_folder": "sales_targets",     "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/crm_reps",          "source_format": "parquet", "target_volume_folder": "crm_reps",          "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/crm_accounts",      "source_format": "parquet", "target_volume_folder": "crm_accounts",      "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/crm_opportunities", "source_format": "parquet", "target_volume_folder": "crm_opportunities", "target_format": "parquet"},
      {"source_folder": "aibi/dbdemos_aibi_sales_pipeline_v2/erp_orders",        "source_format": "parquet", "target_volume_folder": "erp_orders",        "target_format": "parquet"}
    ],
    "sql_queries": [
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.products_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: product lines (PIM)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/products', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.product_launches_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: product regional launch dates (PIM)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/product_launches', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sales_targets_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: quarterly revenue targets (Finance)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/sales_targets', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.crm_reps_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: sales reps (Salesforce)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/crm_reps', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: customer accounts (Salesforce)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/crm_accounts', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.crm_opportunities_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: open pipeline opportunities (Salesforce)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/crm_opportunities', format => 'parquet', pathGlobFilter => '*.parquet')""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.erp_orders_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: daily orders / actual revenue (ERP)' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/erp_orders', format => 'parquet', pathGlobFilter => '*.parquet')"""
      ],
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.products (
            product_line STRING COMMENT 'Product line (Skincare, Makeup, Fragrance, Haircare).',
            category STRING COMMENT 'Product category description.',
            PRIMARY KEY (product_line) RELY
        ) USING delta COMMENT 'Beauty product lines (PIM).'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.product_launches (
            launch_id BIGINT COMMENT 'Unique launch identifier.',
            product_line STRING COMMENT 'Product line launched.',
            region STRING COMMENT 'Region where the line became available (or Global).',
            launch_date DATE COMMENT 'Date the line became available in the region.',
            launch_name STRING COMMENT 'Launch name.',
            description STRING COMMENT 'Launch description.',
            PRIMARY KEY (launch_id) RELY
        ) USING delta COMMENT 'Product regional availability/launches (PIM). The root-cause table: the new Fragrance line launched in EMEA on 2026-05-04, driving the EMEA sales spike.'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sales_targets (
            quarter_start DATE COMMENT 'First day of the fiscal quarter.',
            target_revenue DOUBLE COMMENT 'Company-wide revenue target for the quarter (USD).',
            PRIMARY KEY (quarter_start) RELY
        ) USING delta COMMENT 'Company-wide quarterly revenue targets (Finance).'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.crm_reps (
            owner_id BIGINT COMMENT 'Unique sales rep identifier.',
            rep_name STRING COMMENT 'Sales rep name.',
            region STRING COMMENT 'Rep region.',
            title STRING COMMENT 'Rep title.',
            PRIMARY KEY (owner_id) RELY
        ) USING delta COMMENT 'Sales reps (Salesforce).'"""
      ],
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts (
            account_id BIGINT COMMENT 'Unique account identifier.',
            account_name STRING COMMENT 'Account (retailer) name.',
            segment STRING COMMENT 'Retailer segment (Department Store, Specialty Retail, Pharmacy, E-commerce).',
            region STRING COMMENT 'Account region (AMER, EMEA, APAC, LATAM).',
            country STRING COMMENT 'Account country.',
            country_code STRING COMMENT 'ISO country code.',
            latitude DOUBLE COMMENT 'Account latitude (for maps).',
            longitude DOUBLE COMMENT 'Account longitude (for maps).',
            owner_id BIGINT COMMENT 'Sales rep that owns the account.',
            PRIMARY KEY (account_id) RELY,
            CONSTRAINT acc_owner_fk FOREIGN KEY (owner_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.crm_reps(owner_id)
        ) USING delta COMMENT 'Customer accounts / retailers (Salesforce).'"""
      ],
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.crm_opportunities (
            opp_id BIGINT COMMENT 'Unique opportunity identifier.',
            account_id BIGINT COMMENT 'Account the opportunity belongs to.',
            product_line STRING COMMENT 'Product line of the opportunity.',
            stage STRING COMMENT 'Pipeline stage.',
            expected_revenue DOUBLE COMMENT 'Expected revenue of the opportunity (USD).',
            created_date DATE COMMENT 'Date the opportunity was created.',
            close_date DATE COMMENT 'Expected close date.',
            PRIMARY KEY (opp_id) RELY,
            CONSTRAINT opp_account_fk FOREIGN KEY (account_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts(account_id)
        ) USING delta COMMENT 'Open pipeline opportunities / expansion deals (Salesforce).'""",
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.erp_orders (
            order_id BIGINT COMMENT 'Unique order identifier.',
            order_date DATE COMMENT 'Order date.',
            account_id BIGINT COMMENT 'Account that placed the order.',
            product_line STRING COMMENT 'Product line ordered.',
            region STRING COMMENT 'Region of the order.',
            units BIGINT COMMENT 'Units ordered.',
            revenue DOUBLE COMMENT 'Order revenue (USD).',
            PRIMARY KEY (order_id) RELY,
            CONSTRAINT ord_account_fk FOREIGN KEY (account_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts(account_id),
            CONSTRAINT ord_product_fk FOREIGN KEY (product_line) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.products(product_line)
        ) USING delta COMMENT 'Daily orders / actual revenue (ERP) - the sales fact. EMEA Fragrance revenue spikes after the 2026-05-04 launch.'"""
      ],
      [
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.products SELECT product_line, category FROM `{{CATALOG}}`.`{{SCHEMA}}`.products_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.product_launches SELECT CAST(launch_id AS BIGINT), product_line, region, CAST(launch_date AS DATE), launch_name, description FROM `{{CATALOG}}`.`{{SCHEMA}}`.product_launches_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.sales_targets SELECT CAST(quarter_start AS DATE), target_revenue FROM `{{CATALOG}}`.`{{SCHEMA}}`.sales_targets_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.crm_reps SELECT CAST(owner_id AS BIGINT), rep_name, region, title FROM `{{CATALOG}}`.`{{SCHEMA}}`.crm_reps_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts SELECT CAST(account_id AS BIGINT), account_name, segment, region, country, country_code, latitude, longitude, CAST(owner_id AS BIGINT) FROM `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.crm_opportunities SELECT CAST(opp_id AS BIGINT), CAST(account_id AS BIGINT), product_line, stage, expected_revenue, CAST(created_date AS DATE), CAST(close_date AS DATE) FROM `{{CATALOG}}`.`{{SCHEMA}}`.crm_opportunities_bronze""",
        """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.erp_orders SELECT CAST(order_id AS BIGINT), CAST(order_date AS DATE), CAST(account_id AS BIGINT), product_line, region, CAST(units AS BIGINT), revenue FROM `{{CATALOG}}`.`{{SCHEMA}}`.erp_orders_bronze"""
      ],
      [
        """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.orders_enriched
           COMMENT 'Analysis-ready sales fact: ERP orders unified with the Salesforce account (region, segment, owner, geo), the rep, and the product category. Powers the AI/BI dashboard and Genie. EMEA Fragrance revenue spikes after the new line launched there on 2026-05-04.'
           AS SELECT o.order_id, o.order_date, o.product_line, p.category,
             o.region, a.account_id, a.account_name, a.segment, a.country, a.country_code, a.latitude, a.longitude,
             r.rep_name, r.title AS rep_title,
             o.units, o.revenue
           FROM `{{CATALOG}}`.`{{SCHEMA}}`.erp_orders o
           JOIN `{{CATALOG}}`.`{{SCHEMA}}`.crm_accounts a ON o.account_id = a.account_id
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.crm_reps r ON a.owner_id = r.owner_id
           LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.products p ON o.product_line = p.product_line"""
      ],
      [
        """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_sales
           WITH METRICS LANGUAGE YAML AS $$
           version: 1.1
           source: {{CATALOG}}.{{SCHEMA}}.orders_enriched
           comment: "Governed beauty-brand sales KPIs. Revenue is actual ERP order revenue. The new Fragrance line launched in EMEA on 2026-05-04, driving a sales spike there that pushes the quarter forecast above target."
           dimensions:
             - name: Order Date
               expr: order_date
             - name: Product Line
               expr: product_line
             - name: Category
               expr: category
             - name: Region
               expr: region
             - name: Segment
               expr: segment
             - name: Account
               expr: account_name
             - name: Country
               expr: country
             - name: Country Code
               expr: country_code
             - name: Latitude
               expr: latitude
             - name: Longitude
               expr: longitude
             - name: Sales Rep
               expr: rep_name
           measures:
             - name: Revenue
               expr: SUM(revenue)
             - name: Units
               expr: SUM(units)
             - name: Orders
               expr: COUNT(1)
             - name: Avg Order Value
               expr: SUM(revenue) / NULLIF(COUNT(1),0)
             - name: Avg Unit Price
               expr: SUM(revenue) / NULLIF(SUM(units),0)
           $$;"""
      ]
    ],
    "genie_rooms": [
      {
        "id": "sales-pipeline",
        "display_name": "DBDemos - AI/BI - Sales Pipeline Review",
        "description": "Track a beauty brand's sales against its quarterly target across regions and product lines, with data unified from Salesforce (accounts/pipeline), the ERP (actual orders/revenue), Finance (targets) and the product catalog (launches). Ask whether we'll hit the quarter, what's driving the recent spike, and Genie will trace it to the new Fragrance line that launched in EMEA on 2026-05-04.",
        "table_identifiers": [
          "{{CATALOG}}.{{SCHEMA}}.orders_enriched",
          "{{CATALOG}}.{{SCHEMA}}.metrics_sales",
          "{{CATALOG}}.{{SCHEMA}}.product_launches",
          "{{CATALOG}}.{{SCHEMA}}.sales_targets"
        ],
        "sql_instructions": [
          {
            "title": "Quarter-to-date revenue vs target (Q2 2026)",
            "content": """SELECT ROUND(SUM(o.revenue)) AS qtd_revenue,
       (SELECT target_revenue FROM {{CATALOG}}.{{SCHEMA}}.sales_targets WHERE quarter_start = DATE'2026-04-01') AS quarter_target
FROM {{CATALOG}}.{{SCHEMA}}.orders_enriched o
WHERE o.order_date >= DATE'2026-04-01' AND o.order_date <= DATE'2026-06-30';"""
          },
          {
            "title": "Weekly revenue by region since the spike",
            "content": """SELECT DATE_TRUNC('WEEK', order_date) AS week, region, ROUND(SUM(revenue)) AS revenue
FROM {{CATALOG}}.{{SCHEMA}}.orders_enriched
WHERE order_date >= DATE'2026-03-01'
GROUP BY 1,2 ORDER BY 1,2;"""
          },
          {
            "title": "EMEA revenue by product line, and when each launched there",
            "content": """SELECT o.product_line,
       ROUND(SUM(o.revenue)) AS emea_revenue,
       MAX(l.launch_date) AS emea_launch_date
FROM {{CATALOG}}.{{SCHEMA}}.orders_enriched o
LEFT JOIN {{CATALOG}}.{{SCHEMA}}.product_launches l
  ON l.product_line = o.product_line AND l.region = 'EMEA'
WHERE o.region = 'EMEA'
GROUP BY 1 ORDER BY emea_revenue DESC;"""
          }
        ],
        "instructions": "This data tells the story of a global beauty brand tracking sales against a company-wide quarterly revenue target. Data is unified from several systems: Salesforce (crm_accounts, crm_opportunities, crm_reps), the ERP (erp_orders = actual revenue, the fact behind orders_enriched), Finance (sales_targets) and the product catalog (product_launches). The current fiscal quarter is Q2 2026 (2026-04-01 to 2026-06-30); its target is in sales_targets. When asked whether we'll HIT the target, compare quarter-to-date revenue (and, if asked to forecast, use the ai_forecast SQL function on daily revenue to project quarter-end) against the Q2 target. When asked WHY revenue is spiking / why we're beating, explain that the new Fragrance product line became available in EMEA on 2026-05-04 (product_launches) and EMEA Fragrance revenue surged afterwards - drill region (EMEA) then product_line (Fragrance) then join product_launches for the date. metrics_sales is a governed METRIC VIEW: query its measures with MEASURE(`Revenue`), MEASURE(`Units`), MEASURE(`Orders`), MEASURE(`Avg Order Value`) and group by its dimensions (e.g. `Region`, `Product Line`, `Segment`, `Sales Rep`); prefer it for clean aggregated KPIs.",
        "curated_questions": [
          "Are we going to hit our revenue target this quarter?",
          "What is driving the recent revenue spike?",
          "Why is EMEA revenue surging?",
          "Show weekly revenue by region since March 2026",
          "Which product line grew the most in EMEA, and when did it launch there?"
        ]
      }
    ]
}
