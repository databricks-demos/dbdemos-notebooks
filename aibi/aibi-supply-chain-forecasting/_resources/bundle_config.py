# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "aibi-supply-chain-forecasting",
  "category": "AI-BI",
  "title": "AI/BI: Supply Chain Forecasting",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_aibi_mfg_supply_chain_optimization",
  "description": "Forecast component supply risk with AI/BI Dashboards: weeks of cover, projected stockouts and on-hand inventory across plants and suppliers. Then use Genie to ask which part runs out, when, and why.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Supply-chain",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title": "AI BI: Supply Chain Optimization",
      "description": "Discover Databricks Platform capabilities."
    }
  ],
  "init_job": {},
  "serverless_supported": True,
  "cluster": {},
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AIBI - Supply Chain Optimization",
      "id": "supply-chain",
      "genie_room_id": "supply-chain"
    }
  ],
  "data_folders": [
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/products",             "source_format": "parquet", "target_volume_folder": "products",             "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/distribution_centers", "source_format": "parquet", "target_volume_folder": "distribution_centers", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/market_launches",      "source_format": "parquet", "target_volume_folder": "market_launches",      "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/suppliers",            "source_format": "parquet", "target_volume_folder": "suppliers",            "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/components",           "source_format": "parquet", "target_volume_folder": "components",           "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/plants",               "source_format": "parquet", "target_volume_folder": "plants",               "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/inventory",            "source_format": "parquet", "target_volume_folder": "inventory",            "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/bom",                  "source_format": "parquet", "target_volume_folder": "bom",                  "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/purchase_orders",      "source_format": "parquet", "target_volume_folder": "purchase_orders",      "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization_v2/product_demand",       "source_format": "parquet", "target_volume_folder": "product_demand",       "target_format": "parquet"}
  ],
  "sql_queries": [
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.products_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: products' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/products', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.distribution_centers_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: distribution centers' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/distribution_centers', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.market_launches_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: market launch events' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/market_launches', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.suppliers_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: suppliers' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/suppliers', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.components_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: components' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/components', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.plants_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: plants' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/plants', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.inventory_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: inventory by component x plant' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/inventory', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.bom_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: bill of materials' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/bom', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.purchase_orders_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: purchase orders' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/purchase_orders', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.product_demand_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze: weekly product demand' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/product_demand', format => 'parquet', pathGlobFilter => '*.parquet')"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.products (
          product_id BIGINT COMMENT 'Unique product identifier.',
          product_name STRING COMMENT 'Finished product (e-bike model).',
          category STRING COMMENT 'Product category.',
          PRIMARY KEY (product_id) RELY
      ) USING delta COMMENT 'Finished e-bike products.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.distribution_centers (
          dc_id BIGINT COMMENT 'Unique distribution center identifier.',
          dc_name STRING COMMENT 'Distribution center name.',
          region STRING COMMENT 'Region (EMEA, AMER, APAC, LATAM).',
          country STRING COMMENT 'Country.',
          country_code STRING COMMENT 'ISO country code.',
          latitude DOUBLE COMMENT 'Latitude (for maps).',
          longitude DOUBLE COMMENT 'Longitude (for maps).',
          PRIMARY KEY (dc_id) RELY
      ) USING delta COMMENT 'Distribution centers that fulfil demand.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.market_launches (
          launch_id BIGINT COMMENT 'Unique launch identifier.',
          product_name STRING COMMENT 'Product that launched.',
          region STRING COMMENT 'Region the product launched in.',
          launch_date DATE COMMENT 'Launch date.',
          launch_name STRING COMMENT 'Launch name.',
          description STRING COMMENT 'Launch description.',
          PRIMARY KEY (launch_id) RELY
      ) USING delta COMMENT 'Market launch events - the upstream cause of demand surges. The City E-Bike EMEA market launch (2026-04-20) drives the demand surge that strains the Battery Cell.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.suppliers (
          supplier_id BIGINT COMMENT 'Unique supplier identifier.',
          supplier_name STRING COMMENT 'Supplier name.',
          region STRING COMMENT 'Supplier region.',
          lead_time_weeks INT COMMENT 'Replenishment lead time in weeks. The Battery Cell supplier has an 8-week lead time - the constraint.',
          reliability_pct DOUBLE COMMENT 'On-time delivery reliability (%).',
          PRIMARY KEY (supplier_id) RELY
      ) USING delta COMMENT 'Component suppliers and their lead times.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.components (
          component_id BIGINT COMMENT 'Unique component identifier.',
          component_name STRING COMMENT 'Component name (Battery Cell, Electric Motor, ...).',
          component_type STRING COMMENT 'Component type.',
          supplier_id BIGINT COMMENT 'Supplier of this component.',
          unit_cost DOUBLE COMMENT 'Unit cost (USD).',
          PRIMARY KEY (component_id) RELY,
          CONSTRAINT comp_supplier_fk FOREIGN KEY (supplier_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.suppliers(supplier_id)
      ) USING delta COMMENT 'Components used to build the products.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.plants (
          plant_id BIGINT COMMENT 'Unique plant identifier.',
          plant_name STRING COMMENT 'Assembly plant name.',
          region STRING COMMENT 'Plant region.',
          PRIMARY KEY (plant_id) RELY
      ) USING delta COMMENT 'Assembly plants that hold component inventory. Rotterdam serves EMEA/APAC, Detroit serves AMER.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.inventory (
          component_id BIGINT COMMENT 'Component.',
          plant_id BIGINT COMMENT 'Plant holding the inventory.',
          on_hand_units BIGINT COMMENT 'Units currently on hand.',
          safety_stock_units BIGINT COMMENT 'Safety stock threshold.',
          weekly_supply_units BIGINT COMMENT 'Steady weekly inbound replenishment.',
          CONSTRAINT inv_component_fk FOREIGN KEY (component_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.components(component_id),
          CONSTRAINT inv_plant_fk FOREIGN KEY (plant_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.plants(plant_id)
      ) USING delta COMMENT 'Component inventory per plant. The Battery Cell is the bottleneck, worst at Rotterdam.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.bom (
          product_id BIGINT COMMENT 'Finished product.',
          component_id BIGINT COMMENT 'Component used.',
          qty_per_unit INT COMMENT 'Quantity of the component per finished unit.',
          CONSTRAINT bom_product_fk FOREIGN KEY (product_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.products(product_id),
          CONSTRAINT bom_component_fk FOREIGN KEY (component_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.components(component_id)
      ) USING delta COMMENT 'Bill of materials. Every product uses the Battery Cell, so a demand surge in any e-bike drains it.'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.purchase_orders (
          po_id BIGINT COMMENT 'Unique purchase order identifier.',
          component_id BIGINT COMMENT 'Component ordered.',
          plant_id BIGINT COMMENT 'Destination plant.',
          supplier_id BIGINT COMMENT 'Supplier.',
          order_week DATE COMMENT 'Week the PO was placed.',
          expected_arrival_week DATE COMMENT 'Expected arrival week (order_week + supplier lead time).',
          qty_units BIGINT COMMENT 'Quantity ordered.',
          status STRING COMMENT 'PO status (Received / In Transit / Planned).',
          PRIMARY KEY (po_id) RELY,
          CONSTRAINT po_component_fk FOREIGN KEY (component_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.components(component_id),
          CONSTRAINT po_plant_fk FOREIGN KEY (plant_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.plants(plant_id),
          CONSTRAINT po_supplier_fk FOREIGN KEY (supplier_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.suppliers(supplier_id)
      ) USING delta COMMENT 'Open and historical purchase orders (inbound supply).'""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.product_demand (
          demand_id BIGINT COMMENT 'Unique demand row identifier.',
          week DATE COMMENT 'Week of demand.',
          product_id BIGINT COMMENT 'Product.',
          dc_id BIGINT COMMENT 'Distribution center.',
          demand_units BIGINT COMMENT 'Units of demand that week.',
          PRIMARY KEY (demand_id) RELY,
          CONSTRAINT dem_product_fk FOREIGN KEY (product_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.products(product_id),
          CONSTRAINT dem_dc_fk FOREIGN KEY (dc_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.distribution_centers(dc_id)
      ) USING delta COMMENT 'Weekly product demand by DC. City E-Bike demand surges in EMEA from 2026-04-20 (new market).'"""
    ],
    [
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.products SELECT CAST(product_id AS BIGINT), product_name, category FROM `{{CATALOG}}`.`{{SCHEMA}}`.products_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.distribution_centers SELECT CAST(dc_id AS BIGINT), dc_name, region, country, country_code, latitude, longitude FROM `{{CATALOG}}`.`{{SCHEMA}}`.distribution_centers_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.market_launches SELECT CAST(launch_id AS BIGINT), product_name, region, CAST(launch_date AS DATE), launch_name, description FROM `{{CATALOG}}`.`{{SCHEMA}}`.market_launches_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.suppliers SELECT CAST(supplier_id AS BIGINT), supplier_name, region, CAST(lead_time_weeks AS INT), reliability_pct FROM `{{CATALOG}}`.`{{SCHEMA}}`.suppliers_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.components SELECT CAST(component_id AS BIGINT), component_name, component_type, CAST(supplier_id AS BIGINT), unit_cost FROM `{{CATALOG}}`.`{{SCHEMA}}`.components_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.plants SELECT CAST(plant_id AS BIGINT), plant_name, region FROM `{{CATALOG}}`.`{{SCHEMA}}`.plants_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.inventory SELECT CAST(component_id AS BIGINT), CAST(plant_id AS BIGINT), CAST(on_hand_units AS BIGINT), CAST(safety_stock_units AS BIGINT), CAST(weekly_supply_units AS BIGINT) FROM `{{CATALOG}}`.`{{SCHEMA}}`.inventory_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.bom SELECT CAST(product_id AS BIGINT), CAST(component_id AS BIGINT), CAST(qty_per_unit AS INT) FROM `{{CATALOG}}`.`{{SCHEMA}}`.bom_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.purchase_orders SELECT CAST(po_id AS BIGINT), CAST(component_id AS BIGINT), CAST(plant_id AS BIGINT), CAST(supplier_id AS BIGINT), CAST(order_week AS DATE), CAST(expected_arrival_week AS DATE), CAST(qty_units AS BIGINT), status FROM `{{CATALOG}}`.`{{SCHEMA}}`.purchase_orders_bronze""",
      """INSERT OVERWRITE `{{CATALOG}}`.`{{SCHEMA}}`.product_demand SELECT CAST(demand_id AS BIGINT), CAST(week AS DATE), CAST(product_id AS BIGINT), CAST(dc_id AS BIGINT), CAST(demand_units AS BIGINT) FROM `{{CATALOG}}`.`{{SCHEMA}}`.product_demand_bronze"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.demand_enriched
         COMMENT 'Analysis-ready weekly demand joined with product and distribution center (region, country, geo). Powers the demand metric view, the map and the forecast.'
         AS SELECT d.demand_id, d.week, d.product_id, p.product_name, p.category,
           d.dc_id, dc.dc_name, dc.region, dc.country, dc.country_code, dc.latitude, dc.longitude,
           d.demand_units
         FROM `{{CATALOG}}`.`{{SCHEMA}}`.product_demand d
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.products p ON d.product_id = p.product_id
         JOIN `{{CATALOG}}`.`{{SCHEMA}}`.distribution_centers dc ON d.dc_id = dc.dc_id"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.component_status
         COMMENT 'Per component x plant supply risk: on-hand, safety stock, supplier lead time, recent weekly demand (rolled up from the BOM) and weeks of cover (on-hand / weekly demand). A component is At risk when weeks of cover is below the supplier lead time. Only the Battery Cell is at risk - worst at Rotterdam (~2-3 weeks vs an 8-week lead time).'
         AS WITH plant_demand AS (
              SELECT b.component_id,
                CASE WHEN d.region IN ('EMEA','APAC') THEN 1 ELSE 2 END AS plant_id,
                SUM(d.demand_units * b.qty_per_unit) / COUNT(DISTINCT d.week) AS awd
              FROM `{{CATALOG}}`.`{{SCHEMA}}`.demand_enriched d
              JOIN `{{CATALOG}}`.`{{SCHEMA}}`.bom b ON d.product_id = b.product_id
              WHERE d.week >= (SELECT date_sub(max(week), 28) FROM `{{CATALOG}}`.`{{SCHEMA}}`.demand_enriched)
              GROUP BY 1, 2)
            SELECT c.component_id, c.component_name, c.component_type, pl.plant_id, pl.plant_name,
              s.supplier_name, s.lead_time_weeks,
              i.on_hand_units, i.safety_stock_units, i.weekly_supply_units,
              ROUND(pd.awd) AS avg_weekly_demand,
              ROUND(i.on_hand_units / pd.awd, 1) AS weeks_of_cover,
              CASE WHEN i.on_hand_units / pd.awd <= s.lead_time_weeks THEN 'At risk' ELSE 'Healthy' END AS status
            FROM `{{CATALOG}}`.`{{SCHEMA}}`.components c
            JOIN `{{CATALOG}}`.`{{SCHEMA}}`.suppliers s ON c.supplier_id = s.supplier_id
            JOIN `{{CATALOG}}`.`{{SCHEMA}}`.inventory i ON c.component_id = i.component_id
            JOIN `{{CATALOG}}`.`{{SCHEMA}}`.plants pl ON i.plant_id = pl.plant_id
            JOIN plant_demand pd ON pd.component_id = c.component_id AND pd.plant_id = pl.plant_id"""
    ],
    [
      """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.metrics_demand
         WITH METRICS LANGUAGE YAML AS $$
         version: 1.1
         source: {{CATALOG}}.{{SCHEMA}}.demand_enriched
         comment: "Governed e-bike demand KPIs. Weekly product demand by DC and region. City E-Bike demand surges in EMEA from 2026-04-20 (new market), which via the BOM drains the shared Battery Cell."
         dimensions:
           - name: Week
             expr: week
           - name: Product
             expr: product_name
           - name: Category
             expr: category
           - name: Distribution Center
             expr: dc_name
           - name: Region
             expr: region
           - name: Country
             expr: country
           - name: Country Code
             expr: country_code
           - name: Latitude
             expr: latitude
           - name: Longitude
             expr: longitude
         measures:
           - name: Demand Units
             expr: SUM(demand_units)
         $$;"""
    ]
  ],
  "genie_rooms": [
    {
      "id": "supply-chain",
      "display_name": "DBDemos - AI/BI - Supply Chain Forecasting",
      "description": "Explore an e-bike manufacturer's supply chain. A new EMEA market launch drove a City E-Bike demand surge that is burning through the shared Battery Cell. Ask which component is about to run out, when it stocks out, why it can't be reordered in time (the 8-week supplier lead time), and what's driving the demand - Genie traces it across demand, BOM, inventory, suppliers and the market-launch event.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.demand_enriched",
        "{{CATALOG}}.{{SCHEMA}}.metrics_demand",
        "{{CATALOG}}.{{SCHEMA}}.component_status",
        "{{CATALOG}}.{{SCHEMA}}.bom",
        "{{CATALOG}}.{{SCHEMA}}.inventory",
        "{{CATALOG}}.{{SCHEMA}}.suppliers",
        "{{CATALOG}}.{{SCHEMA}}.market_launches"
      ],
      "sql_instructions": [
        {
          "title": "Which components are at risk (weeks of cover vs supplier lead time)",
          "content": """SELECT component_name, plant_name, supplier_name, lead_time_weeks, on_hand_units, weeks_of_cover, status
FROM {{CATALOG}}.{{SCHEMA}}.component_status
ORDER BY weeks_of_cover ASC;"""
        },
        {
          "title": "Weekly demand by region (EMEA surge after the market launch)",
          "content": """SELECT DATE_TRUNC('WEEK', week) AS week, region, SUM(demand_units) AS demand
FROM {{CATALOG}}.{{SCHEMA}}.demand_enriched
WHERE week >= DATE'2026-03-01'
GROUP BY 1,2 ORDER BY 1,2;"""
        },
        {
          "title": "What launched recently, and which products use the Battery Cell",
          "content": """SELECT * FROM {{CATALOG}}.{{SCHEMA}}.market_launches ORDER BY launch_date DESC;"""
        }
      ],
      "instructions": "This data is for an e-bike manufacturer (products: City E-Bike, Cargo E-Bike, Folding E-Bike, E-Scooter, E-Moped) built from components via a bill of materials (bom). The story: a new EMEA market opened on 2026-04-20 (market_launches), driving a sharp City E-Bike demand surge in EMEA. Because every product uses the shared Battery Cell, that surge is draining the Battery Cell faster than its supplier (PowerCell Industries, 8-week lead_time_weeks) can replenish it - so it is projected to stock out, worst at the Rotterdam plant (Rotterdam serves EMEA+APAC; Detroit serves AMER). Use component_status for 'weeks of cover' and which component is at risk (a component is At risk when weeks_of_cover <= the supplier lead_time_weeks). 'Weeks of cover' = on_hand_units / average weekly demand, where weekly demand is rolled up from demand x bom.qty_per_unit. To project a stockout date or forecast demand, use the ai_forecast SQL function on weekly demand. When asked WHY demand surged, point to market_launches (the EMEA City E-Bike launch). metrics_demand is a governed METRIC VIEW: query its measure with MEASURE(`Demand Units`) and group by dimensions like `Region`, `Product`, `Week`.",
      "curated_questions": [
        "Which component is about to run out?",
        "Why is Battery Cell demand surging?",
        "Show weekly demand by region since March 2026",
        "Which products use the Battery Cell, and how many per unit?",
        "Why can't we just reorder more Battery Cells in time?"
      ]
    }
  ]
}
