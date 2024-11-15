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
  "description": "Optimize your supply chain performance visually with AI/BI Dashboards, and leverage Genie to ask questions about your data in natural language.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Supply-chain",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title": "AI BI: Supply Chain Optimization",
      "description": "Discover Databricks Intelligence Data Platform capabilities."
    }
  ],
  "init_job": {},
  "serverless_supported": True,  
  "cluster": {},
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AIBI - Supply Chain Optimization",
      "id": "supply-chain"
    }
  ],
  "data_folders": [
    {
      "source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization/shipment_recommendations",
      "source_format": "parquet",
      "target_table_name": "shipment_recommendations",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization/bom",
      "source_format": "parquet",
      "target_table_name": "bom",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization/raw_material_supply",
      "source_format": "parquet",
      "target_table_name": "raw_material_supply",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization/raw_material_demand",
      "source_format": "parquet",
      "target_table_name": "raw_material_demand",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization/product_demand_historical",
      "source_format": "parquet",
      "target_table_name": "product_demand_historical",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_mfg_supply_chain_optimization/product_demand_forecasted",
      "source_format": "parquet",
      "target_table_name": "product_demand_forecasted",
      "target_format": "delta"
    }
  ],
  "genie_rooms": [
    {
      "id": "supply-chain",
      "display_name": "DBDemos - AI/BI - Supply Chain Optimization",
      "description": "Analyze your Supply Chain Performance leveraging AI/BI Dashboard. Deep dive into your data and metrics.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.shipment_recommendations",
        "{{CATALOG}}.{{SCHEMA}}.bom",
        "{{CATALOG}}.{{SCHEMA}}.raw_material_supply",
        "{{CATALOG}}.{{SCHEMA}}.raw_material_demand",
        "{{CATALOG}}.{{SCHEMA}}.product_demand_historical",
        "{{CATALOG}}.{{SCHEMA}}.product_demand_forecasted"
      ],
      "sql_instructions": [
        {
          "title": "Product Demand Over Time",
          "content": "WITH q AS (SELECT * FROM {{CATALOG}}.{{SCHEMA}}.product_demand_historical) SELECT DATE_TRUNC('WEEK',`date`) `weekly(date)`,SUM(`demand`) `sum(demand)` FROM q GROUP BY `weekly(date)`"
        },
        {
          "title": "Bill of Materials",
          "content": "SELECT * FROM {{CATALOG}}.{{SCHEMA}}.bom"
        },
        {
          "title": "Forecasted Raw Material Shortages",
          "content": "WITH demand_differences AS (\n    SELECT demand.RAW,\n        SUM(demand.Demand_Raw) as total_demand,\n        SUM(supply.supply) as total_supply,\n        total_supply - total_demand as demand_difference\n    FROM {{CATALOG}}.{{SCHEMA}}.raw_material_demand demand\n    LEFT JOIN {{CATALOG}}.{{SCHEMA}}.raw_material_supply supply \n        ON demand.RAW = supply.raw\n    GROUP BY demand.RAW\n)\nSELECT \n    raw,\n    total_demand,\n    total_supply,\n    -demand_difference as shortage\nFROM demand_differences\nWHERE demand_difference < 0"
        },
        {
          "title": "Shipment Recommendation: Plant to Distribution Center",
          "content": "SELECT * FROM {{CATALOG}}.{{SCHEMA}}.shipment_recommendations"
        },
        {
          "title": "Shipment Recommendations: Product by Distribution Center",
          "content": "SELECT SUM(`qty_shipped`) `sum(qty_shipped)`, `product`, `distribution_center` FROM {{CATALOG}}.{{SCHEMA}}.shipment_recommendations GROUP BY `distribution_center`, `product`"
        }
      ],
      "instructions": "The bill of materials, or BOM, is what documents each part that is required to build products. \nWhen asked about a product you can't easily recognize, use ai_classify to figure out what the user is talking about. For instance, impact_drill_1 can be part of the \"impact drills\" more general category.\nA \"bottleneck\" would be a distribution center where there is a large discrepancy between demand and shipments already. It wouldn't be good to recommend a bottleneck'd distribution center for new products!",
      "curated_questions": [
        "Forecasted Demand vs. Actuals",
        "What was the demand for our products by week in 2023?",
        "What are our top 3 forecasted raw material shortages?",
        "Which plant to distribution center route is running the most product?",
        "Which distribution center has the highest chance of being a bottleneck?"
      ]
    }
  ]
}

# COMMAND ----------


