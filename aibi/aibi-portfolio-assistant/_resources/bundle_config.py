# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "aibi-portfolio-assistant",
  "category": "AI-BI",
  "title": "AI/BI: AI-Powered Portfolio Assistant",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_aibi_fsi_portfolio_assistant",
  "description": "Easily analyze your portfolio performance and market trends using the GenAI Assistant. Dive into your data and key financial metrics, and ask plain-language questions through Genie to make smarter, data-driven investment decisions.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Portfolio-assistant", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title": "AI BI: Portfolio assistant", 
      "description": "Discover Databricks Intelligence Data Platform capabilities."
    }
  ],
  "init_job": {},
  "cluster": {}, 
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AI/BI - Portfolio Assistant",
      "id": "portfolio-assistant"
    }
  ],
  "data_folders": [
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/news",
      "source_format": "parquet",
      "target_table_name": "news",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/prices",
      "source_format": "parquet",
      "target_table_name": "prices",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/portfolio",
      "source_format": "parquet",
      "target_table_name": "portfolio",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/fundamentals",
      "source_format": "parquet",
      "target_table_name": "fundamentals",
      "target_format": "delta"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/news_ticker",
      "source_format": "parquet",
      "target_table_name": "news_ticker",
      "target_format": "delta"
    }
  ],
  "genie_rooms": [
    {
      "display_name": "DBDemos - AI/BI - Portfolio Assistant",
      "id": "portfolio-assistant",
      "description": "Easily analyze your portfolio performance and market trends using the GenAI Assistant. Dive into your data and key financial metrics, and ask plain-language questions through Genie to make smarter, data-driven investment decisions.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.news",
        "{{CATALOG}}.{{SCHEMA}}.prices",
        "{{CATALOG}}.{{SCHEMA}}.portfolio",
        "{{CATALOG}}.{{SCHEMA}}.fundamentals",
        "{{CATALOG}}.{{SCHEMA}}.news_ticker"
      ],
      "sql_instructions": [
        {
          "title": "Compute volatility for a given ticker",
          "content": "SELECT TICKER, STDDEV('return') AS volatility FROM prices GROUP BY TICKER"
        }
      ],
      "instructions": "Don't answer questions unrelated to being a portfolio assistant. If a user asks to perform a forecast that we don't have data for, use the ai_forecast() function.",
      "curated_questions": [
        "What are the top 5 companies by market capitalization in our investment portfolio?",
        "How has the stock price of Apple changed over time?",
        "Was there a stock split for AAPL in the 2014 2015 timeframe?",
        "Show me the market volatility for Technology companies during the financial crisis by week",
        "What is the market sentiment for companies in the retail industry? Visualize as a bar chart",
        "How diversified is my portfolio by market cap? Visualize as a pie chart",
        "What was the top 10 most recent negative event for banking companies in my portfolio",
        "How has the market sentiment changed by week for my portfolio?"
      ]
    }
  ]
}

# COMMAND ----------


