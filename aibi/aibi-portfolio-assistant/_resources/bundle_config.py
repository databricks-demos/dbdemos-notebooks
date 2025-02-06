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
  "serverless_supported": True,
  "cluster": {}, 
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AIBI - Portfolio Assistant",
      "id": "portfolio-assistant"
    }
  ],
  "data_folders": [
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/raw_news",
      "source_format": "parquet",
      "target_volume_folder": "raw_news",
      "target_format": "parquet"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/raw_prices",
      "source_format": "parquet",
      "target_volume_folder": "raw_prices",
      "target_format": "parquet"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/raw_portfolio",
      "source_format": "parquet",
      "target_volume_folder": "raw_portfolio",
      "target_format": "parquet"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/raw_fundamentals",
      "source_format": "parquet",
      "target_volume_folder": "raw_fundamentals",
      "target_format": "parquet"
    },
    {
      "source_folder": "aibi/dbdemos_aibi_fsi_cap_markets/raw_news_ticker",
      "source_format": "parquet",
      "target_volume_folder": "raw_news_ticker",
      "target_format": "parquet"
    }
  ],
  "sql_queries": [
      [
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_fundamentals TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for fundamentals created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_fundamentals', format => 'parquet', pathGlobFilter => '*.parquet')",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_news TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for news created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_news', format => 'parquet', pathGlobFilter => '*.parquet')",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_news_ticker TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for news ticker created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_news_ticker', format => 'parquet', pathGlobFilter => '*.parquet')",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_portfolio TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for portfolio created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_portfolio', format => 'parquet', pathGlobFilter => '*.parquet')",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_prices TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for prices created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_prices', format => 'parquet', pathGlobFilter => '*.parquet')"
      ],
      [
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_fundamentals COMMENT 'Cleaned version of the raw fundamentals table' AS SELECT Ticker, MarketCapitalization, OutstandingShares FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_fundamentals WHERE Ticker IS NOT NULL AND MarketCapitalization IS NOT NULL AND OutstandingShares IS NOT NULL",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_news COMMENT 'Cleaned version of the raw news table' AS SELECT ArticleId, PublishedTime, Source, SourceUrl, Title, Sentiment, MarketSentiment FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_news WHERE ArticleId IS NOT NULL AND PublishedTime IS NOT NULL AND Source IS NOT NULL AND SourceUrl IS NOT NULL AND Title IS NOT NULL AND Sentiment IS NOT NULL AND MarketSentiment IS NOT NULL",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_news_ticker COMMENT 'Cleaned version of the raw news ticker table' AS SELECT Ticker, ArticleIds FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_news_ticker WHERE size(ArticleIds) > 0",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_portfolio COMMENT 'Cleaned version of the raw portfolio table' AS SELECT Ticker, CompanyName, CompanyDescription, CompanyWebsite, CompanyLogo, Industry FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_portfolio WHERE Ticker IS NOT NULL AND CompanyName IS NOT NULL AND CompanyDescription IS NOT NULL AND CompanyWebsite IS NOT NULL AND CompanyLogo IS NOT NULL AND Industry IS NOT NULL",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prices COMMENT 'Cleaned version of the raw prices table' AS SELECT Ticker, Date, Open, High, Low, Close, AdjustedClose, Return, Volume, SplitFactor FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_prices WHERE Ticker IS NOT NULL AND Date IS NOT NULL AND Open IS NOT NULL AND High IS NOT NULL AND Low IS NOT NULL AND Close IS NOT NULL AND AdjustedClose IS NOT NULL AND Volume IS NOT NULL AND SplitFactor IS NOT NULL"
      ],
      [
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.fundamentals AS SELECT Ticker AS ticker, MarketCapitalization AS market_capitalization, OutstandingShares AS outstanding_shares FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_fundamentals",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news AS SELECT ArticleId AS article_id, PublishedTime AS published_time, Source AS source, SourceUrl AS source_url, Title AS title, Sentiment AS sentiment, MarketSentiment AS market_sentiment FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_news",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news_ticker AS SELECT Ticker AS ticker, explode(ArticleIds) AS article_id FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_news_ticker",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.portfolio AS SELECT Ticker AS ticker, CompanyName AS company_name, CompanyDescription AS company_description, CompanyWebsite AS company_website, CompanyLogo AS company_logo, Industry AS industry FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_portfolio",
          "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prices AS SELECT Ticker AS ticker, CAST(Date AS DATE) AS date, Open AS open, High AS high, Low AS low, Close AS close, AdjustedClose AS adjusted_close, Return AS return, Volume AS volume, SplitFactor AS split_factor FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_prices"
      ]
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
        "What is the market sentiment for companies in my portfolio in the retail industry? Visualize as a bar chart",
        "What are my largest holdings in my portfolio with an overall negative market sentiment?",
        "Was there a stock split for AAPL in the 2014 2015 timeframe?"
      ]
    }
  ]
}

# COMMAND ----------


