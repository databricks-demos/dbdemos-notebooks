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
      "description": "Discover Databricks Platform capabilities."
    }
  ],
  "init_job": {},
  "serverless_supported": True,
  "cluster": {}, 
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AIBI - Portfolio Assistant",
      "id": "portfolio-assistant",
      "genie_room_id": "portfolio-assistant"
    }
  ],
  "data_folders": [
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/securities", "source_format": "parquet", "target_volume_folder": "securities", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/sectors", "source_format": "parquet", "target_volume_folder": "sectors", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/portfolios", "source_format": "parquet", "target_volume_folder": "portfolios", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/holdings", "source_format": "parquet", "target_volume_folder": "holdings", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/rebalances", "source_format": "parquet", "target_volume_folder": "rebalances", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/prices", "source_format": "parquet", "target_volume_folder": "prices", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/benchmark", "source_format": "parquet", "target_volume_folder": "benchmark", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/news", "source_format": "parquet", "target_volume_folder": "news", "target_format": "parquet"},
    {"source_folder": "aibi/dbdemos_aibi_fsi_portfolio_assistant_v2/news_ticker", "source_format": "parquet", "target_volume_folder": "news_ticker", "target_format": "parquet"}
  ],
  "sql_queries": [
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.securities_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: securities' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/securities', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sectors_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: sectors' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/sectors', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.portfolios_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: portfolios' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/portfolios', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.holdings_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: holdings' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/holdings', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.rebalances_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: rebalances' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/rebalances', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prices_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: prices' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/prices', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.benchmark_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: benchmark' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/benchmark', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: news' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/news', format => 'parquet', pathGlobFilter => '*.parquet')""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news_ticker_raw TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE) COMMENT 'Bronze: news_ticker' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/news_ticker', format => 'parquet', pathGlobFilter => '*.parquet')"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.securities AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.securities_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sectors AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.sectors_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.portfolios AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.portfolios_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.holdings AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.holdings_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.rebalances AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.rebalances_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.prices AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.prices_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.benchmark AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.benchmark_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.news_raw""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news_ticker AS SELECT * FROM `{{CATALOG}}`.`{{SCHEMA}}`.news_ticker_raw"""
    ],
    [
      """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.holdings_asof AS WITH base AS (SELECT h.portfolio_id,h.ticker,h.effective_date,h.weight_pct,h.shares,s.ai_exposure,s.sector,s.company FROM `{{CATALOG}}`.`{{SCHEMA}}`.holdings h JOIN `{{CATALOG}}`.`{{SCHEMA}}`.securities s ON h.ticker=s.ticker) SELECT *, lead(effective_date) OVER (PARTITION BY portfolio_id,ticker ORDER BY effective_date) AS next_eff FROM base""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.news_enriched AS SELECT n.*,nt.ticker,s.company,s.ai_exposure,s.sector FROM `{{CATALOG}}`.`{{SCHEMA}}`.news n JOIN `{{CATALOG}}`.`{{SCHEMA}}`.news_ticker nt ON n.article_id=nt.article_id JOIN `{{CATALOG}}`.`{{SCHEMA}}`.securities s ON s.ticker=nt.ticker"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_performance AS WITH px AS (SELECT ticker,date,close,close/lag(close) OVER (PARTITION BY ticker ORDER BY date)-1 AS daily_ret FROM `{{CATALOG}}`.`{{SCHEMA}}`.prices), asof AS (SELECT portfolio_id,ticker,weight_pct,effective_date,next_eff FROM `{{CATALOG}}`.`{{SCHEMA}}`.holdings_asof), pd AS (SELECT a.portfolio_id,px.date,SUM(px.daily_ret*a.weight_pct/100.0) port_ret FROM px JOIN asof a ON px.ticker=a.ticker AND px.date>=a.effective_date AND (a.next_eff IS NULL OR px.date<a.next_eff) WHERE px.daily_ret IS NOT NULL GROUP BY a.portfolio_id,px.date), bench AS (SELECT date,close/lag(close) OVER (ORDER BY date)-1 AS bench_ret FROM `{{CATALOG}}`.`{{SCHEMA}}`.benchmark), j AS (SELECT p.portfolio_id,p.date,p.port_ret,b.bench_ret FROM pd p JOIN bench b ON p.date=b.date WHERE b.bench_ret IS NOT NULL) SELECT portfolio_id,date,port_ret,bench_ret, exp(sum(ln(1+port_ret)) OVER (PARTITION BY portfolio_id ORDER BY date))-1 cum_port_ret, exp(sum(ln(1+bench_ret)) OVER (PARTITION BY portfolio_id ORDER BY date))-1 cum_bench_ret FROM j""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.concentration_timeseries AS WITH d AS (SELECT distinct date FROM `{{CATALOG}}`.`{{SCHEMA}}`.prices), asof AS (SELECT d.date,e.portfolio_id,e.weight_pct,e.ai_exposure FROM d JOIN `{{CATALOG}}`.`{{SCHEMA}}`.holdings_asof e ON d.date>=e.effective_date AND (e.next_eff IS NULL OR d.date<e.next_eff)) SELECT portfolio_id,date, round(sum(CASE WHEN ai_exposure='Core AI' THEN weight_pct ELSE 0 END)/sum(weight_pct)*100,1) core_ai_pct, round(sum(CASE WHEN ai_exposure='AI-adjacent' THEN weight_pct ELSE 0 END)/sum(weight_pct)*100,1) ai_adjacent_pct, round(sum(CASE WHEN ai_exposure='Non-AI' THEN weight_pct ELSE 0 END)/sum(weight_pct)*100,1) non_ai_pct, count(CASE WHEN weight_pct>0 THEN 1 END) num_holdings FROM asof GROUP BY portfolio_id,date""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sector_exposure AS WITH cur AS (SELECT e.sector,sum(e.weight_pct) fund_w FROM `{{CATALOG}}`.`{{SCHEMA}}`.holdings_asof e WHERE e.portfolio_id=1 AND e.next_eff IS NULL AND e.weight_pct>0 GROUP BY e.sector) SELECT coalesce(cur.sector,s.sector) sector, coalesce(cur.fund_w,0)/100.0 fund_weight, s.benchmark_weight, coalesce(cur.fund_w,0)/100.0-s.benchmark_weight overweight FROM `{{CATALOG}}`.`{{SCHEMA}}`.sectors s FULL OUTER JOIN cur ON s.sector=cur.sector""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.holdings_enriched AS SELECT p.portfolio_name,e.portfolio_id,e.ticker,e.company,e.sector,e.ai_exposure,e.weight_pct,sec.market_cap_b,sec.country,sec.industry FROM `{{CATALOG}}`.`{{SCHEMA}}`.holdings_asof e JOIN `{{CATALOG}}`.`{{SCHEMA}}`.portfolios p ON p.portfolio_id=e.portfolio_id JOIN `{{CATALOG}}`.`{{SCHEMA}}`.securities sec ON sec.ticker=e.ticker WHERE e.next_eff IS NULL AND e.weight_pct>0"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sharpe_analysis AS WITH ret AS (SELECT ticker,date,close/lag(close) OVER (PARTITION BY ticker ORDER BY date)-1 r FROM `{{CATALOG}}`.`{{SCHEMA}}`.prices), stats AS (SELECT ticker,avg(r)*252 ar,stddev(r)*sqrt(252) arisk,avg(r)/nullif(stddev(r),0)*sqrt(252) sr FROM ret WHERE r IS NOT NULL GROUP BY ticker) SELECT s.ticker,sec.company,sec.sector,sec.ai_exposure,sec.market_cap_b,he.weight_pct fund_weight,round(s.ar*100,1) annualized_return,round(s.arisk*100,1) annualized_risk,round(s.sr,2) sharpe_ratio FROM stats s JOIN `{{CATALOG}}`.`{{SCHEMA}}`.securities sec ON sec.ticker=s.ticker LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.holdings_enriched he ON he.ticker=s.ticker AND he.portfolio_id=1""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.var_metrics AS WITH r_all AS (SELECT port_ret FROM `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_performance WHERE portfolio_id=1), r_post AS (SELECT port_ret FROM `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_performance WHERE portfolio_id=1 AND date>=date'2025-08-04') SELECT 'Post-pivot (current risk)' regime,1 srt,0.95 confidence,round(percentile(port_ret,0.05)*-100,2) var_pct,round(percentile(port_ret,0.05)*-100000000) var_dollar FROM r_post UNION ALL SELECT 'Post-pivot (current risk)',1,0.99,round(percentile(port_ret,0.01)*-100,2),round(percentile(port_ret,0.01)*-100000000) FROM r_post UNION ALL SELECT 'Full history',2,0.95,round(percentile(port_ret,0.05)*-100,2),round(percentile(port_ret,0.05)*-100000000) FROM r_all UNION ALL SELECT 'Full history',2,0.99,round(percentile(port_ret,0.01)*-100,2),round(percentile(port_ret,0.01)*-100000000) FROM r_all""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.returns_distribution AS SELECT round(port_ret*200)/2.0 return_bucket_pct,count(*) days FROM `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_performance WHERE portfolio_id=1 AND date>=date'2025-08-04' GROUP BY 1 ORDER BY 1""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.risk_metrics AS WITH p AS (SELECT date,port_ret,bench_ret FROM `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_performance WHERE portfolio_id=1), agg AS (SELECT 'Overall' period,1 sort_order,port_ret,bench_ret FROM p UNION ALL SELECT CASE WHEN date<date'2025-08-04' THEN 'Before pivot' ELSE 'After pivot' END, CASE WHEN date<date'2025-08-04' THEN 2 ELSE 3 END, port_ret,bench_ret FROM p) SELECT period,sort_order, round(avg(port_ret)*252*100,1) fund_return, round(stddev(port_ret)*sqrt(252)*100,1) fund_volatility, round(avg(port_ret)/stddev(port_ret)*sqrt(252),2) fund_sharpe, round(avg(bench_ret)*252*100,1) bench_return, round(stddev(bench_ret)*sqrt(252)*100,1) bench_volatility, round(avg(bench_ret)/stddev(bench_ret)*sqrt(252),2) bench_sharpe FROM agg GROUP BY period,sort_order ORDER BY sort_order"""
    ],
    [
      """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_metrics WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: {{CATALOG}}.{{SCHEMA}}.holdings_enriched
comment: "AI Growth Fund holdings metrics"
dimensions:
  - name: Portfolio
    expr: portfolio_name
  - name: Ticker
    expr: ticker
  - name: Company
    expr: company
  - name: Sector
    expr: sector
  - name: AI Exposure
    expr: ai_exposure
  - name: Country
    expr: country
measures:
  - name: Total Weight
    expr: SUM(weight_pct)
  - name: Core AI Weight
    expr: SUM(CASE WHEN ai_exposure='Core AI' THEN weight_pct ELSE 0 END)
  - name: Number of Holdings
    expr: COUNT(DISTINCT ticker)
  - name: Avg Market Cap
    expr: AVG(market_cap_b)
  - name: Top Position Weight
    expr: MAX(weight_pct)
$$"""
    ]
  ],
  "genie_rooms": [
    {
      "display_name": "DBDemos - AI/BI - Portfolio Assistant: Concentrated in the AI trade",
      "id": "portfolio-assistant",
      "description": "A wealth manager's flagship AI Growth Fund is beating its benchmark (the Nasdaq-100) by riding the real AI rally — but a series of reorganizations, capped by a major AI pivot on Aug 4 2025, left it dangerously concentrated (~77% core-AI). Walk the story with Genie: see the outperformance, ask WHY (the reorgs that sold defensives and bought AI), watch core-AI concentration step up over time, then quantify the risk with Value at Risk and Sharpe — strong returns, but a large undiversified bet on AI.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.portfolio_performance",
        "{{CATALOG}}.{{SCHEMA}}.holdings_enriched",
        "{{CATALOG}}.{{SCHEMA}}.rebalances",
        "{{CATALOG}}.{{SCHEMA}}.concentration_timeseries",
        "{{CATALOG}}.{{SCHEMA}}.sector_exposure",
        "{{CATALOG}}.{{SCHEMA}}.risk_metrics",
        "{{CATALOG}}.{{SCHEMA}}.var_metrics",
        "{{CATALOG}}.{{SCHEMA}}.sharpe_analysis",
        "{{CATALOG}}.{{SCHEMA}}.news_enriched"
      ],
      "sql_instructions": [
        {
          "title": "How is the AI Growth Fund performing versus its benchmark?",
          "content": """SELECT date, (1 + cum_port_ret) * 100 AS fund_indexed, (1 + cum_bench_ret) * 100 AS benchmark_indexed
FROM `{{CATALOG}}`.`{{SCHEMA}}`.portfolio_performance
WHERE portfolio_id = 1 ORDER BY date;"""
        },
        {
          "title": "Why is the fund outperforming? The reorgs that bought AI and sold defensives",
          "content": """SELECT rebalance_date, rationale, ticker, action, old_weight, new_weight, weight_change
FROM `{{CATALOG}}`.`{{SCHEMA}}`.rebalances
ORDER BY rebalance_date, abs(weight_change) DESC;"""
        },
        {
          "title": "Core-AI concentration stepping up over time",
          "content": """SELECT date, core_ai_pct, ai_adjacent_pct, non_ai_pct
FROM `{{CATALOG}}`.`{{SCHEMA}}`.concentration_timeseries
WHERE portfolio_id = 1 ORDER BY date;"""
        },
        {
          "title": "Did the AI pivot increase risk? Value at Risk and Sharpe before vs after",
          "content": """SELECT v.regime, v.confidence, v.var_pct, v.var_dollar, r.period, r.fund_sharpe, r.fund_volatility, r.bench_sharpe
FROM `{{CATALOG}}`.`{{SCHEMA}}`.var_metrics v
FULL OUTER JOIN `{{CATALOG}}`.`{{SCHEMA}}`.risk_metrics r ON 1=1
ORDER BY v.confidence, r.sort_order;"""
        },
        {
          "title": "Which holdings carry the most risk for the least return?",
          "content": """SELECT ticker, company, ai_exposure, annualized_return, annualized_risk, sharpe_ratio, fund_weight
FROM `{{CATALOG}}`.`{{SCHEMA}}`.sharpe_analysis
WHERE fund_weight IS NOT NULL ORDER BY sharpe_ratio ASC;"""
        }
      ],
      "instructions": "This is a wealth manager's flagship AI Growth Fund (portfolio_id = 1). It is BEATING its benchmark (the Nasdaq-100, QQQ) by riding the real AI rally, but it has become dangerously concentrated in AI names. There is no crash or bubble in the data; the point is that strong returns came with rising, structural risk. The multi-step story: (1) PERFORMANCE - portfolio_performance (portfolio_id=1): cum_port_ret vs cum_bench_ret, index to 100 via (1+cum_ret)*100. (2) WHY - the manager ran three portfolio REORGANIZATIONS that progressively bought AI and sold defensives, capped by a MAJOR AI PIVOT on 2025-08-04 that sold every defensive (JNJ, PG, KO, XOM, JPM...) and concentrated the book in core-AI names; use the rebalances table (rationale labels each reorg; action is Buy/Add/Trim/Sell; weight_change is the delta). (3) CONCENTRATION - core-AI weight stepped up ~30% to ~46% to ~77%, jumping at the August pivot; use concentration_timeseries (core_ai_pct, ai_adjacent_pct, non_ai_pct per day) or holdings_enriched. The fund is ~87% Technology vs ~34% in the benchmark - see sector_exposure (fund_weight, benchmark_weight, overweight). (4) RISK - var_metrics holds 1-day historical Value at Risk on a $100M notional by confidence (0.95, 0.99) and regime ('Post-pivot (current risk)' vs 'Full history'); post-pivot VaR is materially higher. risk_metrics holds annualized return, volatility and SHARPE for fund and benchmark by period ('Overall', 'Before pivot', 'After pivot'): after the pivot volatility rose (~23% to ~29%) but returns rose faster so Sharpe is strong (~2.1, above the benchmark) - the bet has paid off so far; the caution is the large undiversified ~77% core-AI concentration. (5) PER-HOLDING RISK - sharpe_analysis gives each security's annualized_return, annualized_risk and sharpe_ratio; fund_weight is non-null only for current AI Growth Fund holdings; SMCI is the cautionary case (huge volatility, low Sharpe). (6) NEWS - news_enriched joins headlines to tickers with a sentiment score (-1..1) and sentiment_label; AI names skew positive. ai_exposure is one of 'Core AI', 'AI-adjacent', 'Non-AI'. The major pivot date is 2025-08-04. weight_pct sums to ~100 per portfolio. Always filter portfolio_id = 1 for the AI Growth Fund unless the user asks about the Balanced Fund (2) or Income Fund (3). Don't answer questions unrelated to portfolio / investment analysis.",
      "curated_questions": [
        "How is the AI Growth Fund performing versus its benchmark?",
        "Why is the fund beating the benchmark — what drove the outperformance?",
        "Show the portfolio reorganizations — what did we sell and buy at the major AI pivot on Aug 4 2025?",
        "How concentrated is the fund in AI, and how did that change over time?",
        "What is our 95% and 99% 1-day Value at Risk, and did the AI pivot increase it?",
        "Which holdings carry the most risk for the least return?"
      ]
    }
  ]
}

# COMMAND ----------


