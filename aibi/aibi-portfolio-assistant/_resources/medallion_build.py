#!/usr/bin/env python3
"""Rebuild all medallion gold tables for the portfolio-assistant demo from raw parquet."""
import json, subprocess, sys

WH = "9d8a677b3c55b8a7"
S = "main.dbdemos_aibi_fsi_portfolio_assistant_v2"
V = "/Volumes/main/dbdemos_aibi_fsi_portfolio_assistant_v2/raw_data"

def run(sql, label):
    r = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--json", json.dumps({"warehouse_id": WH, "statement": sql})],
        capture_output=True, text=True, env={**__import__("os").environ, "DATABRICKS_CONFIG_PROFILE": "DEFAULT"})
    try:
        d = json.loads(r.stdout)
        st = d["status"]["state"]
        err = d["status"].get("error")
        msg = (err.get("message") if isinstance(err, dict) else err) or ""
        print(f"  {label}: {st} {msg}")
        if st == "FAILED":
            sys.exit(1)
    except Exception:
        print(f"  {label}: ERROR", r.stdout[:200], r.stderr[:200]); sys.exit(1)

for t in ["securities","sectors","portfolios","holdings","rebalances","prices","benchmark","news","news_ticker"]:
    run(f"CREATE OR REPLACE TABLE {S}.{t} AS SELECT * FROM parquet.`{V}/{t}`", t)
print("raw reloaded")

run(f"CREATE OR REPLACE VIEW {S}.holdings_asof AS WITH base AS (SELECT h.portfolio_id,h.ticker,h.effective_date,h.weight_pct,h.shares,s.ai_exposure,s.sector,s.company FROM {S}.holdings h JOIN {S}.securities s ON h.ticker=s.ticker) SELECT *, lead(effective_date) OVER (PARTITION BY portfolio_id,ticker ORDER BY effective_date) AS next_eff FROM base", "holdings_asof")

run(f"CREATE OR REPLACE TABLE {S}.portfolio_performance AS WITH px AS (SELECT ticker,date,close,close/lag(close) OVER (PARTITION BY ticker ORDER BY date)-1 AS daily_ret FROM {S}.prices), asof AS (SELECT portfolio_id,ticker,weight_pct,effective_date,next_eff FROM {S}.holdings_asof), pd AS (SELECT a.portfolio_id,px.date,SUM(px.daily_ret*a.weight_pct/100.0) port_ret FROM px JOIN asof a ON px.ticker=a.ticker AND px.date>=a.effective_date AND (a.next_eff IS NULL OR px.date<a.next_eff) WHERE px.daily_ret IS NOT NULL GROUP BY a.portfolio_id,px.date), bench AS (SELECT date,close/lag(close) OVER (ORDER BY date)-1 AS bench_ret FROM {S}.benchmark), j AS (SELECT p.portfolio_id,p.date,p.port_ret,b.bench_ret FROM pd p JOIN bench b ON p.date=b.date WHERE b.bench_ret IS NOT NULL) SELECT portfolio_id,date,port_ret,bench_ret, exp(sum(ln(1+port_ret)) OVER (PARTITION BY portfolio_id ORDER BY date))-1 cum_port_ret, exp(sum(ln(1+bench_ret)) OVER (PARTITION BY portfolio_id ORDER BY date))-1 cum_bench_ret FROM j", "portfolio_performance")

run(f"CREATE OR REPLACE TABLE {S}.concentration_timeseries AS WITH d AS (SELECT distinct date FROM {S}.prices), asof AS (SELECT d.date,e.portfolio_id,e.weight_pct,e.ai_exposure FROM d JOIN {S}.holdings_asof e ON d.date>=e.effective_date AND (e.next_eff IS NULL OR d.date<e.next_eff)) SELECT portfolio_id,date, round(sum(CASE WHEN ai_exposure='Core AI' THEN weight_pct ELSE 0 END)/sum(weight_pct)*100,1) core_ai_pct, round(sum(CASE WHEN ai_exposure='AI-adjacent' THEN weight_pct ELSE 0 END)/sum(weight_pct)*100,1) ai_adjacent_pct, round(sum(CASE WHEN ai_exposure='Non-AI' THEN weight_pct ELSE 0 END)/sum(weight_pct)*100,1) non_ai_pct, count(CASE WHEN weight_pct>0 THEN 1 END) num_holdings FROM asof GROUP BY portfolio_id,date", "concentration_timeseries")

run(f"CREATE OR REPLACE TABLE {S}.sector_exposure AS WITH cur AS (SELECT e.sector,sum(e.weight_pct) fund_w FROM {S}.holdings_asof e WHERE e.portfolio_id=1 AND e.next_eff IS NULL AND e.weight_pct>0 GROUP BY e.sector) SELECT coalesce(cur.sector,s.sector) sector, coalesce(cur.fund_w,0)/100.0 fund_weight, s.benchmark_weight, coalesce(cur.fund_w,0)/100.0-s.benchmark_weight overweight FROM {S}.sectors s FULL OUTER JOIN cur ON s.sector=cur.sector", "sector_exposure")

run(f"CREATE OR REPLACE TABLE {S}.holdings_enriched AS SELECT p.portfolio_name,e.portfolio_id,e.ticker,e.company,e.sector,e.ai_exposure,e.weight_pct,sec.market_cap_b,sec.country,sec.industry FROM {S}.holdings_asof e JOIN {S}.portfolios p ON p.portfolio_id=e.portfolio_id JOIN {S}.securities sec ON sec.ticker=e.ticker WHERE e.next_eff IS NULL AND e.weight_pct>0", "holdings_enriched")

run(f"CREATE OR REPLACE TABLE {S}.news_enriched AS SELECT n.*,nt.ticker,s.company,s.ai_exposure,s.sector FROM {S}.news n JOIN {S}.news_ticker nt ON n.article_id=nt.article_id JOIN {S}.securities s ON s.ticker=nt.ticker", "news_enriched")

run(f"CREATE OR REPLACE TABLE {S}.sharpe_analysis AS WITH ret AS (SELECT ticker,date,close/lag(close) OVER (PARTITION BY ticker ORDER BY date)-1 r FROM {S}.prices), stats AS (SELECT ticker,avg(r)*252 ar,stddev(r)*sqrt(252) arisk,avg(r)/nullif(stddev(r),0)*sqrt(252) sr FROM ret WHERE r IS NOT NULL GROUP BY ticker) SELECT s.ticker,sec.company,sec.sector,sec.ai_exposure,sec.market_cap_b,he.weight_pct fund_weight,round(s.ar*100,1) annualized_return,round(s.arisk*100,1) annualized_risk,round(s.sr,2) sharpe_ratio FROM stats s JOIN {S}.securities sec ON sec.ticker=s.ticker LEFT JOIN {S}.holdings_enriched he ON he.ticker=s.ticker AND he.portfolio_id=1", "sharpe_analysis")

run(f"CREATE OR REPLACE TABLE {S}.var_metrics AS WITH r_all AS (SELECT port_ret FROM {S}.portfolio_performance WHERE portfolio_id=1), r_post AS (SELECT port_ret FROM {S}.portfolio_performance WHERE portfolio_id=1 AND date>=date'2025-08-04') SELECT 'Post-pivot (current risk)' regime,1 srt,0.95 confidence,round(percentile(port_ret,0.05)*-100,2) var_pct,round(percentile(port_ret,0.05)*-100000000) var_dollar FROM r_post UNION ALL SELECT 'Post-pivot (current risk)',1,0.99,round(percentile(port_ret,0.01)*-100,2),round(percentile(port_ret,0.01)*-100000000) FROM r_post UNION ALL SELECT 'Full history',2,0.95,round(percentile(port_ret,0.05)*-100,2),round(percentile(port_ret,0.05)*-100000000) FROM r_all UNION ALL SELECT 'Full history',2,0.99,round(percentile(port_ret,0.01)*-100,2),round(percentile(port_ret,0.01)*-100000000) FROM r_all", "var_metrics")

run(f"CREATE OR REPLACE TABLE {S}.returns_distribution AS SELECT round(port_ret*200)/2.0 return_bucket_pct,count(*) days FROM {S}.portfolio_performance WHERE portfolio_id=1 AND date>=date'2025-08-04' GROUP BY 1 ORDER BY 1", "returns_distribution")

run(f"CREATE OR REPLACE TABLE {S}.risk_metrics AS WITH p AS (SELECT date,port_ret,bench_ret FROM {S}.portfolio_performance WHERE portfolio_id=1), agg AS (SELECT 'Overall' period,1 sort_order,port_ret,bench_ret FROM p UNION ALL SELECT CASE WHEN date<date'2025-08-04' THEN 'Before pivot' ELSE 'After pivot' END, CASE WHEN date<date'2025-08-04' THEN 2 ELSE 3 END, port_ret,bench_ret FROM p) SELECT period,sort_order, round(avg(port_ret)*252*100,1) fund_return, round(stddev(port_ret)*sqrt(252)*100,1) fund_volatility, round(avg(port_ret)/stddev(port_ret)*sqrt(252),2) fund_sharpe, round(avg(bench_ret)*252*100,1) bench_return, round(stddev(bench_ret)*sqrt(252)*100,1) bench_volatility, round(avg(bench_ret)/stddev(bench_ret)*sqrt(252),2) bench_sharpe FROM agg GROUP BY period,sort_order ORDER BY sort_order", "risk_metrics")

mv = f"""CREATE OR REPLACE VIEW {S}.portfolio_metrics WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: {S}.holdings_enriched
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
run(mv, "portfolio_metrics")
print("gold rebuild complete")
