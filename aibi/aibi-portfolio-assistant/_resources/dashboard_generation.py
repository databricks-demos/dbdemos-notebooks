#!/usr/bin/env python3
"""Generate the AI Growth Fund portfolio-assistant Lakeview dashboard JSON.
Rebuilt as a richer 2-page dashboard:
  Page 1 "Performance & risk" (home): KPI counters WITH sparklines, hero return line,
         VaR counters, risk-vs-return scatter, sector pie.
  Page 2 "Concentration risk": reorg table, concentration step chart, Sharpe before/after,
         returns histogram, sector bar, AI pie, news.
All table refs fully qualified so update/install retarget cleanly.
"""
import json

S = "main.dbdemos_aibi_fsi_portfolio_assistant_v2"

# ---- palette (coherent, cross-hue; semantics pinned) ----
BLUE   = "#2E6CB5"   # fund / primary
TEAL   = "#3FA7B8"   # secondary series
GREEN  = "#5CBE8A"   # good / non-AI
AMBER  = "#E8B54E"   # event marker / AI-adjacent
CORAL  = "#E0795A"   # alert / core-AI / risk
CORAL2 = "#C9483A"   # intense alert (99% VaR)
GREY   = "#9AA7B4"   # benchmark / neutral
VIOLET = "#9B6A9E"

VIZ = [BLUE, TEAL, GREEN, AMBER, CORAL, VIOLET, "#6E8FD8", "#9CC6A6"]

def q(name, disp, lines):
    return {"name": name, "displayName": disp, "queryLines": lines}

# ============================= DATASETS =============================
datasets = [
    {"name": "ds_metrics", "displayName": "Portfolio holdings metrics",
     "asset_name": f"{S}.portfolio_metrics"},

    # --- sparkline KPI datasets (one row per week; headline = latest via max_by) ---
    q("ds_kpi_return", "Return trend",
      [f"SELECT CAST(date_trunc('week', date) AS DATE) AS week,\n",
       f"  max_by(cum_port_ret * 100, date) AS return_pct\n",
       f"FROM {S}.portfolio_performance WHERE portfolio_id = 1\n",
       f"GROUP BY 1 ORDER BY 1"]),
    q("ds_kpi_outperf", "Outperformance trend",
      [f"SELECT CAST(date_trunc('week', date) AS DATE) AS week,\n",
       f"  max_by((cum_port_ret - cum_bench_ret) * 100, date) AS outperf_pts\n",
       f"FROM {S}.portfolio_performance WHERE portfolio_id = 1\n",
       f"GROUP BY 1 ORDER BY 1"]),
    q("ds_kpi_conc", "Concentration trend",
      [f"SELECT CAST(date_trunc('week', date) AS DATE) AS week,\n",
       f"  max_by(core_ai_pct / 100.0, date) AS core_ai\n",
       f"FROM {S}.concentration_timeseries WHERE portfolio_id = 1\n",
       f"GROUP BY 1 ORDER BY 1"]),
    q("ds_kpi_holdings", "Holdings count",
      [f"SELECT count(*) AS holdings FROM {S}.holdings_enriched WHERE portfolio_id = 1"]),

    # --- VaR counters ---
    q("ds_var95", "VaR 95% post-pivot",
      [f"SELECT var_dollar, var_pct FROM {S}.var_metrics WHERE regime='Post-pivot (current risk)' AND confidence=0.95"]),
    q("ds_var99", "VaR 99% post-pivot",
      [f"SELECT var_dollar, var_pct FROM {S}.var_metrics WHERE regime='Post-pivot (current risk)' AND confidence=0.99"]),

    # --- hero performance line ---
    q("ds_perf", "AI Growth Fund vs benchmark (indexed)",
      [f"SELECT date,\n",
       f"  (1 + cum_port_ret) * 100 AS portfolio,\n",
       f"  (1 + cum_bench_ret) * 100 AS benchmark\n",
       f"FROM {S}.portfolio_performance WHERE portfolio_id = 1 ORDER BY date"]),

    # --- risk-vs-return scatter ---
    q("ds_sharpe_scatter", "Risk vs return by holding",
      [f"SELECT ticker, company, sector, ai_exposure,\n",
       f"  annualized_return, annualized_risk, sharpe_ratio,\n",
       f"  coalesce(fund_weight, 0) AS fund_weight\n",
       f"FROM {S}.sharpe_analysis WHERE fund_weight IS NOT NULL ORDER BY sharpe_ratio DESC"]),

    # --- concentration page datasets ---
    q("ds_conc", "Core-AI concentration over time (step)",
      [f"SELECT CAST(date_trunc('week', date) AS DATE) AS week,\n",
       f"  round(avg(core_ai_pct), 1) AS core_ai_pct,\n",
       f"  round(avg(core_ai_pct + ai_adjacent_pct), 1) AS ai_exposed_pct\n",
       f"FROM {S}.concentration_timeseries WHERE portfolio_id = 1 GROUP BY 1 ORDER BY week"]),
    q("ds_rebal", "Portfolio reorganizations",
      [f"SELECT r.rebalance_date, r.ticker, s.company, r.action,\n",
       f"  r.old_weight, r.new_weight, r.weight_change, r.rationale\n",
       f"FROM {S}.rebalances r JOIN {S}.securities s ON s.ticker = r.ticker\n",
       f"ORDER BY r.rebalance_date DESC, abs(r.weight_change) DESC"]),
    q("ds_risk", "Risk-adjusted return — fund vs benchmark",
      [f"SELECT period, sort_order, fund_sharpe, fund_volatility, fund_return,\n",
       f"  bench_sharpe, bench_volatility, bench_return\n",
       f"FROM {S}.risk_metrics ORDER BY sort_order"]),
    q("ds_alloc", "Allocation: fund vs benchmark by sector",
      [f"SELECT sector, fund_weight * 100 AS portfolio_weight, benchmark_weight * 100 AS benchmark_weight\n",
       f"FROM {S}.sector_exposure ORDER BY portfolio_weight DESC"]),
    q("ds_dist", "Daily return distribution",
      [f"SELECT return_bucket_pct, days,\n",
       f"  CASE WHEN return_bucket_pct < 0 THEN 'Down day' ELSE 'Up day' END AS direction\n",
       f"FROM {S}.returns_distribution ORDER BY return_bucket_pct"]),
    q("ds_news", "News sentiment on AI holdings",
      [f"SELECT n.published_date, n.source, n.title, n.sentiment_label, n.sentiment, ne.ticker\n",
       f"FROM {S}.news_enriched ne JOIN {S}.news n ON n.article_id = ne.article_id\n",
       f"WHERE ne.ai_exposure = 'Core AI' ORDER BY n.published_date DESC LIMIT 50"]),
]

# ============================= WIDGET HELPERS =============================
def text(name, lines, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [lines]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def counter_spark(name, ds, value_field, period_field, title, disp, x, y,
                  fmt=None, tmpl=None, color=None, w=3, h=3):
    enc = {"value": {"fieldName": value_field, "displayName": disp},
           "period": {"fieldName": period_field}}
    if fmt: enc["value"]["format"] = fmt
    if tmpl: enc["value"]["formatTemplate"] = tmpl
    if color: enc["value"]["color"] = {"default": color}
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds,
            "fields": [{"name": value_field, "expression": f"`{value_field}`"},
                       {"name": period_field, "expression": f"`{period_field}`"}],
            "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def counter_plain(name, ds, field, title, disp, x, y, fmt=None, tmpl=None, color=None, w=3, h=3):
    enc = {"value": {"fieldName": field, "displayName": disp}}
    if fmt: enc["value"]["format"] = fmt
    if tmpl: enc["value"]["formatTemplate"] = tmpl
    if color: enc["value"]["color"] = {"default": color}
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds,
            "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

NUM0 = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 0}}
NUM1 = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}}
NUM2 = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 2}}
PCT0 = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 0}}

# ============================= PAGE 1: Performance & risk =============================
p1 = []
p1.append(text("title",
    "# AI Growth Fund — winning, but how exposed?\n\nOur flagship fund is beating its benchmark by riding the AI rally. The catch: a series of reorgs piled it into AI names — and one major pivot took core-AI exposure from ~30% to ~80%. Great returns, but how much risk are we carrying if the AI trade turns?",
    0, 0, 12, 2))

# KPI row 1 — performance (with sparklines)
p1.append(counter_spark("kpi_return", "ds_kpi_return", "return_pct", "week",
    "Fund total return", "Total return", 0, 2, NUM0, "+{{ @formatted }}%", BLUE))
p1.append(counter_spark("kpi_outperf", "ds_kpi_outperf", "outperf_pts", "week",
    "Outperformance vs benchmark", "vs benchmark", 3, 2, NUM0, "+{{ @formatted }} pts", GREEN))
p1.append(counter_spark("kpi_conc", "ds_kpi_conc", "core_ai", "week",
    "Core AI concentration", "Core AI weight", 6, 2, PCT0, None, CORAL))
p1.append(counter_plain("kpi_holdings", "ds_kpi_holdings", "holdings",
    "Number of holdings", "Holdings", 9, 2, NUM0, None, None))

# hero performance line (no forecast, pivot marker)
p1.append({"widget": {"name": "hero_perf",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_perf",
        "fields": [{"name": "date", "expression": "`date`"},
                   {"name": "portfolio", "expression": "`portfolio`"},
                   {"name": "benchmark", "expression": "`benchmark`"}],
        "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "line", "encodings": {
        "x": {"fieldName": "date", "scale": {"type": "temporal"}, "displayName": "Date"},
        "y": {"scale": {"type": "quantitative"},
              "fields": [{"fieldName": "portfolio", "displayName": "AI Growth Fund"},
                         {"fieldName": "benchmark", "displayName": "Benchmark (QQQ)"}]},
        "color": {"scale": {"type": "categorical", "mappings": [
            {"value": "AI Growth Fund", "color": BLUE},
            {"value": "Benchmark (QQQ)", "color": GREY}]}}},
        "annotations": [{"type": "vertical-line", "encodings": {
            "x": {"dataValue": "2025-08-04T00:00:00.000", "dataType": "DATETIME"},
            "label": {"value": "Major AI pivot"}, "color": {"value": AMBER}}}],
        "frame": {"showTitle": True, "title": "Cumulative return — AI Growth Fund vs benchmark (indexed to 100)"}}},
    "position": {"x": 0, "y": 5, "width": 8, "height": 6}})

# sector pie
p1.append({"widget": {"name": "sector_pie",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_metrics",
        "fields": [{"name": "Sector", "expression": "`Sector`"},
                   {"name": "measure(Total Weight)", "expression": "MEASURE(`Total Weight`)"}],
        "disaggregated": False}}],
    "spec": {"version": 3, "widgetType": "pie", "encodings": {
        "angle": {"fieldName": "measure(Total Weight)", "scale": {"type": "quantitative"}, "displayName": "Weight"},
        "color": {"fieldName": "Sector", "scale": {"type": "categorical"}, "displayName": "Sector"},
        "label": {"show": True}},
        "frame": {"showTitle": True, "title": "Fund allocation by sector"}}},
    "position": {"x": 8, "y": 5, "width": 4, "height": 6}})

# --- risk band ---
p1.append(text("risk_hdr",
    "## How much could a bad day cost?\n\n**Value at Risk** puts a dollar figure on the downside (on a **$100M** book): on 95% of days the fund won't lose more than the figure below — but concentrating into AI pushed that number up. The chart shows which holdings actually reward their risk: **up** = more return, **right** = more risk, **size** = position weight. Best is top-left; the fund's AI names (coral) sit to the right.",
    0, 11, 12, 2))

p1.append(counter_plain("var95", "ds_var95", "var_dollar",
    "95% 1-day Value at Risk", "Max daily loss (95%)", 0, 13,
    {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}},
    "-{{ @formatted }}", CORAL))
p1.append(counter_plain("var99", "ds_var99", "var_dollar",
    "99% 1-day Value at Risk", "Max daily loss (99%)", 0, 16,
    {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}},
    "-{{ @formatted }}", CORAL2))

# risk-vs-return scatter (right of VaR counters)
p1.append({"widget": {"name": "sharpe_scatter",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_sharpe_scatter",
        "fields": [{"name": "annualized_risk", "expression": "`annualized_risk`"},
                   {"name": "annualized_return", "expression": "`annualized_return`"},
                   {"name": "ai_exposure", "expression": "`ai_exposure`"},
                   {"name": "ticker", "expression": "`ticker`"},
                   {"name": "company", "expression": "`company`"},
                   {"name": "fund_weight", "expression": "`fund_weight`"},
                   {"name": "sharpe_ratio", "expression": "`sharpe_ratio`"}],
        "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "scatter", "encodings": {
        "x": {"fieldName": "annualized_risk", "scale": {"type": "quantitative", "domainMin": 0},
              "displayName": "← safer        Risk / volatility (%)        riskier →"},
        "y": {"fieldName": "annualized_return", "scale": {"type": "quantitative"},
              "displayName": "↑ better        Return (%/yr)"},
        "color": {"fieldName": "ai_exposure", "scale": {"type": "categorical", "mappings": [
            {"value": "Core AI", "color": CORAL},
            {"value": "AI-adjacent", "color": AMBER},
            {"value": "Non-AI", "color": GREEN}]}, "displayName": "AI exposure"},
        "size": {"fieldName": "fund_weight", "scale": {"type": "quantitative"}, "displayName": "Fund weight %"},
        "label": {"show": True, "fieldName": "ticker"},
        "extra": [{"fieldName": "company", "displayName": "Company"},
                  {"fieldName": "ticker", "displayName": "Ticker"},
                  {"fieldName": "ai_exposure", "displayName": "AI exposure"},
                  {"fieldName": "annualized_return", "displayName": "Return (%/yr)"},
                  {"fieldName": "annualized_risk", "displayName": "Risk / volatility (%)"},
                  {"fieldName": "sharpe_ratio", "displayName": "Sharpe ratio"},
                  {"fieldName": "fund_weight", "displayName": "Fund weight (%)"}]},
        "frame": {"showTitle": True, "title": "Risk vs return by holding — each bubble is a stock (hover for details)"}}},
    "position": {"x": 3, "y": 13, "width": 9, "height": 6}})

# holdings table
p1.append(text("holdings_hdr", "## Holdings", 0, 19, 12, 1))
p1.append({"widget": {"name": "holdings_table",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_metrics",
        "fields": [{"name": "Company", "expression": "`Company`"},
                   {"name": "Ticker", "expression": "`Ticker`"},
                   {"name": "Sector", "expression": "`Sector`"},
                   {"name": "AI Exposure", "expression": "`AI Exposure`"},
                   {"name": "measure(Total Weight)", "expression": "MEASURE(`Total Weight`)"}],
        "disaggregated": False}}],
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": "Company", "displayName": "Company"},
        {"fieldName": "Ticker", "displayName": "Ticker"},
        {"fieldName": "Sector", "displayName": "Sector"},
        {"fieldName": "AI Exposure", "displayName": "AI Exposure"},
        {"fieldName": "measure(Total Weight)", "displayName": "Weight %", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}}]},
        "frame": {"showTitle": True, "title": "Holdings"}}},
    "position": {"x": 0, "y": 20, "width": 12, "height": 6}})

# ============================= PAGE 2: Concentration risk =============================
p2 = []
p2.append(text("c_title",
    "# Why the strong returns come with risk\n\nThe outperformance is concentrated in AI. A series of reorgs — capped by one major pivot — sold the fund's defensives and bought AI, taking core-AI exposure from ~30% to ~80%. That single decision drove the returns and the risk at the same time.",
    0, 0, 12, 2))

p2.append(text("c_step1",
    "## 1. The reorgs that piled into AI\n\nThree portfolio reorganizations, ending with a major pivot on **Aug 4, 2025** that sold every defensive (JNJ, PG, KO, XOM, JPM) and concentrated the book in core-AI names.",
    0, 2, 12, 2))
p2.append({"widget": {"name": "rebal_table",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_rebal",
        "fields": [{"name": c, "expression": f"`{c}`"} for c in
                   ["rebalance_date", "company", "ticker", "action", "old_weight", "new_weight", "weight_change", "rationale"]],
        "disaggregated": True}}],
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": "rebalance_date", "displayName": "Date"},
        {"fieldName": "company", "displayName": "Company"},
        {"fieldName": "ticker", "displayName": "Ticker"},
        {"fieldName": "action", "displayName": "Action"},
        {"fieldName": "old_weight", "displayName": "Old %", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}},
        {"fieldName": "new_weight", "displayName": "New %", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}},
        {"fieldName": "weight_change", "displayName": "Change", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 0}}},
        {"fieldName": "rationale", "displayName": "Reorg"}]},
        "frame": {"showTitle": True, "title": "Portfolio reorganizations — what was sold and bought"}}},
    "position": {"x": 0, "y": 4, "width": 12, "height": 6}})

p2.append(text("c_step2",
    "## 2. Concentration stepped up at the reorg\n\nCore-AI exposure was steady near 30%, drifted up with two tilts, then **jumped to ~80% at the August pivot** — and has stayed there. The risk is now structural, not incidental.",
    0, 10, 12, 2))
p2.append({"widget": {"name": "conc_step",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_conc",
        "fields": [{"name": "week", "expression": "`week`"},
                   {"name": "core_ai_pct", "expression": "`core_ai_pct`"},
                   {"name": "ai_exposed_pct", "expression": "`ai_exposed_pct`"}],
        "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "line", "encodings": {
        "x": {"fieldName": "week", "scale": {"type": "temporal"}, "displayName": "Week"},
        "y": {"scale": {"type": "quantitative", "domainMin": 0},
              "fields": [{"fieldName": "ai_exposed_pct", "displayName": "AI-exposed %"},
                         {"fieldName": "core_ai_pct", "displayName": "Core AI %"}]},
        "color": {"scale": {"type": "categorical", "mappings": [
            {"value": "AI-exposed %", "color": TEAL},
            {"value": "Core AI %", "color": CORAL}]}}},
        "annotations": [{"type": "vertical-line", "encodings": {
            "x": {"dataValue": "2025-08-04T00:00:00.000", "dataType": "DATETIME"},
            "label": {"value": "Major AI pivot"}, "color": {"value": AMBER}}}],
        "frame": {"showTitle": True, "title": "Core-AI concentration over time — jumps from ~30% to ~80% at the August pivot"}}},
    "position": {"x": 0, "y": 12, "width": 12, "height": 6}})

p2.append(text("c_step3",
    "## 3. Is the risk paying off — so far? (Sharpe)\n\nThe bet has worked *to date*: after the major pivot the fund's volatility **rose** (from ~23% to ~29%), but returns rose even faster, so its Sharpe is strong (~2.1, above the benchmark's ~1.76). The caution: that reward rides on a single, undiversified ~77% core-AI position — the higher volatility and Value-at-Risk are the price of it, and they bite hard if the AI trade turns.",
    0, 18, 12, 2))
p2.append({"widget": {"name": "risk_table",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_risk",
        "fields": [{"name": c, "expression": f"`{c}`"} for c in
                   ["period", "fund_return", "fund_volatility", "fund_sharpe", "bench_volatility", "bench_sharpe"]],
        "disaggregated": True}}],
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": "period", "displayName": "Period"},
        {"fieldName": "fund_return", "displayName": "Fund return (ann.) %", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}},
        {"fieldName": "fund_volatility", "displayName": "Fund vol %", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}},
        {"fieldName": "fund_sharpe", "displayName": "Fund Sharpe", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 2}}},
        {"fieldName": "bench_volatility", "displayName": "Benchmark vol %", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}},
        {"fieldName": "bench_sharpe", "displayName": "Benchmark Sharpe", "format": {"type": "number", "decimalPlaces": {"type": "max", "places": 2}}}]},
        "frame": {"showTitle": True, "title": "Risk-adjusted return — before vs after the major pivot"}}},
    "position": {"x": 0, "y": 20, "width": 7, "height": 5}})
# returns histogram beside the Sharpe table
p2.append({"widget": {"name": "returns_hist",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_dist",
        "fields": [{"name": "return_bucket_pct", "expression": "`return_bucket_pct`"},
                   {"name": "days", "expression": "`days`"},
                   {"name": "direction", "expression": "`direction`"}],
        "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "bar", "encodings": {
        "x": {"fieldName": "return_bucket_pct", "scale": {"type": "quantitative"}, "displayName": "Daily return %"},
        "y": {"fieldName": "days", "scale": {"type": "quantitative"}, "displayName": "# days"},
        "color": {"fieldName": "direction", "scale": {"type": "categorical", "mappings": [
            {"value": "Down day", "color": CORAL},
            {"value": "Up day", "color": GREEN}]}, "displayName": ""}},
        "frame": {"showTitle": True, "title": "Daily return distribution (post-pivot) — the downside tail"}}},
    "position": {"x": 7, "y": 20, "width": 5, "height": 5}})

p2.append(text("c_step4",
    "## 4. The fund is far more exposed to Technology than the benchmark\n\n~87% Technology vs ~34% in the benchmark, and over half the book in core-AI names — a turn in the AI trade would hit it disproportionately.",
    0, 25, 12, 2))
p2.append({"widget": {"name": "alloc_bar",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_alloc",
        "fields": [{"name": "sector", "expression": "`sector`"},
                   {"name": "portfolio_weight", "expression": "`portfolio_weight`"},
                   {"name": "benchmark_weight", "expression": "`benchmark_weight`"}],
        "disaggregated": True}}],
    "spec": {"version": 3, "widgetType": "bar", "encodings": {
        "y": {"fieldName": "sector", "scale": {"type": "categorical"}, "displayName": "Sector"},
        "x": {"scale": {"type": "quantitative"},
              "fields": [{"fieldName": "portfolio_weight", "displayName": "Fund weight %"},
                         {"fieldName": "benchmark_weight", "displayName": "Benchmark weight %"}]},
        "color": {"scale": {"type": "categorical", "mappings": [
            {"value": "Fund weight %", "color": CORAL},
            {"value": "Benchmark weight %", "color": GREY}]}},
        "mark": {"layout": "group"}},
        "frame": {"showTitle": True, "title": "Sector allocation — fund vs benchmark (Technology is the overweight)"}}},
    "position": {"x": 0, "y": 27, "width": 8, "height": 6}})
p2.append({"widget": {"name": "ai_pie",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_metrics",
        "fields": [{"name": "AI Exposure", "expression": "`AI Exposure`"},
                   {"name": "measure(Total Weight)", "expression": "MEASURE(`Total Weight`)"}],
        "disaggregated": False}}],
    "spec": {"version": 3, "widgetType": "pie", "encodings": {
        "angle": {"fieldName": "measure(Total Weight)", "scale": {"type": "quantitative"}, "displayName": "Weight"},
        "color": {"fieldName": "AI Exposure", "scale": {"type": "categorical", "mappings": [
            {"value": "Core AI", "color": CORAL},
            {"value": "AI-adjacent", "color": AMBER},
            {"value": "Non-AI", "color": GREEN}]}, "displayName": "AI Exposure"},
        "label": {"show": True}},
        "frame": {"showTitle": True, "title": "Weight by AI exposure — 80% core AI"}}},
    "position": {"x": 8, "y": 27, "width": 4, "height": 6}})

p2.append(text("c_step5",
    "## 5. The news flow on those AI names\n\nRecent sentiment on the fund's core-AI holdings — the qualitative signal a manager watches for an early turn.",
    0, 33, 12, 2))
p2.append({"widget": {"name": "news_table",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_news",
        "fields": [{"name": c, "expression": f"`{c}`"} for c in
                   ["published_date", "ticker", "source", "title", "sentiment_label"]],
        "disaggregated": True}}],
    "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [
        {"fieldName": "published_date", "displayName": "Date"},
        {"fieldName": "ticker", "displayName": "Ticker"},
        {"fieldName": "source", "displayName": "Source"},
        {"fieldName": "title", "displayName": "Headline"},
        {"fieldName": "sentiment_label", "displayName": "Sentiment"}]},
        "frame": {"showTitle": True, "title": "Latest news on core-AI holdings"}}},
    "position": {"x": 0, "y": 35, "width": 12, "height": 6}})

# ============================= FILTERS =============================
def filt(name, qn, field, x, single=False, default=None):
    spec = {"version": 2, "widgetType": "filter-single-select" if single else "filter-multi-select",
            "encodings": {"fields": [{"fieldName": field, "displayName": field, "queryName": qn}]},
            "frame": {"showTitle": True, "title": field}}
    if default:
        spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": default}]}}}
    return {"widget": {"name": name,
        "queries": [{"name": qn, "query": {"datasetName": "ds_metrics",
            "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": False}}],
        "spec": spec}, "position": {"x": x, "y": 0, "width": 3, "height": 1}}

filters = [
    filt("f_portfolio", "f_p_q", "Portfolio", 0, single=True, default="AI Growth Fund"),
    filt("f_sector", "f_s_q", "Sector", 3),
    filt("f_ai", "f_ai_q", "AI Exposure", 6),
]

dash = {
    "datasets": datasets,
    "pages": [
        {"name": "performance", "displayName": "Performance & risk", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p1},
        {"name": "concentration", "displayName": "Concentration risk", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": p2},
        {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1", "layout": filters},
    ],
    "uiSettings": {
        "theme": {
            "canvasBackgroundColor": {"light": "#FCFCFC", "dark": "#1F272D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
            "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
            "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
            "visualizationColors": VIZ,
            "widgetHeaderAlignment": "LEFT",
        },
        "genieSpace": {"isEnabled": True, "overrideId": "", "enablementMode": "ENABLED"},
    },
}

# ---- overlap + row-fill validation ----
for p in dash["pages"]:
    if p["pageType"] != "PAGE_TYPE_CANVAS":
        continue
    cells = {}
    for w in p["layout"]:
        po = w["position"]
        for yy in range(po["y"], po["y"] + po["height"]):
            for xx in range(po["x"], po["x"] + po["width"]):
                assert (yy, xx) not in cells, f"OVERLAP {p['name']} {(yy,xx)} {cells.get((yy,xx))} vs {w['widget']['name']}"
                cells[(yy, xx)] = w["widget"]["name"]

out = "aibi/aibi-portfolio-assistant/_build_v2/dashboard.json"
json.dump(dash, open(out, "w"), indent=2)
print("OK — written", out)
print("pages:", [p["displayName"] for p in dash["pages"]], "datasets:", len(datasets))
