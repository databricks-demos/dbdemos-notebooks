#!/usr/bin/env python
"""
AI/BI Portfolio Assistant — synthetic data generation (REAL prices via yfinance, from scratch).

BUSINESS: a wealth / portfolio manager reviews the book. The flagship "AI Growth Fund" has
ridden the AI rally and is OUTPERFORMING its benchmark — but it has become dangerously
CONCENTRATED in AI names (~55% of the book vs ~25% in the benchmark). Two other books
(Balanced, Income) give comparison. The story is RISK, not a crash: great returns, but heavy
exposure if the AI trade turns.

REAL DATA: daily closes are pulled once from Yahoo Finance (yfinance) and snapshotted to
parquet (the demo ships static parquet — it cannot call an API at install time). Holdings,
portfolios, sectors and news/sentiment are synthesized on top.

Tables (raw parquet -> volume; bundle_config builds bronze->silver->enriched + metric view):
  securities    (dim) : ticker, company, sector, industry, ai_exposure, market_cap_b, country
  sectors       (dim) : sector, benchmark_weight  (the index allocation, for concentration compare)
  portfolios    (dim) : portfolio_id, portfolio_name, strategy, manager
  holdings      (fact): portfolio_id, ticker, effective_date, weight_pct, shares  (time-versioned)
  rebalances    (fact): rebalance_date, ticker, action, old_weight, new_weight, weight_change, rationale
  prices        (fact): ticker, date, close, daily_return            <- REAL (yfinance)
  benchmark     (fact): date, close, daily_return                    <- REAL (QQQ)
  news          (dim) : article_id, published_date, source, title, sentiment, sentiment_label
  news_ticker   (link): article_id, ticker

Requires Python 3.12 + databricks-connect (serverless) + yfinance.
Run: DATABRICKS_CONFIG_PROFILE=<profile> python data_generation.py
"""
import datetime as dt
import yfinance as yf
import pandas as pd, numpy as np
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

CATALOG = "main"
SCHEMA = "dbdemos_aibi_fsi_portfolio_assistant_v2"   # build schema; demo default_schema = dbdemos_aibi_fsi_portfolio_assistant
PROFILE = "DEFAULT"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
START = "2024-06-01"
END = "2026-06-10"
BENCHMARK = "QQQ"   # Nasdaq-100 ETF as the benchmark

spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
print(f"== schema + volume {CATALOG}.{SCHEMA} ==")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
def write(df, name, n=None): df.write.mode("overwrite").parquet(f"{VOL}/{name}"); print(f"   {name}: {n if n is not None else df.count()} rows")

# ---- SECURITIES (curated universe; ai_exposure drives the concentration story) ----
# ticker, company, sector, industry, ai_exposure, market_cap_b, country
securities = [
 # --- Core AI ---
 ("NVDA","NVIDIA","Technology","Semiconductors","Core AI",3200,"USA"),
 ("AVGO","Broadcom","Technology","Semiconductors","Core AI",1100,"USA"),
 ("AMD","Advanced Micro Devices","Technology","Semiconductors","Core AI",260,"USA"),
 ("PLTR","Palantir","Technology","Software","Core AI",330,"USA"),
 ("SMCI","Super Micro Computer","Technology","Hardware","Core AI",30,"USA"),
 ("MU","Micron Technology","Technology","Semiconductors","Core AI",130,"USA"),
 ("ANET","Arista Networks","Technology","Networking","Core AI",140,"USA"),
 ("TSM","Taiwan Semiconductor","Technology","Semiconductors","Core AI",900,"Taiwan"),
 ("ORCL","Oracle","Technology","Software","Core AI",450,"USA"),
 ("MRVL","Marvell Technology","Technology","Semiconductors","Core AI",90,"USA"),
 ("DELL","Dell Technologies","Technology","Hardware","Core AI",95,"USA"),
 ("SNOW","Snowflake","Technology","Software","Core AI",60,"USA"),
 ("CRWD","CrowdStrike","Technology","Software","Core AI",90,"USA"),
 ("VRT","Vertiv Holdings","Industrials","Electrical Equipment","Core AI",45,"USA"),
 ("ASML","ASML Holding","Technology","Semiconductors","Core AI",380,"Netherlands"),
 # --- AI-adjacent ---
 ("MSFT","Microsoft","Technology","Software","AI-adjacent",3300,"USA"),
 ("GOOGL","Alphabet","Communication Services","Internet","AI-adjacent",2300,"USA"),
 ("META","Meta Platforms","Communication Services","Internet","AI-adjacent",1500,"USA"),
 ("AMZN","Amazon","Consumer Discretionary","Internet Retail","AI-adjacent",2100,"USA"),
 ("CRM","Salesforce","Technology","Software","AI-adjacent",260,"USA"),
 ("AAPL","Apple","Technology","Consumer Electronics","AI-adjacent",3400,"USA"),
 ("NOW","ServiceNow","Technology","Software","AI-adjacent",190,"USA"),
 ("ADBE","Adobe","Technology","Software","AI-adjacent",230,"USA"),
 ("TSLA","Tesla","Consumer Discretionary","Autos","AI-adjacent",800,"USA"),
 ("QCOM","Qualcomm","Technology","Semiconductors","AI-adjacent",190,"USA"),
 ("INTC","Intel","Technology","Semiconductors","AI-adjacent",95,"USA"),
 # --- Non-AI ---
 ("JNJ","Johnson & Johnson","Healthcare","Pharmaceuticals","Non-AI",380,"USA"),
 ("PG","Procter & Gamble","Consumer Staples","Household Products","Non-AI",390,"USA"),
 ("KO","Coca-Cola","Consumer Staples","Beverages","Non-AI",290,"USA"),
 ("XOM","Exxon Mobil","Energy","Oil & Gas","Non-AI",470,"USA"),
 ("JPM","JPMorgan Chase","Financials","Banks","Non-AI",640,"USA"),
 ("UNH","UnitedHealth Group","Healthcare","Managed Care","Non-AI",520,"USA"),
 ("V","Visa","Financials","Payments","Non-AI",560,"USA"),
 ("WMT","Walmart","Consumer Staples","Retail","Non-AI",560,"USA"),
 ("CVX","Chevron","Energy","Oil & Gas","Non-AI",280,"USA"),
 ("HD","Home Depot","Consumer Discretionary","Home Improvement","Non-AI",380,"USA"),
 ("MCD","McDonald's","Consumer Discretionary","Restaurants","Non-AI",210,"USA"),
 ("PEP","PepsiCo","Consumer Staples","Beverages","Non-AI",230,"USA"),
 ("ABBV","AbbVie","Healthcare","Pharmaceuticals","Non-AI",330,"USA"),
 ("BAC","Bank of America","Financials","Banks","Non-AI",320,"USA"),
]
write(spark.createDataFrame(securities,["ticker","company","sector","industry","ai_exposure","market_cap_b","country"]),"securities",len(securities))
ALL_TICKERS=[s[0] for s in securities]

# ---- SECTORS (benchmark allocation, for the concentration comparison) ----
sectors = [
 ("Technology",0.34),("Communication Services",0.16),("Consumer Discretionary",0.14),
 ("Healthcare",0.10),("Consumer Staples",0.08),("Financials",0.10),("Energy",0.08),
]
write(spark.createDataFrame(sectors,["sector","benchmark_weight"]),"sectors",len(sectors))

# ---- PORTFOLIOS ----
portfolios = [
 (1,"AI Growth Fund","Aggressive growth, AI/tech-led","A. Chen"),
 (2,"Balanced Fund","Diversified core holdings","M. Rossi"),
 (3,"Income Fund","Dividend & low volatility","S. Patel"),
]
write(spark.createDataFrame(portfolios,["portfolio_id","portfolio_name","strategy","manager"]),"portfolios",len(portfolios))

# ---- HOLDINGS (TIME-VERSIONED) + REBALANCES ----
# The AI Growth Fund went through 3 reorgs. The MAJOR one (2025-08-04) sold defensives and
# piled into AI: core-AI exposure jumps from ~30% to ~80%. That reorg drives the outperformance
# AND the concentration risk. Balanced & Income stay static.
# Each "era" is the target weights effective from a date until the next reorg.
START_DATE = "2024-06-03"
ai_eras = [
 # effective_date, weights (sum ~100)
 ("2024-06-03", {  # Era 1: diversified, ~30% core AI
   "NVDA":5,"AVGO":4,"AMD":3,"PLTR":2,"SMCI":2,"MU":2,"ANET":2,"ORCL":2,"MRVL":2,"DELL":2,"CRWD":2,  # core AI = 28
   "MSFT":5,"GOOGL":4,"META":3,"AMZN":3,"AAPL":4,"CRM":2,"QCOM":2,                                    # AI-adjacent = 23
   "JNJ":4,"PG":4,"KO":3,"XOM":4,"JPM":4,"UNH":4,"V":4,"WMT":3,"CVX":2,"HD":3,"MCD":2,"PEP":2,"ABBV":2,"BAC":2}),  # non-AI = 49
 ("2025-02-03", {  # Era 2: minor tilt toward AI, ~43% core AI
   "NVDA":7,"AVGO":6,"AMD":4,"PLTR":4,"SMCI":2,"MU":4,"ANET":4,"ORCL":3,"MRVL":3,"DELL":3,"CRWD":3,   # core AI = 43
   "MSFT":6,"GOOGL":5,"META":4,"AMZN":3,"AAPL":4,"CRM":2,                                             # adjacent = 24
   "JNJ":3,"PG":3,"KO":2,"XOM":2,"JPM":3,"UNH":3,"V":3,"WMT":2,"HD":2,"MCD":2,"ABBV":1,"BAC":1}),     # non-AI = 33
 ("2025-08-04", {  # Era 3: MAJOR AI pivot — sold defensives, bought AI => ~80% core AI
   "NVDA":11,"AVGO":9,"AMD":7,"PLTR":7,"SMCI":3,"MU":6,"ANET":6,"TSM":5,"ORCL":4,"MRVL":4,"DELL":3,"SNOW":2,"CRWD":2,"VRT":2,"ASML":2,  # core AI = 73
   "MSFT":6,"GOOGL":5,"META":4,"AMZN":3,"NOW":2,"QCOM":2}),                                           # adjacent = 22, non-AI = 0
]
static_w = {
 2: {"NVDA":4,"AVGO":3,"AMD":2,"ANET":2,"MU":2,"MSFT":6,"GOOGL":5,"META":4,"AMZN":5,"AAPL":6,"CRM":3,"TSLA":2,
     "JNJ":7,"PG":6,"KO":5,"XOM":4,"JPM":7,"UNH":5,"V":5,"HD":3,"PEP":3,"ABBV":3},
 3: {"JNJ":10,"PG":10,"KO":9,"XOM":8,"JPM":8,"UNH":7,"V":6,"WMT":5,"CVX":4,"HD":4,"MCD":4,"PEP":4,"ABBV":4,"BAC":4,"MSFT":5,"GOOGL":2,"AMZN":2},
}
hold_rows=[]
# AI Growth Fund: one row per (ticker, era) with effective_date.
# Carry a weight-0 row for any ticker dropped in a later era, so the as-of
# weight in force always reflects the FULL current era (sold names => 0),
# instead of the dropped ticker's last non-zero era persisting forever.
ever_held=set()
for eff,w in ai_eras: ever_held |= set(w)
for eff,w in ai_eras:
    for t in sorted(ever_held):
        wt=w.get(t,0)
        hold_rows.append((1,t,eff,float(wt),int(wt*1000)))
# Balanced & Income: single era from START
for pid,w in static_w.items():
    for t,wt in w.items():
        hold_rows.append((pid,t,START_DATE,float(wt),int(wt*1000)))
write(spark.createDataFrame(hold_rows,["portfolio_id","ticker","effective_date","weight_pct","shares"]).withColumn("effective_date",F.to_date("effective_date")),"holdings",len(hold_rows))

# ---- REBALANCES (the events) — what changed at each AI Growth Fund reorg ----
def diff_era(prev, cur, eff, label):
    rows=[]
    keys=set(prev)|set(cur)
    for t in keys:
        o=prev.get(t,0); n=cur.get(t,0)
        if o==n: continue
        action = "Buy" if o==0 else ("Sell" if n==0 else ("Add" if n>o else "Trim"))
        rows.append((eff, t, action, float(o), float(n), float(n-o), label))
    return rows
reb_rows=[]
reb_rows += diff_era({}, ai_eras[0][1], ai_eras[0][0], "Initial allocation")
reb_rows += diff_era(ai_eras[0][1], ai_eras[1][1], ai_eras[1][0], "Minor AI tilt")
reb_rows += diff_era(ai_eras[1][1], ai_eras[2][1], ai_eras[2][0], "Major AI pivot — sold defensives, bought AI")
write(spark.createDataFrame(reb_rows,["rebalance_date","ticker","action","old_weight","new_weight","weight_change","rationale"]).withColumn("rebalance_date",F.to_date("rebalance_date")),"rebalances",len(reb_rows))

# ---- PRICES (REAL via yfinance) ----
print("== fetching REAL prices from Yahoo Finance ==")
dl = yf.download(ALL_TICKERS+[BENCHMARK], start=START, end=END, progress=False, auto_adjust=True)
close = dl["Close"]
# tidy long form: ticker, date, close, daily_return
price_rows=[]
for t in ALL_TICKERS:
    s = close[t].dropna()
    ret = s.pct_change().fillna(0.0)
    for d,c in s.items():
        price_rows.append((t, d.date().isoformat(), float(round(c,4)), float(round(ret.loc[d],6))))
prices_pd = pd.DataFrame(price_rows, columns=["ticker","date","close","daily_return"])
prices_sdf = spark.createDataFrame(prices_pd).withColumn("date", F.to_date("date"))
write(prices_sdf,"prices",len(prices_pd))

# benchmark series
b = close[BENCHMARK].dropna(); bret=b.pct_change().fillna(0.0)
bench_rows=[(d.date().isoformat(), float(round(c,4)), float(round(bret.loc[d],6))) for d,c in b.items()]
bench_pd=pd.DataFrame(bench_rows,columns=["date","close","daily_return"])
write(spark.createDataFrame(bench_pd).withColumn("date",F.to_date("date")),"benchmark",len(bench_pd))

# ---- NEWS + sentiment (synthetic, AI names skew positive given the rally) ----
print("== news + sentiment ==")
rng=np.random.default_rng(42)
ai_core=[s[0] for s in securities if s[4]=="Core AI"]
headlines_pos=["{c} beats earnings on AI demand","{c} raises guidance as data-center orders surge",
  "Analysts lift {c} price target on AI momentum","{c} unveils next-gen AI chips","{c} signs major AI cloud deal"]
headlines_neu=["{c} in line with expectations","{c} holds steady amid market rotation","{c} announces buyback"]
headlines_neg=["{c} slips on valuation concerns","Profit-taking hits {c} after AI run-up","{c} faces export-rule scrutiny"]
sources=["Bloomberg","Reuters","WSJ","CNBC","FT","MarketWatch"]
comp={s[0]:s[1] for s in securities}
news_rows=[]; link_rows=[]; aid=1
start_d=dt.date(2024,6,1); ndays=(dt.date(2026,6,9)-start_d).days
for _ in range(1800):
    t=str(rng.choice(ALL_TICKERS))
    is_ai = t in ai_core or comp[t] in comp
    # AI names skew positive (the rally); others mixed
    if t in [s[0] for s in securities if s[4] in ("Core AI","AI-adjacent")]:
        bucket=rng.choice(["pos","neu","neg"],p=[0.6,0.28,0.12])
    else:
        bucket=rng.choice(["pos","neu","neg"],p=[0.34,0.4,0.26])
    tmpl={"pos":headlines_pos,"neu":headlines_neu,"neg":headlines_neg}[bucket]
    title=str(rng.choice(tmpl)).format(c=comp[t])
    sent={"pos":round(float(rng.uniform(0.3,0.9)),3),"neu":round(float(rng.uniform(-0.15,0.15)),3),"neg":round(float(rng.uniform(-0.9,-0.3)),3)}[bucket]
    label={"pos":"Positive","neu":"Neutral","neg":"Negative"}[bucket]
    d=(start_d+dt.timedelta(days=int(rng.integers(0,ndays)))).isoformat()
    news_rows.append((aid, d, str(rng.choice(sources)), title, sent, label))
    link_rows.append((aid, t)); aid+=1
write(spark.createDataFrame(news_rows,["article_id","published_date","source","title","sentiment","sentiment_label"]).withColumn("published_date",F.to_date("published_date")),"news",len(news_rows))
write(spark.createDataFrame(link_rows,["article_id","ticker"]),"news_ticker",len(link_rows))

print(f"\nDONE. tables in {VOL}")
