#!/usr/bin/env python
"""
AI/BI Sales Pipeline Review — synthetic data generation (self-contained, from scratch).

BUSINESS: a global BEAUTY / cosmetics brand selling high-volume, small-ticket products to
retailers across regions. The value of the demo is that sales data is scattered across
several systems and Databricks unifies them:
  - Salesforce (CRM)     -> accounts + open pipeline opportunities + reps
  - ERP / Order system   -> actual orders & revenue (the fact)
  - Finance spreadsheet  -> quarterly revenue targets
  - PIM / Product system -> product lines + regional launch dates

STORY -- "Hit the number: a new line launches in EMEA and we beat the quarter":
Revenue tracks along across 4 product lines (Skincare, Makeup, Fragrance, Haircare). Then
the brand LAUNCHES its new FRAGRANCE line in EMEA on a date mid the most-recent quarter.
EMEA sales SPIKE as retailers stock it, and the AI forecast of quarter-end revenue now
projects BEATING the company-wide quarterly target. Genie: are we hitting target? -> what's
driving the spike -> EMEA -> why EMEA -> join product_launches -> Fragrance launched in EMEA
on <date> -> which accounts / reps.

Tables (raw parquet -> volume; bundle_config builds bronze->silver->enriched + metric view):
  crm_accounts       (Salesforce): account_id, account_name, segment, region, country, country_code, lat, long, owner_id
  crm_reps           (Salesforce): owner_id, rep_name, region, title
  crm_opportunities  (Salesforce): opp_id, account_id, product_line, stage, expected_revenue, created_date, close_date
  products           (PIM)       : product_line, category
  product_launches   (PIM)       : launch_id, product_line, region, launch_date, launch_name, description
  sales_targets      (Finance)   : quarter_start, target_revenue (company-wide)
  erp_orders         (ERP, FACT) : order_id, order_date, account_id, product_line, region, units, revenue

Requires Python 3.12 + databricks-connect (serverless).
Run: DATABRICKS_CONFIG_PROFILE=<profile> python data_generation.py
"""
import datetime as dt
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType,
                               DoubleType, DateType)

CATALOG = "main"
SCHEMA = "dbdemos_aibi_sales_pipeline_review_v2"   # build schema; demo default_schema = dbdemos_aibi_sales_pipeline_review
PROFILE = "DEFAULT"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

START = dt.date(2024, 12, 1)
END = dt.date(2026, 6, 8)
# Fiscal quarter we're "in": Q2-2026 = Apr 1 -> Jun 30 2026. Fragrance launches in EMEA mid-quarter.
QUARTER_START = dt.date(2026, 4, 1)
QUARTER_END = dt.date(2026, 6, 30)
LAUNCH_DATE = dt.date(2026, 5, 4)   # new Fragrance line goes live in EMEA

spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
print(f"== schema + volume {CATALOG}.{SCHEMA} ==")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
def write(df, name, n=None): df.write.mode("overwrite").parquet(f"{VOL}/{name}"); print(f"   {name}: {n if n is not None else df.count()} rows")

# ---- PRODUCTS (PIM) ----
products = [("Skincare","Face & Body"),("Makeup","Color Cosmetics"),
            ("Fragrance","Perfume & Scent"),("Haircare","Hair & Scalp")]
write(spark.createDataFrame(products,["product_line","category"]),"products",4)

# product_launches: most lines available everywhere from the start; the NEW Fragrance line
# launches region by region, and EMEA is the spike (launches mid-quarter).
launches = [
 (1,"Skincare","Global","2024-01-01","Skincare (core range)","Core skincare range, available worldwide."),
 (2,"Makeup","Global","2024-01-01","Makeup (core range)","Core color cosmetics, available worldwide."),
 (3,"Haircare","Global","2024-01-01","Haircare (core range)","Core haircare range, available worldwide."),
 (4,"Fragrance","AMER","2024-06-01","Fragrance launch - AMER","New signature Fragrance line launched in the Americas."),
 (5,"Fragrance","APAC","2024-09-01","Fragrance launch - APAC","New signature Fragrance line launched in APAC."),
 (6,"Fragrance","EMEA",str(LAUNCH_DATE),"Fragrance launch - EMEA","New signature Fragrance line launched across EMEA - drives a sharp consumption spike in Europe."),
 (7,"Fragrance","LATAM","2026-06-15","Fragrance launch - LATAM","New signature Fragrance line launched in LATAM (very recent)."),
]
write(spark.createDataFrame(launches,["launch_id","product_line","region","launch_date","launch_name","description"])
      .withColumn("launch_date",F.to_date("launch_date")),"product_launches",len(launches))

# ---- SALES TARGETS (Finance) -- company-wide, per quarter ----
# Full quarters run ~$29.5M. Earlier targets ~run-rate. The CURRENT quarter (Q2-2026) was set
# conservatively at $21.5M and is being SMASHED (~$33M projected = ~155% attainment) thanks to
# the EMEA Fragrance launch -- the headline beat the demo tells.
targets = [
 ("2024-10-01",28000000.0),("2025-01-01",28500000.0),("2025-04-01",29000000.0),
 ("2025-07-01",29000000.0),("2025-10-01",29500000.0),("2026-01-01",29500000.0),
 ("2026-04-01",21500000.0),
]
write(spark.createDataFrame(targets,["quarter_start","target_revenue"]).withColumn("quarter_start",F.to_date("quarter_start")),"sales_targets",len(targets))

# ---- REPS (Salesforce) ----
reps = [
 (1,"Sophie Martin","EMEA","Account Executive"),(2,"Liam O'Brien","EMEA","Account Executive"),
 (3,"Emma Schmidt","EMEA","Senior AE"),(4,"James Carter","AMER","Account Executive"),
 (5,"Olivia Nguyen","AMER","Senior AE"),(6,"Noah Williams","AMER","Account Executive"),
 (7,"Yuki Tanaka","APAC","Account Executive"),(8,"Mei Lin","APAC","Senior AE"),
 (9,"Lucas Silva","LATAM","Account Executive"),(10,"Sofia Garcia","LATAM","Account Executive"),
]
write(spark.createDataFrame(reps,["owner_id","rep_name","region","title"]),"crm_reps",len(reps))

# ---- REGIONS (for geo) -- a handful of countries per region with lat/long ----
# region, country, code, lat, long
geo = [
 ("EMEA","France","FR",46.23,2.21),("EMEA","Germany","DE",51.17,10.45),("EMEA","United Kingdom","GB",55.38,-3.44),
 ("EMEA","Italy","IT",41.87,12.57),("EMEA","Spain","ES",40.46,-3.75),("EMEA","UAE","AE",23.42,53.85),
 ("AMER","United States","US",37.09,-95.71),("AMER","Canada","CA",56.13,-106.35),("AMER","Mexico","MX",23.63,-102.55),
 ("APAC","Japan","JP",36.20,138.25),("APAC","Australia","AU",-25.27,133.78),("APAC","Singapore","SG",1.35,103.82),
 ("APAC","South Korea","KR",35.91,127.77),
 ("LATAM","Brazil","BR",-14.24,-51.93),("LATAM","Argentina","AR",-38.42,-63.62),("LATAM","Chile","CL",-35.68,-71.54),
]

# ---- CRM ACCOUNTS (Salesforce) ----
print("== crm_accounts ==")
NACC=150
SEGMENTS=["Department Store","Specialty Retail","Pharmacy","E-commerce"]
acc_struct=StructType([
  StructField("account_id",IntegerType()),StructField("account_name",StringType()),
  StructField("segment",StringType()),StructField("region",StringType()),
  StructField("country",StringType()),StructField("country_code",StringType()),
  StructField("latitude",DoubleType()),StructField("longitude",DoubleType()),
  StructField("owner_id",IntegerType())])
GEO=geo; SEG=SEGMENTS
@F.pandas_udf(acc_struct)
def gen_acc(ids):
    import numpy as np, pandas as pd
    rng=np.random.default_rng()
    geo=GEO
    # region weights: EMEA & AMER bigger
    reg_w={"EMEA":0.34,"AMER":0.34,"APAC":0.20,"LATAM":0.12}
    reps_by_reg={"EMEA":[1,2,3],"AMER":[4,5,6],"APAC":[7,8],"LATAM":[9,10]}
    prefixes=["Belle","Lumière","Aurora","Maison","Glow","Éclat","Nova","Velvet","Rouge","Coastal","Urban","Bloom","Luxe","Pure","Azure"]
    suffixes=["Beauty","Cosmetics","Retail Group","Stores","Pharmacy","Boutique","Market","Distributors","Beauté","Collective"]
    out=[]
    for i in ids:
        i=int(i)
        reg=rng.choice(list(reg_w),p=list(reg_w.values()))
        gopts=[g for g in geo if g[0]==reg]; g=gopts[rng.integers(0,len(gopts))]
        seg=str(rng.choice(SEG,p=[0.18,0.32,0.20,0.30]))
        owner=int(rng.choice(reps_by_reg[reg]))
        name=f"{prefixes[i%len(prefixes)]} {suffixes[(i//len(prefixes))%len(suffixes)]} {i}"
        # jitter lat/long a touch so map bubbles spread
        lat=float(g[3]+rng.normal(0,1.2)); lon=float(g[4]+rng.normal(0,1.2))
        out.append((i,name,seg,reg,g[1],g[2],lat,lon,owner))
    return pd.DataFrame(out,columns=[f.name for f in acc_struct])
accounts=spark.range(1,NACC+1).select(gen_acc(F.col("id")).alias("a")).select("a.*")
accounts.write.mode("overwrite").parquet(f"{VOL}/crm_accounts")
print(f"   crm_accounts: {spark.read.parquet(f'{VOL}/crm_accounts').count()} rows")

# ---- ERP ORDERS (the fact) -- daily small-ticket orders ----
print("== erp_orders (fact) ==")
TD=(END-START).days
LAUNCH_OFF=(LAUNCH_DATE-START).days
acc_pd=[ (r['account_id'], r['region']) for r in spark.read.parquet(f'{VOL}/crm_accounts').select('account_id','region').collect() ]
ACC_BY_REG={}
for aid,reg in acc_pd: ACC_BY_REG.setdefault(reg,[]).append(aid)
import json as _json
ACC_JSON=_json.dumps(ACC_BY_REG)

ord_struct=StructType([
  StructField("offset_days",IntegerType()),StructField("account_id",IntegerType()),
  StructField("product_line",StringType()),StructField("region",StringType()),
  StructField("units",IntegerType()),StructField("revenue",DoubleType())])
TDc=TD; LO=LAUNCH_OFF
@F.pandas_udf(ord_struct)
def gen_ord(ids):
    import numpy as np, pandas as pd, json
    rng=np.random.default_rng()
    acc_by_reg=json.loads(ACC_JSON)
    regions=list(acc_by_reg.keys())
    # base daily order volume mix by region (EMEA/AMER bigger)
    reg_w={"EMEA":0.32,"AMER":0.34,"APAC":0.20,"LATAM":0.14}
    rw=[reg_w[r] for r in regions]
    lines=["Skincare","Makeup","Fragrance","Haircare"]
    # baseline product mix (Fragrance smaller pre-launch in EMEA)
    line_w=[0.34,0.30,0.12,0.24]
    # small-ticket average prices per line
    price={"Skincare":42.0,"Makeup":34.0,"Fragrance":78.0,"Haircare":28.0}
    out=[]
    for _ in ids:
        day=int(rng.integers(0,TDc+1))
        reg=str(rng.choice(regions,p=rw))
        line=str(rng.choice(lines,p=line_w))
        # --- THE EVENT: Fragrance becomes available in EMEA at LAUNCH_OFF -> EMEA fragrance ramps hard ---
        if reg=="EMEA" and line=="Fragrance":
            if day < LO:
                # before EMEA launch: fragrance barely sold in EMEA -> mostly skip (reassign to skincare)
                if rng.random() < 0.85:
                    line="Skincare"
        accs=acc_by_reg[reg]; acc=int(accs[rng.integers(0,len(accs))])
        units=int(np.clip(rng.lognormal(3.0,0.7),1,400))
        # post-launch EMEA fragrance gets a growing boost (adoption ramp)
        boost=1.0
        if reg=="EMEA" and line=="Fragrance" and day>=LO:
            ramp=min(1.0,(day-LO)/45.0)
            boost=1.0+ramp*4.0    # up to ~5x order sizes as adoption ramps
        units=int(units*boost)
        rev=round(units*price[line]*rng.uniform(0.9,1.12),2)
        out.append((day,acc,line,reg,units,rev))
    return pd.DataFrame(out,columns=[f.name for f in ord_struct])

N=180000
orders=spark.range(0,N,numPartitions=32).select(gen_ord(F.col("id")).alias("o")).select("o.*")
orders=(orders
  .withColumn("order_date",F.expr(f"date_add(DATE'{START}', offset_days)"))
  .withColumn("order_id",F.monotonically_increasing_id())
  .drop("offset_days"))
orders.write.mode("overwrite").parquet(f"{VOL}/erp_orders")
print(f"   erp_orders: {spark.read.parquet(f'{VOL}/erp_orders').count()} rows")

# ---- CRM OPPORTUNITIES (Salesforce) -- open pipeline (expansion deals) for coverage ----
print("== crm_opportunities ==")
opp_struct=StructType([
  StructField("opp_id",IntegerType()),StructField("account_id",IntegerType()),
  StructField("product_line",StringType()),StructField("stage",StringType()),
  StructField("expected_revenue",DoubleType()),
  StructField("created_off",IntegerType()),StructField("close_off",IntegerType())])
@F.pandas_udf(opp_struct)
def gen_opp(ids):
    import numpy as np, pandas as pd, json
    rng=np.random.default_rng()
    acc_by_reg=json.loads(ACC_JSON)
    all_acc=[a for v in acc_by_reg.values() for a in v]
    lines=["Skincare","Makeup","Fragrance","Haircare"]
    stages=["Prospecting","Qualification","Proposal","Negotiation","Closed Won","Closed Lost"]
    stage_w=[0.18,0.20,0.22,0.18,0.14,0.08]
    out=[]
    for i in ids:
        i=int(i)
        acc=int(all_acc[rng.integers(0,len(all_acc))])
        line=str(rng.choice(lines,p=[0.3,0.28,0.2,0.22]))
        stage=str(rng.choice(stages,p=stage_w))
        exp=round(float(np.clip(rng.lognormal(10.2,0.7),3000,400000)),2)
        created=int(rng.integers(TDc-180,TDc))   # recent
        close=int(created+rng.integers(15,120))
        out.append((i,acc,line,stage,exp,created,close))
    return pd.DataFrame(out,columns=[f.name for f in opp_struct])
opps=spark.range(1,1201).select(gen_opp(F.col("id")).alias("o")).select("o.*")
opps=(opps
  .withColumn("created_date",F.expr(f"date_add(DATE'{START}', created_off)"))
  .withColumn("close_date",F.expr(f"date_add(DATE'{START}', close_off)"))
  .drop("created_off","close_off"))
opps.write.mode("overwrite").parquet(f"{VOL}/crm_opportunities")
print(f"   crm_opportunities: {spark.read.parquet(f'{VOL}/crm_opportunities').count()} rows")

print(f"\nDONE. tables in {VOL}")
