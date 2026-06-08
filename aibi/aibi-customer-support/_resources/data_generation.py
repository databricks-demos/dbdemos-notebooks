#!/usr/bin/env python
"""
AI/BI Customer Support — synthetic data generation (self-contained, from scratch).

Generates the full travel-company customer-support dataset used by the demo:
  Dimensions : regions, cities, products, customers, calendar   (curated realistic values)
  Fact       : support_cases
  AI story   : ai_assistant_releases, ai_assistant_usage

STORY — "Making support more efficient with AI, powered by Databricks":
A travel-support company launches an AI Support Copilot (built with Agent Bricks) on
2025-06-02. It auto-resolves How-To / Access / Billing cases. From that date the human
support load drops, average resolution time falls (~26h -> ~11h), satisfaction rises and
cost per case halves. The "why" lives in two tables that move in lockstep with the gain:
  - ai_assistant_releases : the release log + notes (what the copilot does)
  - ai_assistant_usage    : daily AI queries / deflections (jump at the GA release)
Regions have distinct personalities (APAC is the slow region) so filtering shows real
differences. Everything is written as parquet to the demo Volume; the demo's
bundle_config.py then builds bronze -> silver -> enriched + a metric view for lineage.

Requires Python 3.12 + databricks-connect (serverless). Run:
  uv venv --python 3.12 .venv && . .venv/bin/activate
  uv pip install "databricks-connect>=16.4,<17.4" faker numpy pandas holidays
  DATABRICKS_CONFIG_PROFILE=<profile> python data_generation.py
"""
import datetime as dt
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               LongType, DoubleType, BooleanType, DateType)
import pandas as pd

# ---------------------------------------------------------------------------
# This script ONLY generates the RAW parquet (the 8 tables) into a Volume.
# The parquet is then copied to ../dbdemos-dataset/aibi/dbdemos_aibi_customer_support_v2/
# and shipped; the bronze -> silver -> enriched tables + the `support_metrics`
# metric view (the lineage) are built at INSTALL time by the SQL in
# _resources/bundle_config.py ("sql_queries"). Do not add lineage SQL here.
#
# CATALOG/SCHEMA below are only the local build target used to (re)generate the
# raw parquet. The shipped demo's default_schema is `dbdemos_aibi_customer_support`.
# ---------------------------------------------------------------------------
CATALOG = "main"
SCHEMA = "dbdemos_aibi_customer_support_v2"   # local test schema; demo default_schema = dbdemos_aibi_customer_support
PROFILE = "DEFAULT"                            # e2-demo-field-eng
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
AI_LAUNCH = dt.date(2025, 6, 2)               # AI Copilot GA — the story's step-change
START = dt.date(2023, 1, 2)                   # data start (a Monday)
END = dt.date(2025, 12, 29)                   # data end on a clean week boundary (no partial trailing week)
N_CASES = 20000

spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
print(f"== schema + volume: {CATALOG}.{SCHEMA} ==")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")

def write(df, name, n=None):
    df.write.mode("overwrite").parquet(f"{VOL}/{name}")
    print(f"   {name}: {n if n is not None else df.count()} rows")

# ===========================================================================
# 1) DIMENSIONS — curated realistic values
# ===========================================================================
# Regions
regions = [(1, "NA", "America/New_York"), (2, "EMEA", "Europe/London"),
           (3, "APAC", "Asia/Singapore"), (4, "LATAM", "America/Sao_Paulo")]
regions_df = spark.createDataFrame(regions, ["region_id", "region_name", "primary_timezone"])
write(regions_df, "regions", len(regions))

# Cities — real travel destinations with real-ish lat/long, mapped to a region
cities_raw = [
    # NA (region 1)
    ("New York", "United States", 1, 40.7128, -74.0060), ("Los Angeles", "United States", 1, 34.0522, -118.2437),
    ("Toronto", "Canada", 1, 43.6532, -79.3832), ("Chicago", "United States", 1, 41.8781, -87.6298),
    ("Vancouver", "Canada", 1, 49.2827, -123.1207), ("Miami", "United States", 1, 25.7617, -80.1918),
    ("San Francisco", "United States", 1, 37.7749, -122.4194), ("Honolulu", "United States", 1, 21.3069, -157.8583),
    ("Las Vegas", "United States", 1, 36.1699, -115.1398), ("Cancun", "Mexico", 1, 21.1619, -86.8515),
    # EMEA (region 2)
    ("London", "United Kingdom", 2, 51.5072, -0.1276), ("Paris", "France", 2, 48.8566, 2.3522),
    ("Rome", "Italy", 2, 41.9028, 12.4964), ("Barcelona", "Spain", 2, 41.3874, 2.1686),
    ("Amsterdam", "Netherlands", 2, 52.3676, 4.9041), ("Dubai", "United Arab Emirates", 2, 25.2048, 55.2708),
    ("Reykjavik", "Iceland", 2, 64.1466, -21.9426), ("Cape Town", "South Africa", 2, -33.9249, 18.4241),
    ("Zurich", "Switzerland", 2, 47.3769, 8.5417), ("Santorini", "Greece", 2, 36.3932, 25.4615),
    # APAC (region 3)
    ("Tokyo", "Japan", 3, 35.6762, 139.6503), ("Singapore", "Singapore", 3, 1.3521, 103.8198),
    ("Sydney", "Australia", 3, -33.8688, 151.2093), ("Bangkok", "Thailand", 3, 13.7563, 100.5018),
    ("Bali", "Indonesia", 3, -8.3405, 115.0920), ("Seoul", "South Korea", 3, 37.5665, 126.9780),
    ("Auckland", "New Zealand", 3, -36.8485, 174.7633), ("Hong Kong", "Hong Kong", 3, 22.3193, 114.1694),
    ("Queenstown", "New Zealand", 3, -45.0312, 168.6626), ("Kyoto", "Japan", 3, 35.0116, 135.7681),
    # LATAM (region 4)
    ("Rio de Janeiro", "Brazil", 4, -22.9068, -43.1729), ("Buenos Aires", "Argentina", 4, -34.6037, -58.3816),
    ("Lima", "Peru", 4, -12.0464, -77.0428), ("Santiago", "Chile", 4, -33.4489, -70.6693),
    ("Bogota", "Colombia", 4, 4.7110, -74.0721), ("Cusco", "Peru", 4, -13.5320, -71.9675),
    ("Cartagena", "Colombia", 4, 10.3910, -75.4794), ("Patagonia", "Argentina", 4, -49.3300, -72.8860),
    ("Mexico City", "Mexico", 4, 19.4326, -99.1332), ("San Jose", "Costa Rica", 4, 9.9281, -84.0907),
]
cities = [(i + 1, n, c, r, lat, lon) for i, (n, c, r, lat, lon) in enumerate(cities_raw)]
cities_df = spark.createDataFrame(cities, ["city_id", "city_name", "country_name", "region_id", "latitude", "longitude"])
write(cities_df, "cities", len(cities))

# Products — travel bundles
prod_names = [
    "Iceland Northern Lights", "Alpine Ski Escape", "Patagonia Trek", "Tokyo City & Culture",
    "Bali Wellness Retreat", "Mediterranean Cruise", "Safari & Cape Town", "Greek Islands Hopper",
    "Machu Picchu Explorer", "Dubai Luxury Stay", "Hawaii Family Bundle", "New Zealand Adventure",
    "Paris Romance Package", "Thailand Beaches", "Rio Carnival Special", "Canadian Rockies Rail",
    "Vietnam Discovery", "Norwegian Fjords", "Costa Rica Eco Tour", "Australian Outback",
    "Kyoto Heritage Tour", "Caribbean All-Inclusive", "Swiss Grand Train", "Morocco Desert", "Antarctica Expedition",
]
tiers = ["Standard", "Plus", "Premium"]; focuses = ["Global", "NA-focused", "EMEA-focused", "APAC-focused", "LATAM-focused"]
products = [(i + 1, nm, tiers[i % 3], focuses[i % 5]) for i, nm in enumerate(prod_names)]
products_df = spark.createDataFrame(products, ["product_id", "product_name", "package_tier", "market_focus"])
write(products_df, "products", len(products))

# Customers — travel agencies / corporate-travel firms.
# Built with pure Spark expressions (no Faker => works on serverless with no remote deps):
# agency name = <prefix> <core> <suffix>, chosen by hashing the id.
N_CUST = 800
_pre  = ["Nova","Summit","Azure","Coastal","Crimson","Silver","Golden","Polar","Atlas","Horizon",
         "Compass","Meridian","Voyager","Pioneer","Lumen","Cedar","Harbor","Vista","Solstice","Onyx"]
_core = ["Escapes","Getaways","Voyages","Journeys","Travel","Holidays","Itineraries","Expeditions","Trails","Tours"]
_suf  = ["Group","Studio","Partners","Networks","Holdings","Collective","Co","Agency"]
def _pick(lst, salt): return F.element_at(F.array(*[F.lit(x) for x in lst]), (F.abs(F.hash(F.col("id"), F.lit(salt))) % len(lst) + 1))
segs = ["SMB", "Mid-Market", "Enterprise"]; inds = ["Travel Agency", "Corporate Travel", "Tour Operator", "Online Travel"]
bands = ["$", "$$", "$$$"]
customers_df = (spark.range(1, N_CUST + 1, numPartitions=8)
    .select(F.col("id").alias("customer_id"),
            F.concat_ws(" ", _pick(_pre,1), _pick(_core,2), _pick(_suf,3)).alias("customer_name"),
            _pick(segs,7).alias("customer_segment"),
            _pick(inds,11).alias("industry"),
            _pick(bands,13).alias("contract_value_band"),
            (F.abs(F.hash(F.col("id"), F.lit(17))) % 4 + 1).cast("int").alias("region_id")))
customers_df.write.mode("overwrite").parquet(f"{VOL}/customers"); print(f"   customers: {N_CUST} rows")

# Calendar — daily, with US holidays + peak travel season flags
import holidays as _hol
us_hol = _hol.UnitedStates(years=range(START.year, END.year + 1))
cal_rows = []
d = START
while d <= END:
    hn = us_hol.get(d)
    peak = d.month in (6, 7, 12) or (d.month == 11 and d.day >= 20)  # summer + holiday travel
    cal_rows.append((d, d.year, (d.month - 1)//3 + 1, d.month, int(d.strftime("%V")), hn is not None, hn or None, peak))
    d += dt.timedelta(days=1)
cal_schema = StructType([
    StructField("date", DateType()), StructField("year", IntegerType()), StructField("quarter", IntegerType()),
    StructField("month", IntegerType()), StructField("week", IntegerType()),
    StructField("is_us_holiday", BooleanType()), StructField("holiday_name", StringType()), StructField("is_peak_season", BooleanType())])
write(spark.createDataFrame(cal_rows, cal_schema), "calendar", len(cal_rows))

# ===========================================================================
# 2) AI assistant releases  (root cause #1 — the release notes Genie reads)
# ===========================================================================
print("== ai_assistant_releases ==")
releases = [
    (1, "v0.9-beta", dt.date(2025, 5, 5), "Internal pilot of the AI Support Copilot",
     "Limited beta of the Databricks-powered AI Support Copilot. Suggests draft replies to human agents for How-To questions only. Not customer-facing.",
     "", "Beta"),
    (2, "v1.0", AI_LAUNCH, "GA launch: AI Support Copilot auto-resolves How-To, Access & Billing",
     "General availability of the AI Support Copilot, built on Databricks with Agent Bricks. The assistant now AUTO-RESOLVES the most common ticket categories end to end - How-To, Access and Billing - by answering customers directly, looking up booking context and issuing standard fixes. Human agents now focus on complex Outage, Bug and Performance cases. Cuts average resolution time and deflects a large share of inbound volume.",
     "How-To,Access,Billing", "GA"),
    (3, "v1.1", dt.date(2025, 8, 18), "Quality + multilingual improvements",
     "Adds multilingual support (EMEA/APAC/LATAM) and improved booking-context retrieval. Higher auto-resolution confidence; further reduces escalations on How-To and Billing.",
     "How-To,Access,Billing", "GA"),
]
rel_schema = StructType([
    StructField("release_id", IntegerType()), StructField("version", StringType()), StructField("release_date", DateType()),
    StructField("capability_summary", StringType()), StructField("release_notes", StringType()),
    StructField("auto_resolved_categories", StringType()), StructField("status", StringType())])
write(spark.createDataFrame(releases, rel_schema), "ai_assistant_releases", len(releases))

# ===========================================================================
# 3) support_cases (fact) — region personalities + AI step-change story
# ===========================================================================
print("== support_cases ==")
TOTDAYS = (END - START).days
LAUNCH_OFF = (AI_LAUNCH - START).days
# region personality: share, category mix [How-To,Access,Billing,Outage,Bug,Performance], speed mult, satisfaction offset
REGION_PROFILE = {
    1: {"share": 0.34, "cat_w": [0.23,0.19,0.13,0.05,0.22,0.18], "res_mult": 0.85, "sat_off":  0.15},
    2: {"share": 0.28, "cat_w": [0.20,0.17,0.18,0.07,0.22,0.16], "res_mult": 1.00, "sat_off":  0.00},
    3: {"share": 0.26, "cat_w": [0.16,0.15,0.10,0.15,0.24,0.20], "res_mult": 1.45, "sat_off": -0.35},  # APAC = slow
    4: {"share": 0.12, "cat_w": [0.27,0.22,0.12,0.06,0.20,0.13], "res_mult": 1.10, "sat_off": -0.05},
}
# city pools per region for destination FK
cities_by_region = {}
for cid, _n, _c, r, _la, _lo in cities:
    cities_by_region.setdefault(r, []).append(cid)
CUST_BY_REGION = {}  # approximate: any customer id, region not needed for FK validity
PROD_IDS = [p[0] for p in products]
RIDS = list(REGION_PROFILE); RSHARE = [REGION_PROFILE[r]["share"] for r in RIDS]
_s = sum(RSHARE); RSHARE = [x/_s for x in RSHARE]
CBR = cities_by_region; PROF = REGION_PROFILE; LAUNCH = LAUNCH_OFF; TD = TOTDAYS; NCUST = N_CUST; NPROD = len(PROD_IDS)

case_struct = StructType([
    StructField("offset_days", IntegerType()), StructField("opened_hour", IntegerType()),
    StructField("category", StringType()), StructField("channel", StringType()), StructField("priority", StringType()),
    StructField("support_tier", StringType()), StructField("region_id", IntegerType()),
    StructField("customer_id", LongType()), StructField("product_id", LongType()), StructField("destination_city_id", LongType()),
    StructField("ai_handled", BooleanType()), StructField("resolution_hours", DoubleType()),
    StructField("satisfaction_score", DoubleType()), StructField("reopened_flag", BooleanType()), StructField("hourly_cost", DoubleType())])

@F.pandas_udf(case_struct)
def gen_case(ids: pd.Series) -> pd.DataFrame:
    import numpy as np
    rng = np.random.default_rng()
    cats = np.array(["How-To","Access","Billing","Outage","Bug","Performance"])
    chans = np.array(["Email","Chat","In-App","Phone"]); chan_w=np.array([0.32,0.30,0.25,0.13])
    pris = np.array(["Low","Medium","High","Critical"]); pri_w=np.array([0.28,0.48,0.20,0.04])
    tiers = np.array(["Tier 1","Tier 2","Tier 3"]); tier_w=np.array([0.61,0.29,0.10])
    AI_CATS = {"How-To","Access","Billing"}
    rids = np.array(RIDS); rshare = np.array(RSHARE)
    out=[]
    for _ in ids:
        # flat-ish volume with slight growth (accept-reject)
        day = int(rng.integers(0, TD + 1))
        while rng.random() > (1.0 + 0.4*(day/TD)) / 1.4:
            day = int(rng.integers(0, TD + 1))
        post = day >= LAUNCH
        hour = int(np.clip(rng.normal(13,4),0,23))
        reg = int(rng.choice(rids, p=rshare)); prof = PROF[reg]
        cat = str(rng.choice(cats, p=np.array(prof["cat_w"])))
        dcity = int(rng.choice(CBR[reg]))
        chan = str(rng.choice(chans, p=chan_w)); pri = str(rng.choice(pris, p=pri_w)); tier = str(rng.choice(tiers, p=tier_w))
        cid = int(rng.integers(1, NCUST + 1)); pid = int(rng.integers(1, NPROD + 1))
        ai = False
        if post and cat in AI_CATS:
            defl = min(0.72, 0.35 + (day - LAUNCH)/120*0.45)
            ai = rng.random() < defl
        if ai:
            res = float(np.clip(rng.exponential(0.4), 0.02, 3))
        else:
            base = {"Outage":12,"Bug":40,"Performance":34,"Billing":26,"Access":18,"How-To":14}[cat]
            base *= {"Critical":0.5,"High":0.8,"Medium":1.0,"Low":1.3}[pri] * prof["res_mult"]
            if post: base *= 0.55
            res = float(np.clip(rng.exponential(base), 0.3, 480))
        if ai:
            sat = float(rng.choice([4,5,5], p=[0.2,0.4,0.4]))
        else:
            base_sat = (4.1 if res < 12 else 3.4) + (0.25 if post else 0.0) + prof["sat_off"]
            sat = float(np.clip(round(rng.normal(base_sat,0.7)),2,5))
        if rng.random() < 0.26: sat = float("nan")
        reop = bool(rng.random() < (0.04 if (ai or res<12) else 0.13))
        hourly = float(round(rng.normal(75,8),2))
        out.append((day,hour,cat,chan,pri,tier,reg,cid,pid,dcity,ai,res,sat,reop,hourly))
    return pd.DataFrame(out, columns=[f.name for f in case_struct])

cases = (spark.range(0, N_CASES, numPartitions=8).select(gen_case(F.col("id")).alias("c")).select("c.*"))
cases = (cases
    .withColumn("opened_at", F.expr(f"timestampadd(DAY, offset_days, timestamp'{START} 00:00:00')"))
    .withColumn("opened_at", F.expr("timestampadd(HOUR, opened_hour, opened_at)"))
    .withColumn("closed_at", F.expr("timestampadd(SECOND, CAST(resolution_hours*3600 AS INT), opened_at)"))
    .withColumn("support_cost", F.round(F.when(F.col("ai_handled"), F.lit(0.30)).otherwise(F.col("resolution_hours")*F.col("hourly_cost")),2))
    .withColumn("case_id", F.monotonically_increasing_id())
    .drop("offset_days","opened_hour","hourly_cost"))
cases.write.mode("overwrite").parquet(f"{VOL}/support_cases")
print(f"   support_cases: {spark.read.parquet(f'{VOL}/support_cases').count()} rows")

# ===========================================================================
# 4) ai_assistant_usage (root cause #2) — derived from cases, jumps at launch
# ===========================================================================
print("== ai_assistant_usage ==")
sc = spark.read.parquet(f"{VOL}/support_cases")
daily = (sc.withColumn("d", F.to_date("opened_at")).groupBy("d")
           .agg(F.sum(F.when(F.col("ai_handled"),1).otherwise(0)).alias("ai_deflections"), F.count("*").alias("total_cases")))
usage = (daily
    .withColumn("is_post", F.col("d") >= F.lit(AI_LAUNCH.isoformat()))
    .withColumn("assist_queries", F.when(F.col("is_post"), F.round((F.col("total_cases")-F.col("ai_deflections"))*1.8)).otherwise(F.lit(0)))
    .withColumn("ai_queries", (F.col("ai_deflections")*F.lit(2)+F.col("assist_queries")).cast("int"))
    .withColumn("ai_compute_cost", F.round(F.col("ai_queries")*F.lit(0.012),2))
    .select(F.col("d").alias("usage_date"), "ai_queries", "ai_deflections",
            F.col("assist_queries").cast("int").alias("assist_queries"), "ai_compute_cost"))
usage.write.mode("overwrite").parquet(f"{VOL}/ai_assistant_usage")
print(f"   ai_assistant_usage: {usage.count()} days")

print(f"\nDONE. 8 tables written to {VOL}")
