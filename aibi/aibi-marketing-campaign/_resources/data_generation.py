#!/usr/bin/env python
"""
AI/BI Marketing Campaign — synthetic data generation (self-contained, from scratch).

DOMAIN: Multi-channel marketing. Campaigns run across 4 channels (TikTok, Instagram,
Google Ads, Email) and 2 platforms (Mobile, Web), targeting audiences worldwide.
Daily performance is tracked: impressions, clicks, spend, conversions, revenue.

STORY — "A bad new creative tanks the campaign (root cause hides in another table)":
Through mid-2025 every channel is healthy (revenue grows, conversions steady). On
2025-09-01 the team swaps in a NEW localized creative -- "Fall Sale - v2 (DE/FR)" -- on
TikTok for the German & French markets. It flops: conversion rate craters (~0.6% vs 2.5%),
so revenue and conversions COLLAPSE even though spend stays flat. The collapse is
concentrated in TikTok x {Germany, France} -- those countries go RED on the map while the
rest stay green. The performance fact only carries a creative_id; the WHY lives in the
`creatives` table. Genie drill: "why did conversions drop in Sept?" -> channel=TikTok ->
market=Germany/France -> join creatives -> "Fall Sale - v2 (DE)" launched Sept 1, conv 0.6%.

Tables (raw parquet -> volume; bundle_config builds bronze->silver->enriched + metric view):
  channels   (dim)  : channel_id, channel_name, channel_type
  campaigns  (dim)  : campaign_id, campaign_name, objective, start_date, end_date
  audiences  (dim)  : audience_id, audience_name, age_band, interest
  regions    (dim)  : region_id, country, country_code, latitude, longitude
  creatives  (dim)  : creative_id, creative_name, channel_id, message_theme, format,
       launch_date, target_market, status (root-cause table; bad creative flagged)
  campaign_performance (fact, daily) : perf_id, date, campaign_id, channel_id, platform,
       audience_id, region_id, creative_id, impressions, clicks, spend, conversions, revenue

Requires Python 3.12 + databricks-connect (serverless).
Run: DATABRICKS_CONFIG_PROFILE=<profile> python data_generation.py
"""
import datetime as dt
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType,
                               DoubleType, DateType)

CATALOG = "main"
SCHEMA = "dbdemos_aibi_cme_marketing_campaign_v2"   # local build schema; demo default_schema = dbdemos_aibi_cme_marketing_campaign
PROFILE = "DEFAULT"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
EVENT_DATE = dt.date(2025, 9, 1)    # bad creative launches -> collapse starts here
START = dt.date(2024, 1, 1)
END = dt.date(2026, 5, 31)
BAD_CHANNEL = 1                     # TikTok
BAD_MARKETS = (3, 4)               # region_id: Germany(3), France(4)
BAD_CREATIVE_ID = 999              # "Fall Sale - v2 (DE/FR)"
BAD_CAMPAIGN_ID = 8                # "Q4 Growth Push" -- the campaign the bad creative belongs to
spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
print(f"== schema + volume {CATALOG}.{SCHEMA} ==")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
def write(df, name, n=None): df.write.mode("overwrite").parquet(f"{VOL}/{name}"); print(f"   {name}: {n if n is not None else df.count()} rows")

# ---- DIMENSIONS (curated) ----
channels = [(1,"TikTok","Social"),(2,"Instagram","Social"),(3,"Google Ads","Search"),(4,"Email","Owned")]
write(spark.createDataFrame(channels,["channel_id","channel_name","channel_type"]),"channels",4)

campaigns = [
 (1,"Spring Launch","Awareness","2024-02-01","2024-04-30"),(2,"Summer Sale","Conversion","2024-06-01","2024-08-31"),
 (3,"Back to Business","Lead Gen","2024-09-01","2024-11-30"),(4,"Holiday Deals","Conversion","2024-11-15","2025-01-15"),
 (5,"New Year Promo","Awareness","2025-01-15","2025-03-15"),(6,"Spring Refresh","Conversion","2025-03-01","2025-05-31"),
 (7,"Summer Blowout","Conversion","2025-06-01","2025-08-31"),(8,"Q4 Growth Push","Conversion","2025-09-01","2026-05-31"),
 (9,"Holiday Mega Sale","Conversion","2025-11-15","2026-01-31"),(10,"2026 Kickoff","Awareness","2026-01-15","2026-03-31"),
 (11,"Spring 2026","Conversion","2026-03-01","2026-05-31"),
]
write(spark.createDataFrame(campaigns,["campaign_id","campaign_name","objective","start_date","end_date"])
      .withColumn("start_date",F.to_date("start_date")).withColumn("end_date",F.to_date("end_date")),"campaigns",len(campaigns))

audiences = [(1,"Gen Z","18-24","Trends & Entertainment"),(2,"Young Pros","25-34","Career & Tech"),
 (3,"Families","35-49","Home & Lifestyle"),(4,"Established","50-65","Finance & Travel")]
write(spark.createDataFrame(audiences,["audience_id","audience_name","age_band","interest"]),"audiences",4)

regions = [  # country, code, lat, lon
 (1,"United States","US",37.09,-95.71),(2,"United Kingdom","GB",55.38,-3.44),(3,"Germany","DE",51.17,10.45),
 (4,"France","FR",46.23,2.21),(5,"Spain","ES",40.46,-3.75),(6,"Italy","IT",41.87,12.57),(7,"Canada","CA",56.13,-106.35),
 (8,"Brazil","BR",-14.24,-51.93),(9,"Mexico","MX",23.63,-102.55),(10,"Japan","JP",36.20,138.25),
 (11,"Australia","AU",-25.27,133.78),(12,"India","IN",20.59,78.96),(13,"Singapore","SG",1.35,103.82),
 (14,"Netherlands","NL",52.13,5.29),(15,"Sweden","SE",60.13,18.64),(16,"Poland","PL",51.92,19.15),
 (17,"UAE","AE",23.42,53.85),(18,"South Korea","KR",35.91,127.77),(19,"Argentina","AR",-38.42,-63.62),(20,"South Africa","ZA",-30.56,22.94),
]
write(spark.createDataFrame(regions,["region_id","country","country_code","latitude","longitude"]),"regions",len(regions))

# ---- CREATIVES (dim) -- the root-cause table. Healthy creatives + one flagged bad one. ----
# creative_id, creative_name, channel_id, message_theme, format, launch_date, target_market, status
creatives = [
 (1,"Always-On Brand","TikTok",1,"Brand Story","Video","2024-01-01","Global","active"),
 (2,"Summer Vibes","TikTok",1,"Lifestyle","Video","2024-06-01","Global","active"),
 (3,"Carousel Classics","Instagram",2,"Product Showcase","Carousel","2024-01-01","Global","active"),
 (4,"Story Highlights","Instagram",2,"Social Proof","Story","2024-05-01","Global","active"),
 (5,"Search Intent","Google Ads",3,"Offer / Discount","Search Text","2024-01-01","Global","active"),
 (6,"Shopping Feed","Google Ads",3,"Product Showcase","Shopping","2024-03-01","Global","active"),
 (7,"Newsletter Promo","Email",4,"Offer / Discount","HTML Email","2024-01-01","Global","active"),
 (8,"Loyalty Rewards","Email",4,"Retention","HTML Email","2024-04-01","Global","active"),
 # --- the flagged bad new creative: localized TikTok variant launched on the event date ---
 (BAD_CREATIVE_ID,"Fall Sale - v2 (DE/FR)","TikTok",1,"Aggressive Discount","Video",
   str(EVENT_DATE),"Germany & France","underperforming"),
]
write(spark.createDataFrame(creatives,["creative_id","creative_name","channel","channel_id",
      "message_theme","format","launch_date","target_market","status"])
      .withColumn("launch_date",F.to_date("launch_date")),"creatives",len(creatives))

# ---- FACT: campaign_performance (daily grain across channel x platform x audience x region x creative) ----
print("== campaign_performance (fact) ==")
TD=(END-START).days; EVENT_OFF=(EVENT_DATE-START).days
NREG=len(regions); NAUD=len(audiences); NCAMP=len(campaigns)
# map channel_id -> its healthy "good" creative ids (for FK assignment)
GOOD_CREATIVES={1:[1,2],2:[3,4],3:[5,6],4:[7,8]}
perf_struct = StructType([
    StructField("offset_days",IntegerType()),StructField("channel_id",IntegerType()),StructField("platform",StringType()),
    StructField("audience_id",IntegerType()),StructField("region_id",IntegerType()),StructField("campaign_id",IntegerType()),
    StructField("creative_id",IntegerType()),
    StructField("impressions",IntegerType()),StructField("clicks",IntegerType()),StructField("spend",DoubleType()),
    StructField("conversions",IntegerType()),StructField("revenue",DoubleType())])

@F.pandas_udf(perf_struct)
def gen(ids):
    import numpy as np, pandas as pd
    rng=np.random.default_rng()
    # TikTok is the biggest channel (the one we lean on for growth)
    chans=np.array([1,2,3,4]); chan_w=np.array([0.42,0.22,0.22,0.14])
    plats=np.array(["Mobile","Web"])
    good={1:[1,2],2:[3,4],3:[5,6],4:[7,8]}
    # region weights: Germany(3) & France(4) are top markets, rest spread worldwide
    reg_ids=np.arange(1,NREG+1)
    reg_w=np.full(NREG, 0.6); reg_w[0]=2.5  # US big
    reg_w[2]=4.0; reg_w[3]=3.5              # Germany, France = major markets
    reg_w[1]=2.0; reg_w[4]=1.5; reg_w[5]=1.5  # UK, Spain, Italy
    reg_w=reg_w/reg_w.sum()
    out=[]
    for _ in ids:
        day=int(rng.integers(0,TD+1))
        post=day>=EVENT_OFF
        ch=int(rng.choice(chans,p=chan_w))
        pm = 0.85 if ch in (1,2) else (0.55 if ch==3 else 0.45)   # social skews mobile
        plat=str(rng.choice(plats,p=[pm,1-pm]))
        aud=int(rng.integers(1,NAUD+1)); reg=int(rng.choice(reg_ids,p=reg_w)); camp=int(rng.integers(1,NCAMP+1))
        # healthy per-channel economics (all 4 land in a comparable ~3-5 revenue/spend band)
        cpc={1:0.30,2:0.55,3:1.20,4:0.45}[ch]
        base_cvr={1:0.030,2:0.028,3:0.045,4:0.050}[ch]
        aov=rng.normal(80,15)
        clicks=int(np.clip(rng.lognormal(4.2,0.7),5,4000))
        # creative FK: healthy creative for this channel by default
        creative=int(rng.choice(good[ch]))
        cvr=base_cvr*rng.uniform(0.9,1.1)
        # --- THE EVENT: bad new creative on TikTok in Germany/France from EVENT_DATE ---
        # spend stays flat; the NEW creative just converts terribly -> revenue & conversions collapse.
        if post and ch==BAD_CHANNEL and reg in BAD_MARKETS:
            creative=BAD_CREATIVE_ID
            camp=BAD_CAMPAIGN_ID      # the bad creative belongs to the "Q4 Growth Push" campaign
            cvr=base_cvr*0.12         # conv rate craters (~0.35% vs ~3%)
        ctr=rng.uniform(0.008,0.03)
        impressions=int(clicks/max(ctr,0.001))
        spend=round(clicks*cpc*rng.uniform(0.9,1.1),2)
        conversions=int(rng.binomial(clicks, min(cvr,0.5)))
        revenue=round(conversions*max(aov,20),2)
        out.append((day,ch,plat,aud,reg,camp,creative,impressions,clicks,spend,conversions,revenue))
    return pd.DataFrame(out,columns=[f.name for f in perf_struct])

N=60000
perf=(spark.range(0,N,numPartitions=16).select(gen(F.col("id")).alias("p")).select("p.*"))
perf=(perf
    .withColumn("date", F.expr(f"date_add(DATE'{START}', offset_days)"))
    .withColumn("perf_id", F.monotonically_increasing_id())
    .drop("offset_days"))
perf.write.mode("overwrite").parquet(f"{VOL}/campaign_performance")
print(f"   campaign_performance: {spark.read.parquet(f'{VOL}/campaign_performance').count()} rows")
print(f"\nDONE. tables in {VOL}")
