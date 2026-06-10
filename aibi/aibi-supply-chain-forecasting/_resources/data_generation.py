#!/usr/bin/env python
"""
AI/BI Supply Chain Forecasting — synthetic data generation (self-contained, from scratch).

BUSINESS: an E-BIKE / micro-mobility manufacturer. It builds finished products (City E-Bike,
Cargo E-Bike, Folding E-Bike, E-Scooter, E-Moped) from components via a bill of materials
(BOM). Components are bought from suppliers (each with a lead time), held as inventory
(on-hand + safety stock), replenished by open purchase orders, and demand is fulfilled from
distribution centers (DCs) worldwide.

STORY -- "A demand surge is about to cause a battery-cell stockout, and the lead time means
we must act now":
Weekly product demand is steady. Then ~6 weeks ago the CITY E-BIKE demand SURGES. Rolling
that through the BOM, the BATTERY CELL (used by every product) is consumed faster than it is
replenished, so projected on-hand depletes -- through the safety-stock level and toward ZERO
around late July. Because the battery-cell supplier's LEAD TIME is 8 weeks, a reorder placed
today barely arrives in time: this is why the planner must act immediately. Every other
component is comfortably covered -- the Battery Cell is the single bottleneck.
Genie: which component is at risk? -> Battery Cell -> when does it stock out? -> ~late July ->
why can't we just reorder? -> 8-week supplier lead time -> what's driving the consumption? ->
the City E-Bike demand surge (via the BOM).

Tables (raw parquet -> volume; bundle_config builds bronze->silver->enriched + metric view):
  products             : product_id, product_name, category
  distribution_centers : dc_id, dc_name, region, country, country_code, latitude, longitude
  suppliers            : supplier_id, supplier_name, region, lead_time_weeks, reliability_pct
  components           : component_id, component_name, component_type, supplier_id, unit_cost
  inventory            : component_id, on_hand_units, safety_stock_units, weekly_supply_units
  purchase_orders      : po_id, component_id, supplier_id, order_week, expected_arrival_week, qty_units, status
  bom                  : product_id, component_id, qty_per_unit
  product_demand (FACT): demand_id, week, product_id, dc_id, demand_units

Requires Python 3.12 + databricks-connect (serverless).
Run: DATABRICKS_CONFIG_PROFILE=<profile> python data_generation.py
"""
import datetime as dt
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType, DateType)

CATALOG = "main"
SCHEMA = "dbdemos_aibi_mfg_supply_chain_optimization_v2"   # build schema; demo default_schema = dbdemos_aibi_mfg_supply_chain_optimization
PROFILE = "DEFAULT"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

START = dt.date(2024, 6, 3)     # weekly data (Mondays), ~2 years
END = dt.date(2026, 6, 1)
SURGE_WEEK = dt.date(2026, 4, 20)   # City E-Bike demand surges ~6 weeks before the data end
SURGE_PRODUCT_ID = 1                # City E-Bike

spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
print(f"== schema + volume {CATALOG}.{SCHEMA} ==")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
def write(df, name, n=None): df.write.mode("overwrite").parquet(f"{VOL}/{name}"); print(f"   {name}: {n if n is not None else df.count()} rows")

# ---- PRODUCTS ----
products = [
 (1,"City E-Bike","E-Bike"),(2,"Cargo E-Bike","E-Bike"),(3,"Folding E-Bike","E-Bike"),
 (4,"E-Scooter","Scooter"),(5,"E-Moped","Moped"),
]
write(spark.createDataFrame(products,["product_id","product_name","category"]),"products",len(products))

# ---- DISTRIBUTION CENTERS (lat/long for the map) ----
dcs = [
 (1,"Amsterdam DC","EMEA","Netherlands","NL",52.37,4.90),
 (2,"Berlin DC","EMEA","Germany","DE",52.52,13.40),
 (3,"Paris DC","EMEA","France","FR",48.86,2.35),
 (4,"Chicago DC","AMER","United States","US",41.88,-87.63),
 (5,"Los Angeles DC","AMER","United States","US",34.05,-118.24),
 (6,"Toronto DC","AMER","Canada","CA",43.65,-79.38),
 (7,"Tokyo DC","APAC","Japan","JP",35.68,139.69),
 (8,"Singapore DC","APAC","Singapore","SG",1.35,103.82),
]
write(spark.createDataFrame(dcs,["dc_id","dc_name","region","country","country_code","latitude","longitude"]),"distribution_centers",len(dcs))

# ---- MARKET LAUNCHES (the UPSTREAM cause of the demand surge) ----
# The City E-Bike opened a major new EMEA market on the surge date -> EMEA demand jumps, which
# (via the BOM) drains the shared Battery Cell at the Rotterdam plant. This is the root cause Genie
# can point to for "why did demand surge?".
market_launches = [
 (1,"City E-Bike","EMEA",str(SURGE_WEEK),"City E-Bike — EMEA market launch",
    "The City E-Bike launched across a major new EMEA market, driving a sharp, sustained demand increase in the region."),
]
write(spark.createDataFrame(market_launches,["launch_id","product_name","region","launch_date","launch_name","description"])
      .withColumn("launch_date",F.to_date("launch_date")),"market_launches",len(market_launches))

# ---- SUPPLIERS (lead times) -- the battery supplier's long lead time is the crux ----
suppliers = [
 (1,"PowerCell Industries","APAC",8,96.5),   # Battery Cell supplier: 8-week lead time (the constraint)
 (2,"DriveTech Motors","EMEA",4,98.2),
 (3,"Alu-Frame Works","EMEA",3,99.0),
 (4,"SafeStop Brakes","AMER",3,97.8),
 (5,"ClearView Displays","APAC",5,95.4),
 (6,"RollFast Tires","AMER",2,99.3),
]
write(spark.createDataFrame(suppliers,["supplier_id","supplier_name","region","lead_time_weeks","reliability_pct"]),"suppliers",len(suppliers))

# ---- COMPONENTS (sourced from a supplier) ----
components = [
 (1,"Battery Cell","Power",1,42.0),
 (2,"Electric Motor","Drivetrain",2,85.0),
 (3,"Frame","Chassis",3,120.0),
 (4,"Brake System","Safety",4,38.0),
 (5,"Display Unit","Electronics",5,29.0),
 (6,"Tire Set","Chassis",6,46.0),
]
write(spark.createDataFrame(components,["component_id","component_name","component_type","supplier_id","unit_cost"]),"components",len(components))

# ---- PLANTS (assembly factories that hold component inventory) ----
plants = [
 (1,"Rotterdam Plant","EMEA"),
 (2,"Detroit Plant","AMER"),
]
write(spark.createDataFrame(plants,["plant_id","plant_name","region"]),"plants",len(plants))

# ---- INVENTORY (on-hand + safety stock + steady weekly inbound) PER COMPONENT x PLANT ----
# Plants serve regions: ROTTERDAM = EMEA + APAC (the surging City E-Bike demand, ~128k cells/wk),
# DETROIT = AMER (~55k cells/wk). "Weeks of stock on hand" = on_hand / weekly_demand.
# Battery Cell is the bottleneck, lowest cover and clearly worse at Rotterdam (surge):
#   Rotterdam battery: on-hand ~300k / ~128k demand -> ~2.3 weeks (critical, supplier lead time is 8 wks)
#   Detroit   battery: on-hand ~165k / ~55k  demand -> ~3.0 weeks
# Every other component carries 9-17 weeks of stock -> healthy. (on-hand sized for varied, realistic cover.)
# (component_id, plant_id, on_hand, safety_stock, weekly_supply)
inventory = [
 # Rotterdam (EMEA+APAC; bears the surge -> battery critical)
 (1,1,300000,90000,95000),  (2,1,180000,24000,26000), (3,1,200000,24000,25000),
 (4,1,210000,26000,26000),  (5,1,200000,24000,25000), (6,1,210000,26000,26000),
 # Detroit (AMER)
 (1,2,165000,48000,48000),  (2,2,210000,16000,16000), (3,2,230000,16000,15000),
 (4,2,240000,18000,16000),  (5,2,230000,16000,15000), (6,2,240000,18000,16000),
]
write(spark.createDataFrame(inventory,["component_id","plant_id","on_hand_units","safety_stock_units","weekly_supply_units"]),"inventory",len(inventory))

# ---- BOM: qty of each component per finished product unit (Battery Cell used by all) ----
bom = [
 (1,1,8),(1,2,1),(1,3,1),(1,4,1),(1,5,1),(1,6,1),     # City E-Bike (8 cells)
 (2,1,12),(2,2,2),(2,3,1),(2,4,1),(2,5,1),(2,6,1),    # Cargo E-Bike (12 cells)
 (3,1,6),(3,2,1),(3,3,1),(3,4,1),(3,5,1),(3,6,1),     # Folding E-Bike (6 cells)
 (4,1,4),(4,2,1),(4,3,1),(4,4,1),(4,5,1),(4,6,1),     # E-Scooter (4 cells)
 (5,1,10),(5,2,1),(5,3,1),(5,4,1),(5,5,1),(5,6,1),    # E-Moped (10 cells)
]
write(spark.createDataFrame(bom,["product_id","component_id","qty_per_unit"]),"bom",len(bom))

# ---- PURCHASE ORDERS (open inbound supply) ----
# Steady weekly POs per component (qty = weekly_supply_units), arriving 'lead_time_weeks' after order.
# A few recent battery-cell POs are still in transit (status In Transit) -> they arrive but the
# surge outpaces them; the next reorder would take 8 weeks, which is the urgency.
print("== purchase_orders ==")
NWEEKS=((END-START).days//7)+1
lead_map={s[0]:s[3] for s in suppliers}      # supplier_id -> lead_time_weeks
comp_sup={c[0]:c[3] for c in components}     # component_id -> supplier_id
# weekly inbound qty per (component, plant) from the per-plant inventory rows
supply_by_cp={(r[0],r[1]):r[4] for r in inventory}
po_rows=[]
po_id=1
for (comp,plant),qty in supply_by_cp.items():
    sup=comp_sup[comp]; lead=lead_map[sup]
    for w in range(NWEEKS):
        order_week=START+dt.timedelta(weeks=w)
        arrival_week=order_week+dt.timedelta(weeks=lead)
        status="Received" if arrival_week < END else ("In Transit" if order_week < END else "Planned")
        po_rows.append((po_id, comp, plant, sup, str(order_week), str(arrival_week), qty, status))
        po_id+=1
write(spark.createDataFrame(po_rows,["po_id","component_id","plant_id","supplier_id","order_week","expected_arrival_week","qty_units","status"])
      .withColumn("order_week",F.to_date("order_week")).withColumn("expected_arrival_week",F.to_date("expected_arrival_week")),"purchase_orders",len(po_rows))

# ---- PRODUCT DEMAND (weekly fact) ----
print("== product_demand (fact) ==")
NDC=len(dcs); NPROD=len(products)
SURGE_OFF=((SURGE_WEEK-START).days//7)
SO=SURGE_OFF
grid=[(p,w,d) for p in range(1,NPROD+1) for w in range(NWEEKS) for d in range(1,NDC+1)]
grid_df=spark.createDataFrame(grid,["product_id","week_off","dc_id"])

@F.pandas_udf(IntegerType())
def demand_udf(product_id, week_off, dc_id):
    import numpy as np, pandas as pd
    rng=np.random.default_rng()
    base={1:5200,2:1800,3:2600,4:4200,5:2200}    # City E-Bike biggest
    dc_w=[0.18,0.16,0.14,0.12,0.10,0.08,0.12,0.10]
    out=[]
    for p,w,d in zip(product_id,week_off,dc_id):
        p=int(p); w=int(w); d=int(d)
        b=base[p]*dc_w[d-1]
        trend=1.0+0.0015*w
        seas=1.0+0.06*np.sin(2*np.pi*w/52.0)
        val=b*trend*seas
        # --- THE SURGE: City E-Bike ramps hard from SO, ONLY in EMEA (DCs 1-3) ---
        # A new EMEA market opened, so only EMEA demand jumps; other regions stay flat.
        if p==1 and w>=SO and d in (1,2,3):
            ramp=min(1.0,(w-SO)/6.0)
            val=val*(1.0+ramp*2.4)
        val=val*rng.uniform(0.92,1.08)
        out.append(int(max(val,0)))
    return pd.Series(out)

demand=grid_df.withColumn("demand_units", demand_udf(F.col("product_id"),F.col("week_off"),F.col("dc_id")))
demand=(demand
  .withColumn("week", F.expr(f"date_add(DATE'{START}', CAST(week_off*7 AS INT))"))
  .withColumn("demand_id", F.monotonically_increasing_id())
  .drop("week_off"))
demand.write.mode("overwrite").parquet(f"{VOL}/product_demand")
print(f"   product_demand: {spark.read.parquet(f'{VOL}/product_demand').count()} rows")
print(f"\nDONE. tables in {VOL}")
