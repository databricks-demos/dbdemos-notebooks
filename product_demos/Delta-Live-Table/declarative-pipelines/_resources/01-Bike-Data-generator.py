# Databricks notebook source
# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

#Note: we do not recommend to change the catalog here as it won't impact all the demo resources such as DLT pipeline and Dashboards.
#Instead, please re-install the demo with a specific catalog and schema using dbdemos.install("lakehouse-retail-c360", catalog="..", schema="...")
catalog = "main__build"
schema = dbName = db = "dbdemos_pipeline_bike"

volume_name = "raw_data"

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False, volume_name)
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, BooleanType

# Bike Ride Logs (Raw GPS/Usage Data)
ride_log_schema = StructType([
    StructField("ride_id", StringType()),  # Unique ride identifier
    StructField("start_time", TimestampType()),  # Ride start timestamp
    StructField("end_time", TimestampType()),  # Ride end timestamp
    StructField("start_station_id", StringType()),
    StructField("end_station_id", StringType()),
    StructField("bike_id", StringType()),  # Bike identifier
    StructField("user_type", StringType()),  # e.g., "member", "casual"
    StructField("customer_id", StringType()),  # Links to customer data
])

# Bike Maintenance Logs (Raw Repair Data)
maintenance_log_schema = StructType([
    StructField("maintenance_id", StringType()),
    StructField("bike_id", StringType()),
    StructField("issue_description", StringType()),  # e.g., "flat tire"
    StructField("reported_time", TimestampType()),
    StructField("resolved_time", TimestampType()),
])

# Weather Data
weather_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("temperature_f", FloatType()),
    StructField("rainfall_in", FloatType()),
    StructField("wind_speed_mph", FloatType())
])

# Customer CDC Data Schema
customer_cdc_schema = StructType([
    StructField("customer_id", StringType()),  # Unique customer identifier
    StructField("user_type", StringType()),  # "member" or "non-member" - links to rides
    StructField("registration_date", StringType()),  # When customer registered
    StructField("email", StringType()),  # Customer email
    StructField("phone", StringType()),  # Customer phone
    StructField("age_group", StringType()),  # "18-25", "26-35", "36-45", "46-55", "55+"
    StructField("membership_tier", StringType()),  # "basic", "premium", "enterprise" (for members)
    StructField("preferred_payment", StringType()),  # "credit_card", "mobile_pay", "cash"
    StructField("home_station_id", StringType()),  # Preferred/home station
    StructField("is_active", BooleanType()),  # Whether customer is currently active
    StructField("operation", StringType()),  # "APPEND", "DELETE", "UPDATE", None
    StructField("event_timestamp", StringType()),  # When the CDC event occurred
])


# COMMAND ----------

import uuid
import random
from datetime import datetime, timedelta, date

days_to_generate = 100
max_rides_per_day = 10
odds_of_maintenance = 1.0/50.0

bikes = [str(uuid.uuid4()) for _ in range(200)]
stations = [str(uuid.uuid4()) for _ in range(25)]

# Add bikes to fleet over time
date_bike_added = {
    # Normal distrubution, where mean is 3/4 of the total days, with a standard deviation of 1/10
    bike_id: max(0, min(int(round(random.normalvariate(days_to_generate * .75, days_to_generate * .10))), days_to_generate - 1 if days_to_generate > 0 else 0))
    # We'll start with 50 bikes
    for bike_id in bikes[50:]
}

# Generate Customer CDC Data with realistic CDC scenarios
age_groups = ["18-25", "26-35", "36-45", "46-55", "55+"]
membership_tiers = ["basic", "premium", "enterprise"]
payment_methods = ["credit_card", "mobile_pay", "cash"]

customers_cdc = []
all_customer_ids = []

# Generate base customers first
base_customers = []

# Generate 800 member customers
for i in range(800):
    customer_id = str(uuid.uuid4())
    reg_date = datetime.now() - timedelta(days=random.randint(30, 365*2))
    
    base_customer = {
        "customer_id": customer_id,
        "user_type": "member",
        "registration_date": reg_date.strftime("%m-%d-%Y %H:%M:%S"),
        "email": f"member{i}@bikerent.com",
        "phone": f"555-{random.randint(1000, 9999)}",
        "age_group": random.choice(age_groups),
        "membership_tier": random.choice(membership_tiers),
        "preferred_payment": random.choice(payment_methods),
        "home_station_id": random.choice(stations),
        "is_active": True
    }
    base_customers.append(base_customer)
    all_customer_ids.append(customer_id)

# Generate 1200 non-member customers
for i in range(1200):
    customer_id = str(uuid.uuid4())
    reg_date = datetime.now() - timedelta(days=random.randint(1, 90))
    
    base_customer = {
        "customer_id": customer_id,
        "user_type": "non-member",
        "registration_date": reg_date.strftime("%m-%d-%Y %H:%M:%S"),
        "email": f"casual{i}@email.com",
        "phone": f"555-{random.randint(1000, 9999)}",
        "age_group": random.choice(age_groups),
        "membership_tier": None,
        "preferred_payment": random.choice(payment_methods),
        "home_station_id": random.choice(stations),
        "is_active": True
    }
    base_customers.append(base_customer)
    all_customer_ids.append(customer_id)

# Now generate CDC events for these customers
for customer in base_customers:
    customer_id = customer["customer_id"]
    
    # Start with APPEND operation (initial insert)
    initial_event_time = datetime.now() - timedelta(days=random.randint(0, days_to_generate))
    append_event = customer.copy()
    append_event.update({
        "operation": "APPEND",
        "event_timestamp": initial_event_time.strftime("%m-%d-%Y %H:%M:%S")
    })
    customers_cdc.append(append_event)
    
    # 10% chance this customer will have additional operations
    if random.random() < 0.1:
        # Generate UPDATE operation
        update_event_time = initial_event_time + timedelta(days=random.randint(1, 30))
        update_event = customer.copy()
        
        # Randomly update some fields
        if random.random() < 0.3:
            update_event["email"] = f"updated_{customer['email']}"
        if random.random() < 0.2:
            update_event["phone"] = f"555-{random.randint(1000, 9999)}"
        if random.random() < 0.1:
            update_event["preferred_payment"] = random.choice(payment_methods)
        
        update_event.update({
            "operation": "UPDATE",
            "event_timestamp": update_event_time.strftime("%m-%d-%Y %H:%M:%S")
        })
        customers_cdc.append(update_event)
        
        # 3% chance this customer will also be deleted
        if random.random() < 0.03:
            delete_event_time = update_event_time + timedelta(days=random.randint(1, 20))
            delete_event = update_event.copy()
            delete_event.update({
                "operation": "DELETE",
                "event_timestamp": delete_event_time.strftime("%m-%d-%Y %H:%M:%S")
            })
            customers_cdc.append(delete_event)

# Add some standalone operations for variety (5% of total)
standalone_operations = int(len(base_customers) * 0.05)
for i in range(standalone_operations):
    customer_id = str(uuid.uuid4())
    event_time = datetime.now() - timedelta(days=random.randint(0, days_to_generate))
    operation = random.choice(["APPEND", "UPDATE", "DELETE"])
    
    standalone_event = {
        "customer_id": customer_id,
        "user_type": random.choice(["member", "non-member"]),
        "registration_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%m-%d-%Y %H:%M:%S"),
        "email": f"standalone{i}@email.com",
        "phone": f"555-{random.randint(1000, 9999)}",
        "age_group": random.choice(age_groups),
        "membership_tier": random.choice(membership_tiers) if random.random() < 0.4 else None,
        "preferred_payment": random.choice(payment_methods),
        "home_station_id": random.choice(stations),
        "is_active": random.choice([True, False]),
        "operation": operation,
        "event_timestamp": event_time.strftime("%m-%d-%Y %H:%M:%S")
    }
    customers_cdc.append(standalone_event)

print(f"Generated {len(customers_cdc)} CDC events for {len(base_customers)} base customers")
print(f"Total customer IDs available for rides: {len(all_customer_ids)}")


rides = []
maintenance_logs= []

for bike in bikes:
  current_station = random.choice(stations)
  remaining_maintenance = 0
  current_maintenance = None
  for day in reversed(range(days_to_generate)):
    # Skip day if bike hasn't been added to the fleet yet
    if (days_to_generate - day) < date_bike_added.get(bike, 0):
      continue
    current_date = datetime.now() - timedelta(days=day)
    start_time = datetime.combine(current_date, datetime.min.time())
    end_time = datetime.combine(current_date, datetime.max.time())

    # If there is a maintenance event, skip the day
    if remaining_maintenance > 0:
      remaining_maintenance -= 1
      if remaining_maintenance == 0:
        current_maintenance["resolved_time"] = start_time
        maintenance_logs.append(current_maintenance)
        current_maintenance = None
      continue
    
    # Generate a random number of trips in a day for this bike
    trips_in_day = random.randint(1, max_rides_per_day)

    # Generate a random list of start/end times for each trip
    total_seconds = (end_time - start_time).total_seconds()
    ride_times = sorted([random.randint(0, int(total_seconds)) for _ in range(2 * trips_in_day)])
    for i in range(0, 2 * trips_in_day, 2):
      # Randomly assign user type and customer ID
      user_type = random.choice(["member", "non-member"])
      customer_id = random.choice(all_customer_ids) if all_customer_ids else str(uuid.uuid4())
      
      rides.append({
          "ride_id": str(uuid.uuid4()),
          "start_time": start_time + timedelta(seconds=ride_times[i]),
          "end_time": start_time + timedelta(seconds=ride_times[i+1]),
          "start_station_id": current_station,
          "end_station_id": random.choice(stations),
          "bike_id": bike,
          "user_type": user_type,
          "customer_id": customer_id
      })

      # Random odds of a maintenance event
      if random.random() < odds_of_maintenance:
        current_maintenance = {
          "maintenance_id": str(uuid.uuid4()),
          "bike_id": bike,
          "issue_description": random.choice(["brakes", "chain", "tires", "seat", "handlebars", "safety reflectors"]),
          "reported_time": start_time + timedelta(seconds=ride_times[i+1])
        }
        remaining_maintenance = random.randint(1, 5)
        break
    
      

# San Francisco monthly averages (index 0=Jan, 11=Dec)
monthly_avg = {
    "temp_f": [57, 60, 62, 63, 64, 67, 67, 68, 70, 70, 64, 58],  # Avg highs
    "rain_in": [4.72, 1.35, 2.58, 1.35, 0.48, 0.14, 0.01, 0.04, 0.08, 0.94, 3.0, 3.5],
    "wind_mph": [16, 21, 24, 24, 23, 22, 19, 14, 15, 17, 18, 20],
}

weather_data = []
for day in reversed(range(days_to_generate)):
    current_date = datetime.combine(datetime.now() - timedelta(days=day), datetime.min.time())
    month_idx = current_date.month - 1  # 0-based index

    # Temperature with daily variation and seasonal trend
    temp = random.gauss(monthly_avg["temp_f"][month_idx], 3)
    temp += 5 * (1 + random.random())  # Add daily variation

    # Rainfall with monthly base and chance of showers
    rain = max(0, random.gauss(monthly_avg["rain_in"][month_idx] / 30, 0.05))
    if random.random() < 0.3:  # 30% chance of no rain even if avg >0
        rain = 0.0

    # Wind speed with daily variation
    wind = max(5, random.gauss(monthly_avg["wind_mph"][month_idx], 4))

    weather_data.append(
        {
            "timestamp": current_date,
            "temperature_f": round(float(temp), 1),
            "rainfall_in": round(float(rain), 2),
            "wind_speed_mph": round(float(wind), 1),
        }
    )



rides_df = spark.createDataFrame(rides, ride_log_schema)
maintenance_logs_df = spark.createDataFrame(maintenance_logs, maintenance_log_schema)
weather_data_df = spark.createDataFrame(weather_data, weather_schema)
customers_cdc_df = spark.createDataFrame(customers_cdc, customer_cdc_schema)


rides_df.display()
maintenance_logs_df.display()
weather_data_df.display()
customers_cdc_df.display()

# COMMAND ----------

maintenance_logs_with_descriptions = spark.sql(
    """
select
  * except (issue_description),
  case 
    when rand() > 0.95 then "Broken" -- Inject some random bad data
    when rand() > 0.95 then null
    else ai_query(
      "databricks-meta-llama-3-3-70b-instruct",
      "You are a user of a bicycle rental service that rents bikes by the hour. The bike you just rented has an issue with the " || issue_description || " . You are writing a report of the issue with your bike. Your response should be 2 to 3 sentences. The response should be informal and terse. Your response should not include the bike number.",
      modelParameters => named_struct("temperature", 2, "top_k", 100)
  ) end as issue_description
from
  {maintenance_logs}
""",
    maintenance_logs=maintenance_logs_df,
)

maintenance_logs_with_descriptions.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import expr, to_date
import pandas as pd

dbutils.fs.rm(f"{volume_path}/rides", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/rides")
dbutils.fs.rm(f"{volume_path}/maintenance_logs", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/maintenance_logs")
dbutils.fs.rm(f"{volume_path}/weather", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/weather")
dbutils.fs.rm(f"{volume_path}/customers_cdc", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/customers_cdc")


def write_to_csv(key: tuple[str], pdf: pd.DataFrame) -> pd.DataFrame:
    pdf.to_csv(key[0], index=False)
    return pd.DataFrame(data={"file": [key[0]], "count": [pdf.shape[0]]})

def write_to_json(key: tuple[str], pdf: pd.DataFrame) -> pd.DataFrame:
    pdf.to_json(key[0], orient="records")
    return pd.DataFrame(data={"file": [key[0]], "count": [pdf.shape[0]]})

def write_to_parquet(key: tuple[str], pdf: pd.DataFrame) -> pd.DataFrame:
    pdf.to_parquet(key[0], index=False)
    return pd.DataFrame(data={"file": [key[0]], "count": [pdf.shape[0]]})

rides_df.groupBy(
    expr(f"'{volume_path}/rides/rides_' || date_format(start_time, 'yyyy-MM-dd') || '.csv'")
).applyInPandas(write_to_csv, schema="file string, count int").display()


maintenance_logs_with_descriptions.groupBy(
    expr(f"'{volume_path}/maintenance_logs/maintenance_logs_' || date_format(reported_time, 'yyyy-MM-dd') || '.csv'")
).applyInPandas(write_to_csv, schema="file string, count int").display()


weather_data_df.groupBy(
    expr(f"'{volume_path}/weather/weather_' || date_format(timestamp, 'yyyy-MM-dd') || '.json'")
).applyInPandas(write_to_json, schema="file string, count int").display()

# Partition customer CDC files by date (without adding event_date column to final data)
customers_cdc_df.groupBy(
    expr(f"'{volume_path}/customers_cdc/customers_cdc_' || date_format(to_date(event_timestamp, 'MM-dd-yyyy HH:mm:ss'), 'yyyy-MM-dd') || '.parquet'")
).applyInPandas(write_to_parquet, schema="file string, count int").display()
