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
      rides.append({
          "ride_id": str(uuid.uuid4()),
          "start_time": start_time + timedelta(seconds=ride_times[i]),
          "end_time": start_time + timedelta(seconds=ride_times[i+1]),
          "start_station_id": current_station,
          "end_station_id": random.choice(stations),
          "bike_id": bike,
          "user_type": random.choice(["member", "non-member"])
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


rides_df.display()
maintenance_logs_df.display()
weather_data_df.display()

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

from pyspark.sql.functions import expr
import pandas as pd

dbutils.fs.rm(f"{volume_path}/rides", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/rides")
dbutils.fs.rm(f"{volume_path}/maintenance_logs", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/maintenance_logs")
dbutils.fs.rm(f"{volume_path}/weather", recurse=True)
dbutils.fs.mkdirs(f"{volume_path}/weather")


def write_to_csv(key: tuple[str], pdf: pd.DataFrame) -> pd.DataFrame:
    pdf.to_csv(key[0], index=False)
    return pd.DataFrame(data={"file": [key[0]], "count": [pdf.shape[0]]})

def write_to_json(key: tuple[str], pdf: pd.DataFrame) -> pd.DataFrame:
    pdf.to_json(key[0], orient="records")
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
