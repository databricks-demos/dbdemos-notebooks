from pyspark import pipelines as dp
from pyspark.sql.functions import expr

# ==========================================================================
# STREAMING TABLE: rides
# ==========================================================================
@dp.table(name="rides", comment="Streaming table containing processed ride data from bike shares.")
@dp.expect_or_drop("invalid_ride_duration", "DATEDIFF(MINUTE, start_time, end_time) > 0")
def rides():
    return (
        spark.readStream.table("RIDES_RAW")
        .selectExpr(
            "DATE(start_time) AS ride_date",
            "ride_id",
            "start_time",
            "end_time",
            "start_station_id",
            "end_station_id",
            "bike_id",
            "user_type",
        )
        .withColumn(
            "ride_revenue",
            expr(
                """
                CASE WHEN user_type = 'member'
                     THEN (DATEDIFF(MINUTE, start_time, end_time) / 60.0) * 10.0
                     ELSE (DATEDIFF(MINUTE, start_time, end_time) / 60.0) * 15.0
                END
                """
            ).cast("DECIMAL(19,4)")
        )
    )

# ==========================================================================
# STREAMING TABLE: maintenance_logs
# ==========================================================================
@dp.table(
    name="maintenance_logs",
    comment="Streaming table containing processed maintenance logs for bikes, including AI classified issue types."
)
@dp.expect_or_drop("no_maintenance_description", "issue_description IS NOT NULL")
@dp.expect("short_maintenance_description", "length(issue_description) > 10")
def maintenance_logs():
    return (
        spark.readStream.table("MAINTENANCE_LOGS_RAW")
        .selectExpr(
            "DATE(reported_time) AS maintenance_date",
            "maintenance_id",
            "bike_id",
            "reported_time",
            "resolved_time",
            "issue_description",
        )
        # Call the SQL AI function from Python via expr
        .withColumn(
            "issue_type",
            expr("AI_CLASSIFY(issue_description, array('brakes','chains_pedals','tires','other'))")
        )
    )

# ==========================================================================
# STREAMING TABLE: weather
# ==========================================================================
@dp.table(name="weather", comment="Streaming table containing processed weather data, converted to standard units.")
def weather():
    return (
        spark.readStream.table("WEATHER_RAW")
        .selectExpr(
            "DATE(FROM_UNIXTIME(`timestamp`)) AS weather_date",
            "temperature_f AS temperature",
            "rainfall_in AS rainfall",
            "wind_speed_mph AS wind_speed"
        )
    )

# ==========================================================================
# AUTO CDC: customers (SCD Type 2 Table)
# ==========================================================================
# Create the SCD2 target streaming table with required START/END columns
dp.create_streaming_table(
    name="customers",
    comment="Customer data with SCD Type 2 tracking for maintaining complete change history",
    schema="""
      customer_id STRING,
      user_type STRING,
      registration_date STRING,
      email STRING,
      phone STRING,
      age_group STRING,
      membership_tier STRING,
      preferred_payment STRING,
      home_station_id STRING,
      is_active BOOLEAN,
      __START_AT TIMESTAMP,
      __END_AT TIMESTAMP
    """
)

# Define the AUTO CDC flow (SCD Type 2) from the raw CDC stream into the target
dp.create_auto_cdc_flow(
    target="customers",
    source="CUSTOMERS_CDC_RAW",
    keys=["customer_id"],
    sequence_by=expr("to_timestamp(event_timestamp, 'MM-dd-yyyy HH:mm:ss')"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "event_timestamp", "_rescued_data"],
    stored_as_scd_type="2"
)