from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ==========================================================================
# MATERIALIZED VIEW: bikes
# ==========================================================================
@dp.materialized_view(
    name="bikes",
    comment="Daily aggregated bike-level metrics including total rides, revenue, first start station, last end station, and a flag indicating if maintenance was reported for the bike on that day."
)
def bikes():
    r = spark.read.table("rides").alias("r")
    ml = spark.read.table("maintenance_logs").alias("ml")

    joined = r.join(
        ml,
        (F.col("r.bike_id") == F.col("ml.bike_id")) &
        (F.col("ml.maintenance_date") == F.col("r.ride_date")),
        "left"
    )

    agg = joined.groupBy("r.ride_date", "r.bike_id").agg(
        F.countDistinct("r.ride_id").alias("total_rides"),
        F.sum("r.ride_revenue").alias("total_revenue_raw"),
        F.expr("min_by(r.start_station_id, r.start_time)").alias("start_station_id"),
        F.expr("max_by(r.end_station_id, r.end_time)").alias("end_station_id"),
        F.max(F.when(F.col("ml.maintenance_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("requires_maintenance_int")
    )

    return (
        agg.select(
            F.col("r.ride_date").alias("ride_date"),
            F.col("r.bike_id").alias("bike_id"),
            F.col("total_rides"),
            F.col("total_revenue_raw").cast("DECIMAL(19,4)").alias("total_revenue"),
            F.col("start_station_id"),
            F.col("end_station_id"),
            (F.col("requires_maintenance_int") > 0).cast("boolean").alias("requires_maintenance")
        )
    )

# ==========================================================================
# MATERIALIZED VIEW: stations
# ==========================================================================
@dp.materialized_view(
    name="stations",
    comment="Daily station-level metrics including rides originating, rides terminating, end-of-day bike inventory, and revenue generated from rides starting or ending at the station."
)
def stations():
    r = spark.read.table("rides")

    starts = (
        r.groupBy("ride_date", "start_station_id")
         .agg(
             F.sum("ride_revenue").alias("total_revenue"),
             F.count(F.lit(1)).alias("total_rides")
         )
         .withColumnRenamed("start_station_id", "station_id")
    )

    ends = (
        r.groupBy("ride_date", "end_station_id")
         .agg(
             F.sum("ride_revenue").alias("total_revenue"),
             F.count(F.lit(1)).alias("total_rides")
         )
         .withColumnRenamed("end_station_id", "station_id")
    )

    bikes_mv = spark.read.table("bikes")
    inventory = (
        bikes_mv.groupBy("ride_date", "end_station_id")
                .agg(F.count(F.lit(1)).alias("total_bikes"))
                .withColumnRenamed("end_station_id", "station_id")
    )

    s = starts.alias("s")
    e = ends.alias("e")
    i = inventory.alias("i")

    joined = (
        s.join(e, (F.col("s.station_id") == F.col("e.station_id")) & (F.col("s.ride_date") == F.col("e.ride_date")), "full")
         .join(
             i,
             (F.coalesce(F.col("s.station_id"), F.col("e.station_id")) == F.col("i.station_id")) &
             (F.coalesce(F.col("s.ride_date"), F.col("e.ride_date")) == F.col("i.ride_date")),
             "left"
         )
    )

    return (
        joined.select(
            F.coalesce(F.col("s.ride_date"), F.col("e.ride_date")).alias("ride_date"),
            F.coalesce(F.col("s.station_id"), F.col("e.station_id")).alias("station_id"),
            F.coalesce(F.col("s.total_rides"), F.lit(0)).alias("total_rides_as_origin"),
            F.coalesce(F.col("e.total_rides"), F.lit(0)).alias("total_rides_as_destination"),
            F.coalesce(F.col("i.total_bikes"), F.lit(0)).alias("end_of_day_inventory"),
            F.coalesce(F.col("s.total_revenue"), F.lit(0)).cast("DECIMAL(19,4)").alias("total_revenue_as_origin"),
            F.coalesce(F.col("e.total_revenue"), F.lit(0)).cast("DECIMAL(19,4)").alias("total_revenue_as_destination"),
        )
    )

# ==========================================================================
# MATERIALIZED VIEW: maintenance_events
# ==========================================================================
@dp.materialized_view(
    name="maintenance_events",
    comment="Maintenance log-level metrics including the number of days taken to resolve an issue and an estimate of typical revenue per bike during the downtime."
)
def maintenance_events():
    ml = spark.read.table("maintenance_logs").alias("ml")
    r = spark.read.table("rides").alias("r")

    window_join = ml.join(
        r,
        (F.col("r.start_time") > F.col("ml.reported_time")) & (F.col("r.start_time") < F.col("ml.resolved_time")),
        "inner"
    )

    agg = window_join.groupBy("ml.maintenance_id").agg(
        F.datediff(F.max("ml.resolved_time"), F.min("ml.reported_time")).alias("days_to_resolve"),
        (F.sum("r.ride_revenue") / F.countDistinct("r.bike_id")).alias("revenue_lost_raw")
    )

    return agg.select(
        F.col("ml.maintenance_id").alias("maintenance_id"),
        F.col("days_to_resolve").cast("int").alias("days_to_resolve"),
        F.col("revenue_lost_raw").cast("DECIMAL(19,4)").alias("revenue_lost")
    )