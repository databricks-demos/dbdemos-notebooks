# -- ----------------------------------
# -- Create enriched transaction dataset for ML and analytics
# -- - Join transactions with customer information
# -- - Enrich with geographic coordinates for originating and destination countries
# -- - Convert fraud flag to boolean type
# -- - Validate transaction amounts
# -- Creates comprehensive feature set for fraud detection models
# -- ----------------------------------


from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gold_transactions",
    comment="Gold, ready for Data Scientists to consume"
)
@dp.expect("amount_decent", "amount > 10")
def gold_transactions():
    t = spark.read.table("silver_transactions").alias("t")
    o = spark.read.table("country_coordinates").alias("o")
    d = spark.read.table("country_coordinates").alias("d")
    c = spark.read.table("banking_customers").alias("c")

    joined = (
        t.join(o, F.col("t.countryOrig") == F.col("o.alpha3_code"), "inner")
         .join(d, F.col("t.countryDest") == F.col("d.alpha3_code"), "inner")
         .join(c, F.col("c.id") == F.col("t.customer_id"), "inner")
    )

    # t.* EXCEPT(countryOrig, countryDest, is_fraud)
    t_cols = [F.col(f"t.`{col}`") for col in t.columns if col not in ["countryOrig", "countryDest", "is_fraud"]]

    # c.* EXCEPT(id, _rescued_data)
    c_cols = [F.col(f"c.`{col}`") for col in c.columns if col not in ["id", "_rescued_data"]]

    return joined.select(
        *t_cols,
        *c_cols,
        F.coalesce(F.col("t.is_fraud"), F.lit(0)).cast("boolean").alias("is_fraud"),
        F.col("o.alpha3_code").alias("countryOrig"),
        F.col("o.country").alias("countryOrig_name"),
        F.col("o.long_avg").alias("countryLongOrig_long"),
        F.col("o.lat_avg").alias("countryLatOrig_lat"),
        F.col("d.alpha3_code").alias("countryDest"),
        F.col("d.country").alias("countryDest_name"),
        F.col("d.long_avg").alias("countryLongDest_long"),
        F.col("d.lat_avg").alias("countryLatDest_lat"),
    )