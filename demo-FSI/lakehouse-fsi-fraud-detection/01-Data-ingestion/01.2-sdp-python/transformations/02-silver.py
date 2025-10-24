# -- ----------------------------------
# -- Clean and transform transaction data
# -- - Clean up country codes (remove "--" characters)
# -- - Calculate balance differences for originating and destination accounts
# -- - Join with fraud reports to add fraud labels
# -- - Enforce data quality with expectations on critical fields
# -- Creates clean dataset ready for analytics and ML feature engineering
# -- ----------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver_transactions",
    comment="Enforce quality and materialize our tables for Data Analysts"
)
@dp.expect("correct_data", "id IS NOT NULL")
@dp.expect("correct_customer_id", "customer_id IS NOT NULL")
def silver_transactions():
    t = spark.readStream.table("bronze_transactions").alias("t")
    f = spark.read.table("fraud_reports").alias("f")

    joined = t.join(f, on="id", how="left")

    # t.* EXCEPT(countryOrig, countryDest, t._rescued_data)
    t_cols = [
        F.col(f"t.`{c}`") 
        for c in t.columns 
        if c not in ["countryOrig", "countryDest", "_rescued_data"]
    ]

    # f.* EXCEPT(id, f._rescued_data)
    f_cols = [
        F.col(f"f.`{c}`") 
        for c in f.columns 
        if c not in ["id", "_rescued_data"]
    ]

    return joined.select(
        *t_cols,
        *f_cols,
        F.regexp_replace(F.col("t.countryOrig"), "--", "").alias("countryOrig"),
        F.regexp_replace(F.col("t.countryDest"), "--", "").alias("countryDest"),
        (F.col("t.newBalanceOrig") - F.col("t.oldBalanceOrig")).alias("diffOrig"),
        (F.col("t.newBalanceDest") - F.col("t.oldBalanceDest")).alias("diffDest"),
    )