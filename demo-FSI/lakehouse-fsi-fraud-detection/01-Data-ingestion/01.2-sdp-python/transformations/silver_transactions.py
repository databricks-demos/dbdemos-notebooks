import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="silver_transactions",
    comment="Enforce quality and materialize our tables for Data Analysts"
)
@dlt.expect("correct_data", "id IS NOT NULL")
@dlt.expect("correct_customer_id", "customer_id IS NOT NULL")
def silver_transactions():
    t = dlt.read_stream("bronze_transactions").alias("t")
    f = dlt.read("fraud_reports").alias("f")

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