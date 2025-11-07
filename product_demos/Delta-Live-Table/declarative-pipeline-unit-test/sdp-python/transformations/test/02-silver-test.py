from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, when, sum as _sum

@dp.materialized_view(
    name="TEST_user_silver_ldp_anonymize",
    comment="TEST: check silver table removes null ids and anonymize emails",
    private=True
)
@dp.expect_all_or_fail({
    "keep_all_rows": "num_rows = 4",
    "email_should_be_anonymized": "clear_email = 0",
    "null_ids_removed": "null_id_count = 0"
})
def TEST_user_silver_ldp_anonymize():
    df = spark.read.table("user_silver_ldp")
    return (
        df.select(
            count("*").alias("num_rows"),
            _sum(when(col("email").like("%@%"), 1).otherwise(0)).alias("clear_email"),
            _sum(when(col("id").isNull(), 1).otherwise(0)).alias("null_id_count")
        )
    )