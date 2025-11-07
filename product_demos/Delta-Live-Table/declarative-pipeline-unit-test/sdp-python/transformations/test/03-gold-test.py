from pyspark import pipelines as dp
from pyspark.sql.functions import count

@dp.materialized_view(
    name="TEST_user_gold_ldp",
    comment="TEST: check that gold table only contains unique customer id",
    private=True
)
@dp.expect_or_fail("pk_must_be_unique", "duplicate = 1")
def TEST_user_gold_ldp():
    return (
        spark.read.table("user_gold_ldp")
        .groupBy("id")
        .agg(count("*").alias("duplicate"))
    )