# Let's make sure incorrect input rows (bad schema) are dropped

from pyspark import pipelines as dp
from pyspark.sql.functions import count, col

@dp.materialized_view(
    name="TEST_user_bronze_ldp",
    comment="TEST: bronze table properly drops row with incorrect schema",
    private=True
)
@dp.expect_or_fail("incorrect_data_removed", "not_empty_rescued_data = 0")
def TEST_user_bronze_ldp():
    return (
        spark.read.table("user_bronze_ldp")
        .filter(col("_rescued_data").isNotNull() | (col("email") == "margaret84@example.com"))
        .agg(count("*").alias("not_empty_rescued_data"))
    )

