from pyspark import pipelines as dp
from config import get_rules


spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")
catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")


@dp.table(comment="Raw user data - Test")
def raw_user_data():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaHints", "id int")
      .load(f"/Volumes/main__build/dbdemos_ldp_unit_test/raw_data/test/users_json/*.json"))


from pyspark import pipelines as dp

@dp.table(comment="Raw spend data - Test")
def raw_spend_data():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaHints", "id int, age int, annual_income float, spending_core float")
    .load(f"/Volumes/main__build/dbdemos_ldp_unit_test/raw_data/test/spend_csv/*.csv"))

  # Ingest raw User stream data in incremental mode

@dp.table(comment="Raw user data")
@dp.expect_all_or_drop(get_rules('user_bronze_ldp')) #get the rules from our centralized table.
def user_bronze_ldp():
  return spark.readStream.table("raw_user_data")