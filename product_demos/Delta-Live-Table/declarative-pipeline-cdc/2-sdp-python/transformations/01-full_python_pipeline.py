## This file implements the same logic in python as the SQL version

from pyspark import pipelines as dp
from pyspark.sql.functions import *


# -------------------------------------------------------------------
# --- 1. Ingest data with autoloader: loop on all folders -----------
# -------------------------------------------------------------------
# Let's loop over all the folders and dynamically generate our SDP pipeline.

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

def create_pipeline(table_name):
    print(f"Building SDP CDC pipeline for {table_name}")

    ##Raw CDC Table
    # .option("cloudFiles.maxFilesPerTrigger", "1")
    @dp.table(
        name=table_name + "_cdc",
        comment=f"New {table_name} data incrementally ingested from cloud object storage landing zone",
    )
    def raw_cdc():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"/Volumes/{catalog}/{schema}/raw_data/" + table_name)
        )

    ##Clean CDC input and track quality with expectations
    @dp.temporary_view(
        name=table_name + "_cdc_clean",
        comment="Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type",
    )
    @dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
    @dp.expect_or_drop("valid_id", "id IS NOT NULL")
    @dp.expect_or_drop("valid_operation", "operation IN ('APPEND', 'DELETE', 'UPDATE')")
    def raw_cdc_clean():
        return spark.readStream.table(table_name + "_cdc")

    ##Materialize the final table
    dp.create_streaming_table(name=table_name, comment="Clean, materialized " + table_name)
    dp.create_auto_cdc_flow(
        target=table_name,  # The customer table being materilized
        source=table_name + "_cdc_clean",  # the incoming CDC
        keys=["id"],  # what we'll be using to match the rows to upsert
        sequence_by=col("operation_date"),  # we deduplicate by operation date getting the most recent value
        ignore_null_updates=False,
        apply_as_deletes=expr("operation = 'DELETE'"),  # DELETE condition
        except_column_list=["operation", "operation_date", "_rescued_data"], # in addition we drop metadata columns
    )

for folder in dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/raw_data"):
    table_name = folder.name[:-1]
    create_pipeline(table_name)


# ---------------------------------------------------------------
# --- -- Slowly Changing Dimension of type 2 (SCD2) -------------
# ---------------------------------------------------------------

# create the table
dp.create_streaming_table(
    name="SCD2_customers", comment="Slowly Changing Dimension Type 2 for customers"
)

# store all changes as SCD2
dp.create_auto_cdc_flow(
    target="SCD2_customers",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="2",
)  # Enable SCD2 and store individual updates