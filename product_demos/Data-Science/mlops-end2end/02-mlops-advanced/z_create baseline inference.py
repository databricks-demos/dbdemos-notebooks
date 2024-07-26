# Databricks notebook source
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create/Materialize baseline table (for ad-hoc model monitoring)

# COMMAND ----------

# Convert test/baseline pandas dataframe into pyspark dataframe
test_baseline_df = spark.createDataFrame(y_test.reset_index())

# COMMAND ----------

baseline_table_name = f"{catalog}.{dbName}.{inference_table_name}_baseline"
baseline_model_version = client.get_model_version_by_alias(name=model_name, alias="Baseline").version # Champion

baseline_predictions_df = fe.score_batch(
    df=test_baseline_df,
    model_uri=f"models:/{model_name}/{baseline_model_version}",
    result_type=test_baseline_df.schema[label_col].dataType
  ).withColumn("Model_Version", F.lit(baseline_model_version))

(
  baseline_predictions_df.drop(timestamp_col)
  .write
  .format("delta")
  .mode("overwrite") # "append" also works if baseline evolves
  .option("overwriteSchema",True)
  .option("delta.enableChangeDataFeed", "true")
  .saveAsTable(baseline_table_name)
)
