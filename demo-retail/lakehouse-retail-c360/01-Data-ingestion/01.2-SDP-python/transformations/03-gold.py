from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ----------------------------------
# Create ML features by aggregating and joining data
# Enrich user data with behavioral and transaction metrics
# Features include:
# - Order statistics: count, total amount, items, last transaction date
# - App event statistics: platform, event count, session count, last event
# - Temporal features: days since creation, last activity, last event
# ----------------------------------
@dp.materialized_view(comment="Final user table with all information for Analysis / ML")
def churn_features():
  churn_app_events_stats_df = (
          spark.read.table("churn_app_events")
          .groupby("user_id")
          .agg(F.first("platform").alias("platform"),
               F.count('*').alias("event_count"),
               F.count_distinct("session_id").alias("session_count"),
               F.max(F.to_timestamp("date", "MM-dd-yyyy HH:mm:ss")).alias("last_event"))
                              )

  churn_orders_stats_df = (
          spark.read.table("churn_orders")
          .groupby("user_id")
          .agg(F.count('*').alias("order_count"),
               F.sum("amount").alias("total_amount"),
               F.sum("item_count").alias("total_item"),
               F.max("creation_date").alias("last_transaction"))
         )

  return (
          spark.read.table("churn_users")
          .join(churn_app_events_stats_df, on="user_id")
          .join(churn_orders_stats_df, on="user_id")
          .withColumn("days_since_creation", F.datediff(F.current_timestamp(), F.col("creation_date")))
          .withColumn("days_since_last_activity", F.datediff(F.current_timestamp(), F.col("last_activity_date")))
          .withColumn("days_last_event", F.datediff(F.current_timestamp(), F.col("last_event")))
         )

# ----------------------------------
# Load ML model and register as UDF
# Load the churn prediction model from MLflow registry
# and register it for use in predictions
# ----------------------------------
import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                                                     Stage/version
#                                                                                   Model name               |
#                                                                                       |                    |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/main__build.dbdemos_retail_c360.dbdemos_customer_churn@prod", "long", env_manager='virtualenv')
spark.udf.register("predict_churn", predict_churn_udf)

# ----------------------------------
# Apply ML model to predict customer churn
# Uses the predict_churn UDF to score each customer
# Identifies customers at risk of churning based on their behavioral and demographic features
# ----------------------------------
model_features = predict_churn_udf.metadata.get_input_schema().input_names()

@dp.materialized_view(comment="Customer at risk of churn")
def churn_prediction():
  return (
          spark.read.table('churn_features')
          .withColumn('churn_prediction', predict_churn_udf(*model_features)))
