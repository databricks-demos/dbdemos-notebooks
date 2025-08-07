# Databricks notebook source
# MAGIC %md
# MAGIC ## Generate Eval dataset

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

spark.sql(f'use catalog {catalog}')
spark.sql(f'use schema {dbName}')

# COMMAND ----------

# MAGIC %md
# MAGIC Generate a synthetic evaluation dataset for customer data and billing questions.

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, pandas_udf, concat_ws, lit, col, from_json, struct, expr
from pyspark.sql.types import StringType, ArrayType
import pandas as pd

# Step 1: Load 20 customers
df = spark.table("customers").limit(20).withColumn("row_id", monotonically_increasing_id())

# Step 2: Generate question using 20 templates
@pandas_udf(StringType())
def generate_question(email: pd.Series, row_id: pd.Series) -> pd.Series:
    templates = [
        "What is the phone number of {email}?",
        "List all orders placed by {email}.",
        "What is the current subscription status for {email}?",
        "Show billing details for {email}.",
        "Does {email} have any unpaid invoices?",
        "Which products did {email} purchase?",
        "What is the loyalty tier of {email}?",
        "When did {email} register as a customer?",
        "Summarize all subscriptions held by {email}.",
        "What city does {email} live in?",
        "What is the current account status of {email}?",
        "How many years has {email} been a customer?",
        "What is the churn risk score for {email}?",
        "What is the customer value score for {email}?",
        "Is autopay enabled for {email}'s account?",
        "How many late payments has {email} had?",
        "What is the zip code of {email}?",
        "What type of customer is {email} (e.g., Individual, Business)?",
        "What is the full address of {email}?"
    ]
    return pd.Series([
        templates[int(i) % len(templates)].format(email=e)
        for i, e in zip(row_id, email)
    ])

df = df.withColumn("question", generate_question("email", "row_id"))

# Step 3: Convert to Pandas then back to Spark to finalize UDF materialization
df_pd = df.toPandas()
df_clean = spark.createDataFrame(df_pd)

# Step 4: Build prompt for AI_QUERY
df_clean = df_clean.withColumn(
    "prompt",
    concat_ws(
        " ",
        lit("You are evaluating an AI system."),
        lit("Based on the following customer record:"),
        concat_ws(", ",
            df_clean.first_name, df_clean.last_name, df_clean.email, df_clean.phone,
            df_clean.address, df_clean.city, df_clean.state, df_clean.zip_code,
            df_clean.customer_segment, df_clean.registration_date.cast("string"),
            df_clean.customer_status, df_clean.loyalty_tier,
            df_clean.tenure_years.cast("string"), df_clean.churn_risk_score.cast("string"),
            df_clean.customer_value_score.cast("string")
        ),
        lit("Generate a JSON array of factual statements (expected_facts) that should be included in the correct answer to the following question. Each item must be a complete, natural language sentence. Return only a valid JSON array of strings, nothing else."),
        lit("Question:"), df_clean.question
    )
)

# Step 5: Register and call AI_QUERY
df_clean.createOrReplaceTempView("customer_test_questions")

final_df_raw = spark.sql("""
SELECT 
  question,
  AI_QUERY("databricks-claude-3-7-sonnet", prompt) AS expected_facts_json
FROM customer_test_questions
""")

# Step 6: Parse JSON string into Array<String>
final_df = final_df_raw.withColumn(
    "expected_facts",
    from_json(col("expected_facts_json"), ArrayType(StringType()))
)

# Step 7: Build structured evaluation format
eval_df = final_df.withColumn("inputs", struct("question")) \
                  .withColumn("predictions", lit("")) \
                  .withColumn("expectations", struct("expected_facts")) \
                  .select("inputs", "predictions", "expectations")

# Step 8: Save
eval_df.write.format('json').mode("overwrite").save(f"/Volumes/{catalog}/{dbName}/{volume_name}/eval_dataset")

# COMMAND ----------

display(eval_df)
