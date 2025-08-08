# Databricks notebook source
# MAGIC %md
# MAGIC ## Generate Eval dataset for FAQ and documentation

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

spark.sql(f'use catalog {catalog}')
spark.sql(f'use schema {dbName}')

# COMMAND ----------

# MAGIC %md
# MAGIC Generate a synthetic evaluation dataset for FAQ and questions about documentation and manuals.

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit, from_json, explode, struct
from pyspark.sql.types import StringType, ArrayType

# Step 1: Load 10 guides
guides_df = spark.table("knowledge_base").limit(10)

# Step 2: Create prompt for question generation
guides_with_prompts = guides_df.withColumn(
    "question_prompt",
    concat_ws(
        " ",
        lit("You are building a question-answering dataset to evaluate an AI assistant trained on product documentation."),
        lit("Based on the following technical guide, generate 3 realistic user questions."),
        lit("Focus on error codes, troubleshooting, and step-by-step usage."),
        lit("Return only a JSON array of questions."),
        lit("Guide title:"), col("title"),
        lit("Product:"), col("product_name"),
        lit("Guide:"), col("full_guide")
    )
)


# Step 3: Call AI_QUERY directly (in Python, no view)
questions_raw = guides_with_prompts.select(
    "id", "product_name", "title",
    expr('AI_QUERY("databricks-claude-3-7-sonnet", question_prompt)').alias("questions_json")
)

# Step 4: Parse JSON → array<string>
questions_parsed = questions_raw.withColumn("questions", from_json("questions_json", ArrayType(StringType())))

# Step 5: Explode questions
questions = questions_parsed.select("id", "product_name", "title", explode("questions").alias("question"))
# Step 6: Join back with full_guide
questions_with_guides = questions.join(guides_df.select("id", "full_guide"), on="id", how="inner")

# Step 7: Build prompt to get expected facts
questions_with_facts_prompt = questions_with_guides.withColumn(
    "fact_prompt",
    concat_ws(
        " ",
        lit("You are evaluating an AI system. Based on the following product guide:"),
        col("full_guide"),
        lit("Return a JSON array of distinct factual statements (expected_facts) that would appear in a correct, helpful answer to the question."),
        lit("Each fact must be concise, complete, and non-redundant."),
        lit("Avoid repeating the same point in different words."),
        lit("Use full, natural language sentences."),
        lit("Return only the JSON array."),
        lit("Question:"), col("question")
    )
)

# Step 8: Call AI_QUERY again for expected_facts
facts_raw = questions_with_facts_prompt.select(
    "question",
    expr('AI_QUERY("databricks-claude-3-7-sonnet", fact_prompt)').alias("expected_facts_json")
)


# Step 9: Parse JSON array → array<string>
facts_df = facts_raw.withColumn("expected_facts", from_json("expected_facts_json", ArrayType(StringType())))

# Step 10: Build final eval format
eval_guides_df = facts_df.withColumn("inputs", struct("question")) \
                         .withColumn("predictions", lit("")) \
                         .withColumn("expectations", struct("expected_facts")) \
                         .select("inputs", "predictions", "expectations")

eval_guides_df.write.format('json').mode("append").save(f"/Volumes/{catalog}/{dbName}/{volume_name}/eval_dataset")
# Preview the result
display(spark.read.json(f"/Volumes/{catalog}/{dbName}/{volume_name}/eval_dataset"))
