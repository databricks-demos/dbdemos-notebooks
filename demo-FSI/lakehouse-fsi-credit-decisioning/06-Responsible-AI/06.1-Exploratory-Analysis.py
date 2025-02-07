# Databricks notebook source
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security and Table Access Controls
# MAGIC
# MAGIC Table Access Control (TAC) in Databricks lets administrators manage access to specific tables and columns, controlling permissions like read, write, or modify. Integrated with Unity Catalog, it allows fine-grained security to protect sensitive data and ensure only authorized users have access. This feature enhances data governance, compliance, and secure collaboration across the platform.
# MAGIC
# MAGIC Now, let's grant only a ```SELECT``` access to ```customer_gold``` table to everyone in the group ```datascientists```.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT ON TABLE customer_gold TO `datascientists`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Exploratory Data Analysis
# MAGIC
# MAGIC The first step as Data Scientist is to explore and understand the data. [Databricks Notebooks](https://docs.databricks.com/en/notebooks/index.html) offer native data quality profiling and dashboarding capabilities that allow users to easily assess and visualize the quality of their data. Built-in tools enable automatic data profiling, checking for issues like missing values, outliers, or inconsistencies, and generating insights in an interactive format. Users can then create dashboards to monitor and track data quality metrics in real-time, helping teams identify and address issues quickly, ensuring reliable, high-quality data for analysis and decision-making.

# COMMAND ----------

# DBTITLE 1,Use SQL to explore your data
# MAGIC %sql
# MAGIC SELECT * FROM customer_gold

# COMMAND ----------

# DBTITLE 1,Use any of your usual python libraries for analysis
data = spark.table("customer_gold") \
              .where("tenure_months BETWEEN 10 AND 150") \
              .groupBy("tenure_months", "education").sum("income_monthly") \
              .orderBy('education').toPandas()

px.bar(data, x="tenure_months", y="sum(income_monthly)", color="education", title="Total Monthly Income")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Class representation
# MAGIC
# MAGIC Understanding class representation during exploratory data analysis is crucial for identifying potential bias in data. Imbalanced classes can lead to biased models, where underrepresented classes are poorly learned, resulting in inaccurate predictions. If certain groups are over- or underrepresented, the model may inherit societal biases, leading to unfair outcomes. Identifying skewed distributions helps in selecting appropriate resampling techniques or adjusting model evaluation metrics. Moreover, biased training data can reinforce discrimination in applications like credit decisioning. Detecting imbalance early allows for corrective actions, ensuring a more robust, fair, and generalizable model that performs well across all classes.

# COMMAND ----------

data = spark.table("customer_gold") \
              .groupBy("gender").count() \
              .orderBy('gender').toPandas()

px.pie(data_frame=data, names="gender", values="count", color="gender", title="Percentage of Males vs. Females")

# COMMAND ----------


