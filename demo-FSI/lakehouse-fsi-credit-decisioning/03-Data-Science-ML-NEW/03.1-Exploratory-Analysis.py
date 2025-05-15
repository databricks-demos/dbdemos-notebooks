# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Data exploration is a critical first step in building a Responsible AI solution, as it helps ensure transparency, fairness, and reliability from the outset. In this notebook, we will explore and analyze our dataset on the Databricks Data Intelligence Platform. This process lays the foundation for responsible feature engineering and model development. Human validation remains an essential part of this step, ensuring that data-driven insights align with domain knowledge and ethical considerations.
# MAGIC
# MAGIC By leveraging Databricks’ unified data and AI capabilities, we can conduct secure and scalable exploratory data analysis (EDA), assess data distributions, and validate class representation before moving forward with model development.
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/_resources/images/architecture_1.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security and Table Access Controls
# MAGIC
# MAGIC Before proceeding with data exploration, it is crucial to implement security and access controls to ensure data integrity and compliance. Proper security measures help:
# MAGIC
# MAGIC - Protect sensitive financial and customer data, preventing unauthorized access and ensuring regulatory adherence.
# MAGIC - Ensure data consistency, so analysts work with validated and high-quality data without discrepancies.
# MAGIC - Avoid data leakage risks, preventing the unintentional exposure of confidential information that could lead to compliance violations.
# MAGIC - Facilitate accountability, ensuring that any modifications or transformations are logged and traceable.
# MAGIC
# MAGIC By establishing a secure data foundation, we ensure that subsequent analysis and modeling steps are performed responsibly, with complete confidence in data integrity and compliance.
# MAGIC
# MAGIC Table Access Control (TAC) in Databricks lets administrators manage access to specific tables and columns, controlling permissions like read, write, or modify. Integrated with Unity Catalog, it allows fine-grained security to protect sensitive data and ensure only authorized users have access. This feature enhances data governance, compliance, and secure collaboration across the platform.
# MAGIC
# MAGIC Now, let's grant only a ```SELECT``` access to ```customer_gold``` table to everyone in the group ```Data Scientist```.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT ON TABLE customer_gold TO `Data Scientist`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Exploratory Data Analysis
# MAGIC
# MAGIC The first step as Data Scientist is to explore and understand the data. [Databricks Notebooks](https://docs.databricks.com/en/notebooks/index.html) offer native data quality profiling and dashboarding capabilities that allow users to easily assess and visualize the quality of their data. Built-in tools allows us to:
# MAGIC - Identify missing values and potential data quality issues.
# MAGIC - Detect outliers and anomalies that may skew model predictions.
# MAGIC - Assess statistical distributions of key features to uncover potential biases.
# MAGIC
# MAGIC Databricks enables scalable EDA through interactive notebooks, where we can visualize distributions, perform statistical tests, and generate summary reports seamlessly.

# COMMAND ----------

# DBTITLE 1,Use SQL to explore your data
# MAGIC %sql
# MAGIC SELECT * FROM customer_gold

# COMMAND ----------

# MAGIC %md
# MAGIC While Databricks provides built-in data profiling tools, additional Python libraries such as Plotly and Seaborn can be used to enhance analysis. These libraries allow for more interactive and customizable visualizations, helping uncover hidden patterns in the data. 
# MAGIC
# MAGIC Using these additional libraries in combination with Databricks' built-in capabilities ensures a more comprehensive data exploration process, leading to better insights for responsible model development.

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

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC After completing exploratory analysis, we proceed to [06.2-Feature-Updates]($./06-Responsible-AI/06.2-Feature-Updates), where we:
# MAGIC - Continuously ingest new data to keep features relevant and up to date.
# MAGIC - Apply responsible transformations while maintaining full lineage and compliance.
# MAGIC - Log feature changes to ensure transparency in model evolution.
# MAGIC
# MAGIC By systematically updating features, we reinforce responsible AI practices and enhance our credit scoring model’s fairness, reliability, and effectiveness.
