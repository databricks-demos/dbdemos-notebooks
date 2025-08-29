# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or disable the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01_feature_engineering&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC mlflow==3.3.0
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --quiet mlflow --upgrade
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Analysis
# MAGIC To get a feel for the data, what needs cleaning, pre-processing, etc.
# MAGIC - **Use Databricks's native visualization tools**
# MAGIC   - After running a SQL query in a notebook cell, use the `+` tab to add charts to visualize the results.
# MAGIC - Bring your own visualization library of choice (i.e., seaborn, plotly)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mlops_churn_bronze_customers

# COMMAND ----------

telco_df = spark.read.table("mlops_churn_bronze_customers").pandas_api()
telco_df["internet_service"].value_counts().plot.pie()

# COMMAND ----------

# DBTITLE 1,Read in Bronze Delta table using Spark
# Read into Spark
telcoDF = spark.read.table("mlops_churn_bronze_customers")
display(telcoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define data cleaning and featurization Logic
# MAGIC
# MAGIC We will define a function to clean the data and implement featurization logic. We will:
# MAGIC
# MAGIC 1. Compute the number of optional services
# MAGIC 2. Provide meaningful labels
# MAGIC 3. Impute null values
# MAGIC
# MAGIC _This can also work for streaming based features_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Pandas On Spark API
# MAGIC
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use the [pandas on spark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC
# MAGIC *Note: Pandas API on Spark used to be called Koalas. Starting from `spark 3.2`, Koalas is builtin, and we can get a Pandas Dataframe using `pandas_api()` [Details](https://spark.apache.org/docs/latest/api/python/migration_guide/koalas_to_pyspark.html).*

# COMMAND ----------

# DBTITLE 1,Define featurization function
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_churn_features(dataDF: DataFrame) -> DataFrame:
  """
  Simple cleaning function leveraging pandas API
  """

  # Convert to pandas on spark dataframe
  data_psdf = dataDF.pandas_api()
  # Convert some columns
  data_psdf = data_psdf.astype({"senior_citizen": "string"})
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({"1" : "Yes", "0" : "No"})

  data_psdf["total_charges"] = data_psdf["total_charges"].apply(lambda x: float(x) if x.strip() else 0)


  # Fill some missing numerical values with 0
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  def sum_optional_services(df):
      """Count number of optional services enabled, like streaming TV"""
      cols = ["online_security", "online_backup", "device_protection", "tech_support",
              "streaming_tv", "streaming_movies"]
      return sum(map(lambda c: (df[c] == "Yes"), cols))

  data_psdf["num_optional_services"] = sum_optional_services(data_psdf)

  # Return the cleaned Spark dataframe
  return data_psdf.to_spark()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Compute features & write table with features and labels
# MAGIC
# MAGIC Once our features are ready, we'll save them along with the labels as a Delta Lake table. This can then be retrieved later for model training.
# MAGIC
# MAGIC In this Quickstart demo, we will look at how we train a model using this labeled dataset saved as a Delta Lake table and capture the table-model lineage. Model lineage brings traceability and governance to our deployment, letting us know which model depends on which set of feature tables.
# MAGIC
# MAGIC Databricks has a Feature Store capability tightly integrated into the platform. Any Delta Lake table with a primary key can be used as a Feature Store table for model training and batch and online serving. We will look at an example of using the Feature Store to perform feature lookups in a more advanced demo.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Compute Churn Features and append a timestamp
churn_features = clean_churn_features(telcoDF)
display(churn_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write table for training
# MAGIC
# MAGIC Write the labeled data that has the prepared features and labels as a Delta Table. We will later use this table to train the model to predict churn.

# COMMAND ----------

# Specify train-test split
train_ratio, test_ratio = 0.8, 0.2
churn_features = (churn_features.withColumn("random", F.rand(seed=42))
                                .withColumn("split",
                                            F.when(F.col("random") < train_ratio, "train")
                                            .otherwise("test"))
                                .drop("random"))

# Write table for training
(churn_features.write.mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable("mlops_churn_training"))

# Add comment to the table
spark.sql(f"""COMMENT ON TABLE {catalog}.{db}.mlops_churn_training IS \'The features in this table are derived from the mlops_churn_bronze_customers table in the lakehouse. 
              We created service features and cleaned up their names.  No aggregations were performed.'""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! The labeled features are now ready to be used for training.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train a base model
# MAGIC
# MAGIC Next step: [Train a lightGBM model]($./02_train_lightGBM)
