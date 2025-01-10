# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01_feature_engineering&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Install latest feature engineering client for UC [for MLR < 13.2] and databricks python sdk
# MAGIC %pip install --quiet mlflow==2.19
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Anaylsis
# MAGIC To get a feel of the data, what needs cleaning, pre-processing etc.
# MAGIC - **Use Databricks's native visualization tools**
# MAGIC   - After running a SQL query in a notebook cell, use the `+` tab to add charts to visualize the results.
# MAGIC - Bring your own visualization library of choice (i.e. seaborn, plotly)

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
# MAGIC 1. Compute number of optional services
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
# MAGIC *Note: Pandas API on Spark used to be called Koalas. Starting from `spark 3.2`, Koalas is builtin and we can get an Pandas Dataframe using `pandas_api()` [Details](https://spark.apache.org/docs/latest/api/python/migration_guide/koalas_to_pyspark.html).*

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
# MAGIC In this Quickstart demo, we will look at how we train a model using this labeled dataset saved as a Delta Lake table and capture the table-model lineage. Model lineage brings traceability and governance in our deployment, letting us know which model is dependent of which set of feature tables.
# MAGIC
# MAGIC Databricks has a Feature Store capability that is tightly integrated into the platform. Any Delta Lake table with a primary key can be used as a Feature Store table for model training, as well as batch and online serving. We will look at an example of how to use the Feature Store to perform feature lookups in a more advanced demo.
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

# Specify train-val-test split
train_ratio, val_ratio, test_ratio = 0.7, 0.2, 0.1
churn_features = (churn_features.withColumn("random", F.rand(seed=42))
                                .withColumn("split",
                                            F.when(F.col("random") < train_ratio, "train")
                                            .when(F.col("random") < train_ratio + val_ratio, "validate")
                                            .otherwise("test"))
                                .drop("random"))

# Write table for training
(churn_features.write.mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable("mlops_churn_training"))

# Add comment to the table
spark.sql(f"""COMMENT ON TABLE {catalog}.{db}.mlops_churn_training IS \'The features in this table are derived from the mlops_churn_bronze_customers table in the lakehouse. 
              We created service features, cleaned up their names.  No aggregations were performed.'""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! The labeled features are now ready to be used for training.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Churn model creation using Databricks AutoML
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient.
# MAGIC
# MAGIC Instead of creating the same boilerplate for each new project, Databricks AutoML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC
# MAGIC ### Using Databricks AutoML with our Churn dataset
# MAGIC
# MAGIC AutoML is available in the "Machine Learning" space. All we have to do is start a new AutoML experiment and select the table we just created (`dbdemos.schema.mlops_churn_training`).
# MAGIC
# MAGIC Our prediction target is the `churn` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/en/machine-learning/automl/train-ml-model-automl-api.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using AutoML with labelled feature tables
# MAGIC
# MAGIC [AutoML](https://docs.databricks.com/en/machine-learning/automl/how-automl-works.html) works on an input table with prepared features and the corresponding labels. For this quicktstart demo, this is what we will be doing. We run AutoML on the table `dbdemos.schema.mlops_churn_training` and capture the table lineage at training time.
# MAGIC
# MAGIC #### Using AutoML with tables in the Feature Store
# MAGIC
# MAGIC AutoML also works with tables containing only the ground-truth labels, and joining it with feature tables in the Feature Store. This will be illustrated in a more advanced demo.
# MAGIC
# MAGIC You can join/use features directly from the Feature Store from the [UI](https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-ui.html#use-existing-feature-tables-from-databricks-feature-store) or [python API](https://docs.databricks.com/en/machine-learning/automl/train-ml-model-automl-api.html#automl-experiment-with-feature-store-example-notebook)
# MAGIC * Select the table containing the ground-truth labels
# MAGIC * Join remaining features from the feature table

# COMMAND ----------

# DBTITLE 1,Run 'baseline' autoML experiment in the back-ground
from datetime import datetime

xp_path = "/Shared/dbdemos/experiments/mlops"
xp_name = f"automl_churn_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"

churn_features = churn_features.withMetadata("num_optional_services", {"spark.contentAnnotation.semanticType":"numeric"})
try: 
    from databricks import automl 

    # Add/Force semantic data types for specific colums (to facilitate autoML and make sure it doesn't interpret it as categorical)

    automl_run = automl.classify(
        experiment_name = xp_name,
        experiment_dir = xp_path,
        dataset = churn_features,
        target_col = "churn",
        split_col = "split", #This required DBRML 15.3+
        timeout_minutes = 10,
        exclude_cols ='customer_id'
    )
    #Make sure all users can access dbdemos shared experiment
    DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

except Exception as e: 
    if "cannot import name 'automl'" in str(e):
        # Note: cannot import name 'automl' likely means you're using serverless. Dbdemos doesn't support autoML serverless API - this will be improved soon.
        # adding a temporary workaround to make sure this works well for now -- ignore this for classic run
        DBDemos.create_mockup_automl_run_for_dbdemos(f"{xp_path}/{xp_name}", churn_features.toPandas()) 
    else: 
        raise e
    
# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the generated notebook to build our model
# MAGIC
# MAGIC Next step: [Explore the generated Auto-ML notebook]($./02_automl_best_run)
# MAGIC
# MAGIC **Note:**
# MAGIC For demo purposes, run the above notebook to create and register a new version of the model from your autoML experiment and label/alias the model as "Champion"
