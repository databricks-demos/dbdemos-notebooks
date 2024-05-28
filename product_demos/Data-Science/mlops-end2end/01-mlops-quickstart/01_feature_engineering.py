# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-1.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_feature_prep&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Feature engineering",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["feature store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,Install latest feature engineering client for UC [for MLR < 13.2] and databricks python sdk
# MAGIC %pip install databricks-sdk==0.23.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data='false'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Anaylsis
# MAGIC To get a feel of the data, what needs cleaning, pre-processing etc.
# MAGIC - **Use Databricks's native visualization tools**
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
# MAGIC ## Define Featurization Logic(s) for BATCH feature computation
# MAGIC
# MAGIC 1. Compute number of active services
# MAGIC 2. Clean-up names and manual mapping
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
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({1 : "Yes", 0 : "No"})
  data_psdf = data_psdf.astype({"total_charges": "double", "senior_citizen": "string"})

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

  # Move the label column "churn" to the end of the column list
  col_names = data_psdf.columns.to_list()
  col_names.remove("churn")
  col_names.append("churn")

  # Add/Force semantic data types for specific colums (to facilitate autoML)
  data_cleanDF = data_psdf.to_spark()
  data_cleanDF = data_cleanDF.withMetadata(primary_key, {"spark.contentAnnotation.semanticType":"native"})
  data_cleanDF = data_cleanDF.withMetadata("num_optional_services", {"spark.contentAnnotation.semanticType":"numeric"})

  # Return the cleaned Spark dataframe, with columns in the right order
  return data_cleanDF.select(col_names)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Compute features & write table with features and labels
# MAGIC
# MAGIC Once our features are ready, we'll save them along with the labels as a Delta Lake table. This can then be retrieved later for model training.
# MAGIC
# MAGIC In this Quickstart demo, we will save the features and labels as a Delta Lake table. We will then look at how we train a model using this labeled dataset and capture the table-model lineage. Model lineage brings traceability and governance in our deployment, letting us know which model is dependent of which set of feature tables.
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

# Write table for training
(churn_features.write.mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(f"{catalog}.{db}.mlops_churn_training"))

# Add comment to the table
spark.sql(
    f"""
  COMMENT ON TABLE {catalog}.{db}.mlops_churn_training IS \'The features in this table are derived from the {catalog}.{db}.mlops_churn_bronze_customers table in the lakehouse. We created service features, cleaned up their names.  No aggregations were performed.'
  """
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! The labeled features are now ready to be used for training.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Churn model creation using Databricks Auto-ML
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient.
# MAGIC
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the table with the ground truth labels we just created (`dbdemos.schema.mlops_churn_labels`) and join it with the feature table we just created (`dbdemos.schema.mlops_churn_features`).
# MAGIC
# MAGIC Our prediction target is the `churn` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/en/machine-learning/automl/train-ml-model-automl-api.html)
# MAGIC
# MAGIC #### Join/Use features directly from the Feature Store from the [UI](https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-ui.html#use-existing-feature-tables-from-databricks-feature-store) or [python API](https://docs.databricks.com/en/machine-learning/automl/train-ml-model-automl-api.html)
# MAGIC
# MAGIC
# MAGIC * Select the table containing the ground-truth labels (i.e. `dbdemos.schema.mlops_churn_labels`)
# MAGIC * Join remaining features from the feature table (i.e. `dbdemos.schema.mlops_churn_features`)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Auto ML with labeled datasets
# MAGIC
# MAGIC [Auto ML](https://docs.databricks.com/en/machine-learning/automl/how-automl-works.html) also works on an input table with prepared features and the corresponding labels. For this quicktstart demo, this is what we will be doing. We will create a labelled dataset and save it as a Delta Lake table `dbdemos.schema.mlops_churn_training`. We run AutoML on this table and capture the table lineage at training time.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Run 'baseline' autoML experiment in the back-ground
from databricks import automl
from datetime import datetime

xp_path = "/Shared/dbdemos/experiments/mlops"
xp_name = f"automl_churn_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"

automl_run = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = churn_features,
    target_col = "churn",
    timeout_minutes = 5
)
#Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the generated notebook to build our model
# MAGIC
# MAGIC Next step: [Explore the generated Auto-ML notebook]($./02_automl_champion)
# MAGIC
# MAGIC **Note:**
# MAGIC For demo purposes, run the above notebook OR create and register a new version of the model from your autoML experiment and label/alias the model as "Champion"
