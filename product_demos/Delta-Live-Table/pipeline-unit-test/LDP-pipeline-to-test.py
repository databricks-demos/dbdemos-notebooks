# Databricks notebook source
# MAGIC %md 
# MAGIC # Lakeflow Declarative Pipelines - Unit testing
# MAGIC
# MAGIC ## Why testing?
# MAGIC
# MAGIC Deploying tests on your LDP pipelines will guarantee that your ingestion is always stable and future proof.
# MAGIC
# MAGIC The tests can be deployed as part of traditional CI/CD pipeline and can be run before a new version deployment, ensuring that a new version won't introduce a regression.
# MAGIC
# MAGIC This is critical in the Lakehouse ecosystem, as the data we produce will then leveraged downstream:
# MAGIC
# MAGIC * By Data Analyst for reporting/BI
# MAGIC * By Data Scientists to build ML model for downstream applications
# MAGIC
# MAGIC ## Unit testing strategy with LDP
# MAGIC
# MAGIC Lakeflow Declarative Pipeline logic can be unit tested leveraging Expectation.
# MAGIC
# MAGIC At a high level, the LDP pipelines can be constructed as following:
# MAGIC
# MAGIC * The ingestion step (first step of the pipeline on the left) is written in a separate notebook. This correspond to the left **green** (prod) and **blue** (test) input sources.
# MAGIC    * The Production pipeline is defined with the PROD ingestion notebook:[./ingestion_profile/LDP-ingest_prod]($./ingestion_profile/LDP-ingest_prod) and connects to the live datasource (ex: kafka server, staging blob storage)
# MAGIC    * The Test pipeline (only used to run the unit test) is defined with the TEST ingestion notebook: [./ingestion_profile/LDP-ingest_test]($./ingestion_profile/LDP-ingest_test) and can consume from local files used for our unit tests (ex: adhoc csv file)
# MAGIC * A common LDP pipeline logic is used for both the prod and the test pipeline (the **yellow** in the graph)
# MAGIC * An additional notebook containing all the unit tests is used in the TEST pipeline (the **blue `TEST_xxx` tables** in the image on the right side)
# MAGIC
# MAGIC
# MAGIC <div><img width="1100" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-advanecd/DLT-advanced-unit-test-0.png"/></div>
# MAGIC
# MAGIC ## Accessing the pipeline
# MAGIC
# MAGIC Your pipeline has been created! You can directly access the <a dbdemos-pipeline-id="dlt-test" href="#joblist/pipelines/cade4f82-4003-457c-9f7c-a8e5559873b6">Lakeflow Declarative Pipeline for unit-test demo</a>.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=LDP-pipeline-to-test&demo_name=dlt-unit-test&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Main Pipeline definition
# MAGIC
# MAGIC <img style="float: right" width="700px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-advanecd/DLT-advanced-unit-test-2.png"/>
# MAGIC
# MAGIC This notebook contains the main pipeline definition, the one we want to test (in yellow in the diagram).
# MAGIC
# MAGIC For this example, we centralized our main expectations in a metadata table that we'll use in the table definition.
# MAGIC
# MAGIC Theses expectations are your usual expectations, used to ensure and track data quality during the ingestion process. 
# MAGIC
# MAGIC We can then build DBSQL dashboard on top of it and triggers alarms when we see error in our data (ex: incompatible schema, increasing our expectation count)

# COMMAND ----------

# DBTITLE 1,Define all our expectations as a metadata table
# In this example, we'll store our rules as a delta table for more flexibility & reusability. 
# While this isn't directly related to Unit test, it can also help for programatical analysis/reporting.
catalog = "main__build"
schema = dbName = db = "dbdemos_ldp_unit_test"

data = [
 # tag/table name      name              constraint
 ("user_bronze_ldp",  "correct_schema", "_rescued_data IS NULL"),
 ("user_silver_ldp",  "valid_id",       "id IS NOT NULL AND id > 0"),
 ("spend_silver_ldp", "valid_id",       "id IS NOT NULL AND id > 0"),
 ("user_gold_ldp",    "valid_age",      "age IS NOT NULL"),
 ("user_gold_ldp",    "valid_income",   "annual_income IS NOT NULL"),
 ("user_gold_ldp",    "valid_score",    "spending_core IS NOT NULL")
]
#Typically only run once, this doesn't have to be part of the LDP pipeline.
spark.createDataFrame(data=data, schema=["tag", "name", "constraint"]).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.expectations")

# COMMAND ----------

# DBTITLE 1,Make expectations portable and reusable from a Delta Table
#Return the rules matching the tag as a format ready for LDP annotation.
from pyspark.sql.functions import expr, col

def get_rules(tag):
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.table(f"{catalog}.{schema}.expectations").where(f"tag = '{tag}'")
  for row in df.collect():
    rules[row['name']] = row['constraint']
  return rules


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1/ Loading our data
# MAGIC
# MAGIC This is the first step of the pipeline. Note that we consume the data from the `raw_user_data` view.
# MAGIC
# MAGIC This view is defined in the ingestion notebooks:
# MAGIC * For PROD: [./ingestion_profile/LDP-ingest_prod]($./ingestion_profile/LDP-ingest_prod), reading from prod system (ex: kafka)
# MAGIC * For TEST: [./ingestion_profile/LDP-ingest_test]($./ingestion_profile/LDP-ingest_test), consuming the test dataset (csv files)
# MAGIC
# MAGIC Start by reviewing the notebooks to see how the data is ingested.
# MAGIC
# MAGIC
# MAGIC *Note: LDP is available as SQL or Python, this example will use Python*

# COMMAND ----------

# DBTITLE 1,Ingest raw User stream data in incremental mode
import dlt

@dlt.table(comment="Raw user data")
@dlt.expect_all_or_drop(get_rules('user_bronze_ldp')) #get the rules from our centralized table.
def user_bronze_ldp():
  return dlt.read_stream("raw_user_data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2/ Customer Silver layer
# MAGIC The silver layer is consuming **incremental** data from the bronze one, and cleaning up some information.
# MAGIC
# MAGIC We're also adding an expectation on the ID. As the ID will be used in the next join operation, ID should never be null and be positive.
# MAGIC
# MAGIC Note that the expectations have been defined in the metadata expectation table under `user_silver_ldp`

# COMMAND ----------

# DBTITLE 1,Clean and anonymize User data
from pyspark.sql.functions import *

@dlt.table(comment="User data cleaned and anonymized for analysis.")
@dlt.expect_all_or_drop(get_rules('user_silver_ldp'))
def user_silver_ldp():
  return (
    dlt.read_stream("user_bronze_ldp").select(
      col("id").cast("int"),
      sha1("email").alias("email"),
      to_timestamp(col("creation_date"),"MM-dd-yyyy HH:mm:ss").alias("creation_date"),
      to_timestamp(col("last_activity_date"),"MM-dd-yyyy HH:mm:ss").alias("last_activity_date"),
      "firstname", 
      "lastname", 
      "address", 
      "city", 
      "last_ip", 
      "postcode"
    )
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3/ Ingest spend information
# MAGIC
# MAGIC This is the same logic as for the customer data, we consume from the view defined in the TEST/PROD ingestion notebooks.
# MAGIC
# MAGIC We're also adding an expectation on the ID column as we'll join the 2 tables based on this field, and we want to track it's data quality

# COMMAND ----------

# DBTITLE 1,Ingest user spending score
@dlt.table(comment="Spending score from raw data")
@dlt.expect_all_or_drop(get_rules('spend_silver_ldp'))
def spend_silver_ldp():
    return dlt.read_stream("raw_spend_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4/ Joining the 2 tables to create the gold layer
# MAGIC We can now join the 2 tables on customer ID to create our final gold table.
# MAGIC
# MAGIC As our ML model will be using `age`, `annual_income` and `spending_score` we're adding expectation to only keep valid entries 

# COMMAND ----------

# DBTITLE 1,Join both data to create our final table
@dlt.table(comment="Final user table with all information for Analysis / ML")
@dlt.expect_all_or_drop(get_rules('user_gold_ldp'))
def user_gold_ldp():
  return dlt.read_stream("user_silver_ldp").join(dlt.read("spend_silver_ldp"), ["id"], "left")

# COMMAND ----------

# MAGIC %md # Our pipeline is now ready to be tested!
# MAGIC
# MAGIC Our pipeline now entirely defined.
# MAGIC
# MAGIC Here are a couple of example we might want to test:
# MAGIC
# MAGIC * Are we safely handling wrong data type as entry (ex: customer ID is sent as an incompatible STRING)
# MAGIC * Are we resilient to NULL values in our primary keys
# MAGIC * Are we enforcing uniqueness in our primary keys
# MAGIC * Are we properly applying business logic (ex: proper aggregation, anonymization of PII field etc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the test dataset
# MAGIC
# MAGIC The next step is to create a test dataset.
# MAGIC
# MAGIC Creating the test dataset is a critical step. As any Unit tests, we need to add all possible data variation to ensure our logic is properly implemented.
# MAGIC
# MAGIC As example, let's make sure we'll ingest data having NULL id or ids as string.
# MAGIC
# MAGIC Open the [./test/LDP-Test-Dataset-setup]($./test/LDP-Test-Dataset-setup) notebook to see how this is done

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining the Unit Tests
# MAGIC
# MAGIC We now have the data ready.
# MAGIC
# MAGIC The final step is creating the actual test.
# MAGIC
# MAGIC Open the [./test/LDP-Tests]($./test/LDP-Tests) notebook to see how this is done!

# COMMAND ----------

# MAGIC %md
# MAGIC # That's it! our pipeline is fully ready & tested.
# MAGIC
# MAGIC We can then process as usual: build dashboard to track production metrics (ex: data quality & quantity) but also BI reporting & Data Science for final business use-case leveraging the Lakehouse:
# MAGIC
# MAGIC Here is a full example of the test pipeline definition.
# MAGIC
# MAGIC Note that we have 3 notebooks in the LDP pipeline:
# MAGIC
# MAGIC * **LDP-ingest_test**: ingesting our test datasets
# MAGIC * **LDP-pipeline-to-test**: the actual pipeline we want to test
# MAGIC * **test/LDP-Tests**: the test definition
# MAGIC
# MAGIC Remember that you'll have to schedule FULL REFRESH everytime your run the pipeline to get accurate test results (we want to consume all the entry dataset from scratch).
# MAGIC
# MAGIC This test pipeline can be scheduled to run within a Workflow, or as part of a CICD step (ex: triggered after a git commit)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Going further with LDP

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking your data quality metrics with Lakeflow Declarative Pipelines
# MAGIC Lakeflow Declarative Pipelines tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building our first business dashboard with Databricks SQL
# MAGIC
# MAGIC Once the data is ingested, we switch to Databricks SQL to build a new dashboard based on all the data we ingested.
# MAGIC
# MAGIC Here is an example:
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
