# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Patient cohort analysis and Dashboarding
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-flow-3.png" style="float: right; margin-left: 30px; margin-top:10px" width="650px" />
# MAGIC
# MAGIC Now that our table are clean and secure, our Clinical Data Scientist and researcher can start exploring this data.
# MAGIC
# MAGIC Databricks provides interactive notebooks where Analysts can leverage python, SQL and R.
# MAGIC
# MAGIC To be able to analyze and reduce our patient readmission risk, we'll start by analyzing the underlying data and build patient cohorts.
# MAGIC
# MAGIC Once completed, we will create Dashboard to visualize and share our results. Note that visualization can also be done with external tools such as PowerBI or Tableau.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03-Data-Analysis-BI-Warehousing-patient-readmission&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## The need for Enhanced Collaboration
# MAGIC
# MAGIC Data Analysis is an iterative process - we need to quickly query new data, test hypothesis and deliver outcome. The Databricks Lakehouse enables data teams to collaborate extremely effectively through the following Databricks Notebook features:
# MAGIC 1. Sharing and collaborating in the same Notebook by any team member (with different access modes),
# MAGIC 2. Ability to use python, SQL, and R simultaneously in the same Notebook on the same data,
# MAGIC 3. Native integration with a Git repository (including AWS Code Commit, Azure DevOps, GitLabs, Github, and others), making the Notebooks tools for CI/CD,
# MAGIC 4. Variables explorer,
# MAGIC 5. Automatic Data Profiling (in the cell below), and
# MAGIC 6. GUI-based dashboards (in the cell below) that can also be added to any Databricks SQL Dashboard.
# MAGIC
# MAGIC These features enable teams within HLS organizations to become extremely fast and efficient in building the best Analysis.

# COMMAND ----------

# DBTITLE 1,Use SQL to explore your data
# MAGIC %sql select * from patients

# COMMAND ----------

# MAGIC %sql
# MAGIC select GENDER, ceil(months_between(current_date(),BIRTHDATE)/12/5)*5 as age, count(*) as count from patients group by GENDER, age order by age
# MAGIC -- Can use buildin visualization (Area: Key: age, group: gender_source_value, Values: count)

# COMMAND ----------

# DBTITLE 1,Plot data with any DS libraries (python or R)
import plotly.express as px
px.area(_sqldf.toPandas(), x="age", y="count", color="GENDER", line_group="GENDER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Patient Condition
# MAGIC
# MAGIC The condition information are stored under the `conditions` table

# COMMAND ----------

#We can also leverage pure SQL to access data
df = spark.table("patients").join(spark.table("conditions"), col("Id")==col("PATIENT")) \
          .groupBy(['GENDER', 'conditions.DESCRIPTION']).count() \
          .orderBy(F.desc('count')).limit(20).toPandas()
#And use our usual plot libraries
px.bar(df, x="DESCRIPTION", y="count", color="GENDER", barmode="group")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1/ Cohort definition
# MAGIC
# MAGIC To ensure better reproducibility and organizing the data, we first create patient cohorts based on the criteria of interest (being admitted to hopital, infection status, disease hirtory etc). We can the store the result in a downstream table that we'll use later on to train a model on patient readmission.

# COMMAND ----------

# DBTITLE 1,Creating our cohort tables
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cohort (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   patient STRING,
# MAGIC   cohort_start_date DATE,
# MAGIC   cohort_end_date DATE
# MAGIC );
# MAGIC ALTER TABLE cohort OWNER TO `account users`;

# COMMAND ----------

# DBTITLE 1,Create patient cohorts based on specific conditions
import random
def create_save_cohort(name, condition_codes = []):
  cohort1 = (spark.sql('select patient, to_date(start) as cohort_start_date, to_date(stop) as cohort_end_date from conditions')
                 .withColumn('id', F.lit(random.randint(999999, 99999999)))
                 .withColumn('name', F.lit(name)))
  if len(condition_codes)> 0:
    cohort1 = cohort1.where(col('CODE').isin(condition_codes))
  cohort1.write.mode("append").saveAsTable('cohort')

#Create cohorts based on patient condition (for ex: 840539006 is COVID)
create_save_cohort('COVID-19-cohort', [840539006])
create_save_cohort('heart-condition-cohort', [1505002, 32485007, 305351004, 76464004])
create_save_cohort('all_patients')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Cohort visualization - Your Lakehouse is the best Warehouse
# MAGIC
# MAGIC Working with the lakehouse unlock traditional BI analysis but also real time applications having a direct connection to your entire data, while remaining fully secured.
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/dbsql.png" width="700px" style="float: left" />
# MAGIC
# MAGIC <div style="float: left; margin-top: 240px; font-size: 23px">
# MAGIC   Instant, elastic compute<br>
# MAGIC   Lower TCO with Serveless<br>
# MAGIC   Zero management<br><br>
# MAGIC
# MAGIC   Governance layer - row level<br><br>
# MAGIC
# MAGIC   Your data. Your schema (star, data vault…)
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffsi%2Flakehouse_credit_scoring%2Fbi&dt=LAKEHOUSE_CREDIT_SCORING">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Databricks SQL Warehouses: best-in-class BI engine
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
# MAGIC
# MAGIC Databricks SQL is a warehouse engine packed with thousands of optimizations to provide you with the best performance for all your tools, query types and real-world applications. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>It won the Data Warehousing Performance Record.</a>
# MAGIC
# MAGIC This includes the next-generation vectorized query engine Photon, which together with SQL warehouses, provides up to 12x better price/performance than other cloud data warehouses.
# MAGIC
# MAGIC **Serverless warehouse** provide instant, elastic SQL compute — decoupled from storage — and will automatically scale to provide unlimited concurrency without disruption, for high concurrency use cases.
# MAGIC
# MAGIC Make no compromise. Your best Datawarehouse is a Lakehouse.
# MAGIC
# MAGIC ### Creating a SQL Warehouse
# MAGIC
# MAGIC SQL Wharehouse are managed by databricks. [Creating a warehouse](/sql/warehouses) is a 1-click step: 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Creating your first Patient Dashboard
# MAGIC
# MAGIC <img style="float: left; margin-right: 10px" width="600px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-patient-dashboard.png" />
# MAGIC
# MAGIC Our Clinical Data Scientists can now start running SQL queries using the SQL editor and add new visualizations.
# MAGIC
# MAGIC By leveraging auto-completion and the schema browser, we can start running adhoc queries on top of our data.
# MAGIC
# MAGIC Once merged While this is ideal for Data Analyst to start analysing our customer Churn, other personas can also leverage DBSQL to track our data ingestion pipeline, the data quality, model behavior etc.
# MAGIC
# MAGIC Open the [Queries menu](/sql/queries) to start writting your first analysis.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Using Third party BI tools
# MAGIC
# MAGIC <iframe style="float: right; margin-left: 10px" width="560" height="315" src="https://www.youtube.com/embed/EcKqQV0rCnQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
# MAGIC
# MAGIC SQL warehouse can also be used with an external BI tool such as Tableau or PowerBI.
# MAGIC
# MAGIC This will allow you to run direct queries on top of your table, with a unified security model and Unity Catalog (ex: through SSO). Now analysts can use their favorite tools to discover new business insights on the most complete and freshest data.
# MAGIC
# MAGIC To start using your Warehouse with third party BI tool, click on "Partner Connect" on the bottom left and chose your provider.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open the <a dbdemos-dashboard-id="patient-summary" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Cohort patient analysis Dashboard</a> to start reviewing our cohort data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Predict readmission risk to improve our patient care
# MAGIC
# MAGIC We're now ready for more advanced data analysis. As next step, we will build a model to predict patient readmission and use this information to adapt our patient journey, targeting the population at risk.
# MAGIC
# MAGIC Open the [04.1-Feature-Engineering-patient-readmission]($../04-Data-Science-ML/04.1-Feature-Engineering-patient-readmission) notebook to start building your first model on the Lakehouse.
