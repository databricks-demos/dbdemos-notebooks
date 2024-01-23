# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="float:right ;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks/koalas/master/Koalas-logo.png" width="180"/>
# MAGIC </div>
# MAGIC 
# MAGIC # Scale pandas API with Databricks runtime as backend
# MAGIC Using Databricks, Data Scientist don't have to learn a new API to analyse data and deploy new model in production
# MAGIC 
# MAGIC * If you model is small and fit in a single node, you can use a Single Node cluster with pandas directly
# MAGIC * If your data grow, no need to re-write the code. Just switch to pandas on spark and your cluster will parallelize your compute out of the box.
# MAGIC 
# MAGIC ## Scalability beyond a single machine
# MAGIC 
# MAGIC <div style="float:right ;">
# MAGIC   <img src="https://databricks.com/fr/wp-content/uploads/2021/09/Pandas-API-on-Upcoming-Apache-Spark-3.2-blog-img-4.png" width="300"/>
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC One of the known limitations in pandas is that it does not scale with your data volume linearly due to single-machine processing. For example, pandas fails with out-of-memory if it attempts to read a dataset that is larger than the memory available in a single machine.
# MAGIC 
# MAGIC 
# MAGIC Pandas API on Spark overcomes the limitation, enabling users to work with large datasets by leveraging Spark!
# MAGIC 
# MAGIC 
# MAGIC **As result, Data Scientists can access dataset in Unity Catalog with simple SQL or spark command, and then switch to the API they know (pandas) best without having to worry about the table size and scalability!**
# MAGIC 
# MAGIC *Note: Starting with spark 3.2, pandas API are directly part of spark runtime, no need to import external library!*
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fkoalas%2Fnotebook%2Fkoalas&dt=ML">

# COMMAND ----------

# DBTITLE 1,Initialize data
# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

display_slide('1jlWYocKPAVERC_u7GLCsrpbmIEGWkaB8GgYSO3ktHSE', '6') #HIDE_THIS_CODE

# COMMAND ----------

# MAGIC %md ## Loading data from files at scale using pandas API

# COMMAND ----------

# DBTITLE 1,Reading json file with pandas API & spark distributed speed
#instead of Pandas read_json (from pandas import read_json)
#just import pyspark pandas to leverage spark distributed capabilities:
from pyspark.pandas import read_json
pdf = read_json(cloud_storage_path+"/koalas/users")
print(f"pdf is of type {type(pdf)}")
display(pdf)

# COMMAND ----------

# DBTITLE 1,Pandas Dataframe to Pandas On Spark Dataframe 
from pyspark.pandas import from_pandas
dates = pd.date_range('20130101', periods=6)
df = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
#Transform our pandas dataframe to pandas on spark & have all spark speed & parallelism
pdf = from_pandas(df)
print(f"pdf is of type {type(pdf)}")
#Apply standard pandas operationhttps://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#
pdf.mean()

# COMMAND ----------

# DBTITLE 1,Spark dataframe to Pandas On Spark dataframe
# Spark dataframe can also be switched to a Pandas dataframe with one instruction 
# As example, You could easily read one table from Unity Catalog, then leverage pandas API for your Data Science analysis
users = spark.read.json(cloud_storage_path+"/koalas/users").pandas_api()
print(f"users is of type {type(users)}")
users.describe()

# COMMAND ----------

# MAGIC %md ## Pandas APIs are now fully available to explore our dataset
# MAGIC Existing pandas code will now work and scale out of the box:

# COMMAND ----------

# DBTITLE 1,Data exploration
users["age_group"].value_counts(dropna=False).sort_values()

# COMMAND ----------

# MAGIC %md ## Running SQL on top of pandas dataframe
# MAGIC Pandas dataframe on spark can be queried using plain SQL, allowing a perfect match between pandas python API and SQL usage

# COMMAND ----------

# DBTITLE 1,Import pandas SQL, and run any SQL referencing the pandas dataframe by name:
from pyspark.pandas import sql
age_group = 2
sql("""SELECT age_group, COUNT(*) AS customer_per_segment FROM {users} 
        where age_group > {age_group}
      GROUP BY age_group ORDER BY age_group """, users=users, age_group=age_group)

# COMMAND ----------

# MAGIC %md ## Pandas visualisation
# MAGIC Pandas api on spark use plotly with interactive charts. Use the standard `.plot` on a pandas dataframe to get insights: 

# COMMAND ----------

df = users.groupby('gender')['age_group'].value_counts().unstack(0)
df.plot.bar(title="Customer age distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's it! you're now ready to use all the capabilities of Databricks while staying on the API you know the best!
# MAGIC 
# MAGIC ## Going further with koalas
# MAGIC 
# MAGIC Want to know more? Check the [koalas documentation](https://koalas.readthedocs.io/en/latest/) and the integration with [spark 3.2](https://databricks.com/fr/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html)
