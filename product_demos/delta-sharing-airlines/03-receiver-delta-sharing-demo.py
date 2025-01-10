# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing the data as a Consumer
# MAGIC
# MAGIC In the previous notebook, we shared our data and granted read access to our RECIPIENT.
# MAGIC
# MAGIC Let's now see how external consumers can directly access the data.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/delta-sharing/resources/images/delta-sharing-flow.png" width="900px"/>
# MAGIC
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_sharing%2Fnotebook_consumer%2Faccess&dt=FEATURE_DELTA_SHARING">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Delta Sharing Credentials
# MAGIC
# MAGIC When a new Recipient entity is created for a Delta Share an activation link for that recipient will be generated. That URL will lead to a website for data recipients to download a credential file that contains a long-term access token for that recipient. Following the link will be take the recipient to an activation page that looks similar to this:
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/kanonymity_share_activation.png" width=600>
# MAGIC
# MAGIC
# MAGIC From this site the .share credential file can be downloaded by the recipient. This file contains the information and authorization token needed to access the Share. The contents of the file will look similar to the following example.
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_sharing_cred_file_3.png" width="800">
# MAGIC
# MAGIC Due to the sensitive nature of the token, be sure to save it in a secure location and be careful when visualising or displaying the contents. 

# COMMAND ----------

# DBTITLE 0,Let's Start With the Vast and Trusted Python Developers as Consumers
# MAGIC %md
# MAGIC # Accessing the data using plain Python
# MAGIC
# MAGIC `delta-sharing` is available as a python package that can be installed via pip. <br>
# MAGIC
# MAGIC This simplifies the consumer side integration; anyone who can run python can consume shared data via SharingClient object. <br>

# COMMAND ----------

# DBTITLE 1,Step 0: Installing Delta Sharing library
# MAGIC %pip install delta-sharing

# COMMAND ----------

import delta_sharing
# Southwest Airlines
# In the previous notebook, we saved the credential file under dbfs:/FileStore/southwestairlines.share
# Let's re-use it directly to access our data. If you get access error, please re-run the previous notebook
americanairlines_profile = '/Volumes/main__build/dbdemos_sharing_airlinedata/raw_data/americanairlines.share'

# Create a SharingClient
client = delta_sharing.SharingClient(americanairlines_profile)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC It is possible to iterate through the list to view all of the tables along with their corresponding schemas and shares. <br>
# MAGIC The share file can be stored on a remote storage.

# COMMAND ----------

shares = client.list_shares()

for share in shares:
    schemas = client.list_schemas(share)
    for schema in schemas:
        tables = client.list_tables(schema)
        for table in tables:
            print(f'Table Name = {table.name}, share = {table.share}, schema = {table.schema}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Query the Shared Table Using the Ever so Popular Pandas

# COMMAND ----------

# MAGIC %md
# MAGIC Delta sharing allows us to access data via Pandas connector. <br>
# MAGIC To access the shared data we require a properly constructed url. <br>
# MAGIC The expected format of the url is: < profile_file \>#< share_id \>.< database \>.< table \><br>

# COMMAND ----------

table_url = f"{americanairlines_profile}#dbdemos_americanairlines.dbdemos_sharing_airlinedata.lookupcodes"

# Use delta sharing client to load data
flights_df = delta_sharing.load_as_pandas(table_url)

flights_df.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # Query Big Dataset using Spark

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly to Pandas connect delta sharing comes with a spark connector. <br>
# MAGIC The way to specify the location of profile file slightly differs between connectors. <br>
# MAGIC For spark connector the profile file path needs to be HDFS compliant. <br>

# COMMAND ----------

# MAGIC %md
# MAGIC To load the data into spark, we can use delta sharing client.

# COMMAND ----------

spark_flights_df = delta_sharing.load_as_spark(f"{americanairlines_profile}#dbdemos_americanairlines.dbdemos_sharing_airlinedata.flights_protected")

from pyspark.sql.functions import sum, col, count

display(spark_flights_df.
        where('cancelled = 1').
        groupBy('UniqueCarrier', 'month', 'year').
        agg(count('FlightNum').alias('Total Cancellations')).
        orderBy(col('year').asc(), col('month').asc(), col('Total Cancellations').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, we can use 'deltaSharing' fromat in spark reader. 

# COMMAND ----------

spark_flights_df = spark.read.format('deltaSharing').load(f"{americanairlines_profile}#dbdemos_americanairlines.dbdemos_sharing_airlinedata.lookupcodes")

display(spark_flights_df.
        where('cancelled = 1').
        groupBy('UniqueCarrier', 'month').
        agg(count('FlightNum').alias('Total Cancellations')).
        orderBy(col('month').asc(), col('Total Cancellations').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Query your Delta Sharing table using plain SQL with Databricks!
# MAGIC
# MAGIC As a Databricks user, you can experience Delta Sharing using plain SQL directly in your notebook, making data access even easier.
# MAGIC
# MAGIC It's then super simple to do any kind of queries using the remote table, including joining a Delta Sharing table with a local one or any other operation.

# COMMAND ----------

# MAGIC %md 
# MAGIC We can create a SQL table and use `'deltaSharing'` as a datasource. <br>
# MAGIC As usual, we need to provide the url as: `< profile_file >#< share_id >.< database >.< table >` <br>
# MAGIC Note that in this case we cannot use secrets since other parties would be able to see the token in clear text via table properties.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dbdemos_delta_sharing_demo_flights;
# MAGIC CREATE TABLE IF NOT EXISTS dbdemos_delta_sharing_demo_flights
# MAGIC     USING deltaSharing
# MAGIC     LOCATION "/Volumes/main__build/dbdemos_sharing_airlinedata/raw_data/americanairlines.share#dbdemos_americanairlines.dbdemos_sharing_airlinedata.flights_protected";

# COMMAND ----------

# MAGIC %sql select * from dbdemos_delta_sharing_demo_flights

# COMMAND ----------

# DBTITLE 1,Make sure you delete your demo catalog at the end of the demo
#CLEANUP THE DEMO FOR FRESH START, delete all share and recipient created
#cleanup_demo()

# COMMAND ----------

# MAGIC %md
# MAGIC # Integration with external tools such as Power BI
# MAGIC
# MAGIC Delta Sharing is natively integrated with many tools outside of Databricks. 
# MAGIC
# MAGIC As example, users can natively access a Delta Sharing table within powerBI directly:

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <iframe width="560" height="315" src="https://www.youtube.com/embed/vZ1jcDh_tsw" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Conclusion
# MAGIC To recap, Delta Sharing is a cloud and platform agnostic solution to share your data with external consumer. 
# MAGIC
# MAGIC It's simple (pure SQL), open (can be used on any system) and scalable.
# MAGIC
# MAGIC All recipients can access your data, using Databricks or any other system on any Cloud.
# MAGIC
# MAGIC Delta Sharing enable critical use cases around Data Sharing and Data Marketplace. 
# MAGIC
# MAGIC When combined with Databricks Unity catalog, it's the perfect too to accelerate your Datamesh deployment and improve your data governance.
# MAGIC
# MAGIC Next: Discover how to easily [Share data within Databricks with Unity Catalog]($./04-share-data-within-databricks)
# MAGIC
# MAGIC
# MAGIC [Back to Overview]($./01-Delta-Sharing-presentation)
