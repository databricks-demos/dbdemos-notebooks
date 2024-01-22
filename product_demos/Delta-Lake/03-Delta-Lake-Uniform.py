# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Delta Lake Uniform: Universal Format for all your Lakehouse tools
# MAGIC
# MAGIC
# MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/image1_5.png" width="700px" style="float: right; margin-left: 50px"/>
# MAGIC
# MAGIC Companies want to leverage open format and stay away from vendor lockin. Migration is costly and difficult, so they want to make the right decision up front and only have to save data once. 
# MAGIC
# MAGIC They ultimately want the best performance at the cheapest price for all of their data workloads including ETL, BI, and AI, and the flexibility to consume that data anywhere.
# MAGIC
# MAGIC
# MAGIC Delta Universal Format (UniForm) automatically unifies table formats, without creating additional copies of data or more data silos. 
# MAGIC
# MAGIC Teams that use query engines designed to work with Iceberg or Hudi data will be able to read Delta tables seamlessly, without having to copy data over or convert it. 
# MAGIC
# MAGIC Customers don’t have to choose a single format, because **tables written by Delta will be universally accessible by Iceberg and Hudi readers.**
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Fperf&dt=FEATURE_DELTA">
# MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{}}] -->

# COMMAND ----------

# DBTITLE 1,Init the demo data
# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Leveraging Delta Lake power across all formats
# MAGIC
# MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/image3_2.png" style="float: right" width="650px" />
# MAGIC
# MAGIC UniForm takes advantage of the fact that all three open lakehouse formats are thin layers of metadata atop Parquet data files. As writes are made, UniForm will incrementally generate this layer of metadata to spec for Hudi, Iceberg and Delta.
# MAGIC
# MAGIC UniForm introduces negligible performance and resource overhead. 
# MAGIC
# MAGIC We also saw improved read performance on UniForm-enabled tables relative to native Iceberg tables, thanks to Delta’s improved data layout capabilities .
# MAGIC
# MAGIC With UniForm, customers can choose Delta with confidence, knowing that they’ll have broad support from any tool that supports lakehouse formats.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE user_uniform (
# MAGIC     id BIGINT,
# MAGIC     firstname STRING,
# MAGIC     lastname STRING,
# MAGIC     email STRING)
# MAGIC   TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');
# MAGIC
# MAGIC INSERT INTO user_uniform SELECT id, firstname, lastname, email FROM user_delta;  
# MAGIC
# MAGIC SELECT * FROM user_uniform

# COMMAND ----------

# MAGIC %md
# MAGIC Uniform delta tables are available as any other table. Because we enabled `iceberg` as format, each subsequential write will update the Delta and Iceberg metadata. <br>
# MAGIC Technically speaking, your table now contains 2 metadata folders:
# MAGIC
# MAGIC * `delta_log` containing all Delta Lake format metadata
# MAGIC * `metadata` containing Iceberg format metadata
# MAGIC
# MAGIC
# MAGIC Your Delta Lake table is still available like any other table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_uniform;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access your Delta Lake table using Iceberg
# MAGIC
# MAGIC Our Delta Lake table is now available by any system reading Iceberg tables, such as native Iceberg reader or Big Query.
# MAGIC
# MAGIC For this demo, let's install python iceberg and access the table directly using Iceberg.

# COMMAND ----------

# DBTITLE 1,Install python iceberg package 
# MAGIC %pip install pyiceberg

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,With Uniform, a new /metadata folder is created containing iceberg metadata:
location = next(filter(lambda d: d['col_name'] == 'Location', spark.sql('describe extended user_uniform').collect()), None)
#Iceberg metadata folder
metadata = dbutils.fs.ls(location['data_type']+'/metadata')
metadata.sort(key=lambda x: x.modificationTime, reverse=True)
display(metadata)
json_metadata_file = next(filter(lambda file: file.name.endswith('.json'), metadata))
print(f'Iceberg json metadata file: {json_metadata_file}')

# COMMAND ----------

# DBTITLE 1,Access our table natively from metadata with iceberg
from pyiceberg.table import StaticTable

table = StaticTable.from_metadata(json_metadata_file.path)
table.scan().to_pandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC That's it! You can now access all your Delta Lake table as Iceberg table, while getting the power of Delta Lake and bazing fast queries with Liquid Clustering.
# MAGIC
# MAGIC Your lakehouse is now fully open, without any vender lock-in. 
# MAGIC
# MAGIC
# MAGIC Next: Deep dive into Delta Lake Change Data Capture capability with [the 04-Delta-Lake-CDF notebook]($./04-Delta-Lake-CDF) or go back to [00-Delta-Lake-Introduction]($./00-Delta-Lake-Introduction).
# MAGIC
