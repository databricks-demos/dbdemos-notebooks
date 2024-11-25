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
# MAGIC CREATE OR REPLACE TABLE workspace.default.repro1 (
# MAGIC     id BIGINT,
# MAGIC     firstname STRING,
# MAGIC     lastname STRING,
# MAGIC     email STRING
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.universalFormat.enabledFormats' = 'iceberg', 
# MAGIC     'delta.enableIcebergCompatV2' = 'true'
# MAGIC );

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE repro3 (
    id BIGINT,
    firstname STRING,
    lastname STRING,
    email STRING)
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg', 
    'delta.enableIcebergCompatV2' = 'true'
)""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO user_uniform SELECT id, firstname, lastname, email FROM user_delta;  
# MAGIC
# MAGIC SELECT * FROM user_uniform;

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
# MAGIC SHOW TBLPROPERTIES user_uniform;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access your Delta Lake table using Iceberg REST Catalog API
# MAGIC
# MAGIC Our Delta Lake table is now available by any system reading Iceberg tables, such as native Iceberg reader or external system like Big Query.
# MAGIC
# MAGIC If you're using an external storage, Databricks expose the table information through:

# COMMAND ----------

from databricks.sdk import WorkspaceClient
ws = WorkspaceClient() 
table_info = ws.api_client.do('GET', f'/api/2.1/unity-catalog/tables/{catalog}.{schema}.user_uniform')
table_info['delta_uniform_iceberg']

# COMMAND ----------

# MAGIC %md
# MAGIC To read a managed table, you can leverage the Iceberg Catalog:
# MAGIC
# MAGIC ```
# MAGIC curl -X GET -H "Authentication: Bearer $OAUTH_TOKEN" -H "Accept: application/json" \
# MAGIC https://<workspace-instance>/api/2.1/unity-catalog/iceberg/v1/catalogs/<uc_catalog_name>/namespaces/<uc_schema_name>/tables/<uc_table_name>
# MAGIC ```
# MAGIC
# MAGIC You should then receive a response like this:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "metadata-location": "s3://bucket/path/to/iceberg/table/metadata/file",
# MAGIC   "metadata": <iceberg-table-metadata-json>,
# MAGIC   "config": {
# MAGIC     "expires-at-ms": "<epoch-ts-in-millis>",
# MAGIC     "s3.access-key-id": "<temporary-s3-access-key-id>",
# MAGIC     "s3.session-token":"<temporary-s3-session-token>",
# MAGIC     "s3.secret-access-key":"<temporary-secret-access-key>",
# MAGIC     "client.region":"<aws-bucket-region-for-metadata-location>"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC For more details, see [the Uniform Documentation](https://docs.databricks.com/en/delta/uniform.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC That's it! You can now access all your Delta Lake table as Iceberg table, while getting the power of Delta Lake and bazing fast queries with Liquid Clustering.
# MAGIC
# MAGIC Your lakehouse is now fully open, without any vender lock-in. 
# MAGIC
# MAGIC
# MAGIC Next: Deep dive into Delta Lake Change Data Capture capability with [the 04-Delta-Lake-CDF notebook]($./04-Delta-Lake-CDF) or go back to [00-Delta-Lake-Introduction]($./00-Delta-Lake-Introduction).
# MAGIC
