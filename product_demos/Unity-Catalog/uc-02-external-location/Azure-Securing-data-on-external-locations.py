# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Securing access to External Tables / Files with Unity Catalog
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/uc-external-location-global.png?raw=true" style="float:right; margin-left:10px" width="600"/>
# MAGIC
# MAGIC By default, Unity Catalog will create managed tables in your primary storage, providing a secured table access for all your users.
# MAGIC
# MAGIC In addition to these managed tables, you can manage access to External tables and files, located in another cloud storage (S3/ADLS/GCS). 
# MAGIC
# MAGIC This give you capabilities to ensure a full data governance, storing your main tables in the managed catalog/storage while ensuring secure access for for specific cloud storage.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=Azure-Securing-data-on-external-locations&demo_name=uc-02-external-location&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cluster setup for UC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/clusters_assigned.png?raw=true" style="float: right"/>
# MAGIC
# MAGIC
# MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
# MAGIC
# MAGIC Go in the compute page, create a new cluster.
# MAGIC
# MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC ## Working with External Locations
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/azure-uc-external-location.png?raw=true" style="float:right; margin-left:10px" width="800"/>
# MAGIC
# MAGIC
# MAGIC Accessing external cloud storage is easily done using `External locations`.
# MAGIC
# MAGIC This can be done using 3 simple SQL command:
# MAGIC
# MAGIC
# MAGIC 1. First, create a Storage credential. It'll reference the Managed Identity or Service Principal required to access your cloud storage
# MAGIC 1. Create an External location using your Storage credential. It can be any cloud location (a sub folder)
# MAGIC 1. Finally, Grant permissions to your users to access this Storage Credential

# COMMAND ----------

# MAGIC %md-sandbox ## 1/ Create the STORAGE CREDENTIAL
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/azure-uc-external-location-1.png?raw=true" style="float:right; margin-left:10px" width="700px"/>
# MAGIC
# MAGIC The first step is to create the `STORAGE CREDENTIAL`.
# MAGIC
# MAGIC To do that, we'll use Databricks Unity Catalog UI:
# MAGIC
# MAGIC 1. Open the Data Explorer in DBSQL
# MAGIC 1. Select the "Storage Credential" menu
# MAGIC 1. Click on "Create Credential"
# MAGIC 1. Since it is recommended to use a Managed Identity, fill out the fields with your credential information: Storage credential name, Access connector ID, and optionally your User assigned managed identity ID.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/azure-storage-credential.png?raw=true" width="400"/>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For our demo, let's make sure all users can alter this storage credential:
# MAGIC ALTER STORAGE CREDENTIAL `field_demos_credential`  OWNER TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE STORAGE CREDENTIAL `field_demos_credential`

# COMMAND ----------

# MAGIC %md-sandbox ## 2/ Create the EXTERNAL LOCATION
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/azure-uc-external-location-2.png?raw=true" style="float:right; margin-left:10px" width="700px"/>
# MAGIC
# MAGIC
# MAGIC We'll then create our `EXTERNAL LOCATION` using the following path:<br/>
# MAGIC `abfss://deltalake@oneenvadls.dfs.core.windows.net/demofieldeng/external_location/`
# MAGIC
# MAGIC Note that you need to be Account Admin to do that, it'll fail with a permission error if you are not. But don't worry, the external location has been created for you.
# MAGIC
# MAGIC You can also update your location using SQL operations:
# MAGIC <br/>
# MAGIC ```ALTER EXTERNAL LOCATION `xxxx`  RENAME TO `yyyy`; ```<br/>
# MAGIC ```DROP EXTERNAL LOCATION IF EXISTS `xxxx`; ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace this with your own path
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `field_demos_external_location`
# MAGIC   URL 'abfss://deltalake@oneenvadls.dfs.core.windows.net/demofieldeng/external_location/' 
# MAGIC   WITH (CREDENTIAL `field_demos_credential`)
# MAGIC   COMMENT 'External Location for demos' ;
# MAGIC
# MAGIC -- let's make everyone owner for the demo to be able to change the permissions easily. DO NOT do that for real usage.
# MAGIC ALTER EXTERNAL LOCATION `field_demos_external_location`  OWNER TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION `field_demos_external_location`;

# COMMAND ----------

# MAGIC %md-sandbox ## 3/ GRANT permissions on the external location
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/azure-uc-external-location-3.png?raw=true" style="float:right; margin-left:10px" width="700px"/>
# MAGIC
# MAGIC All we have to do is now GRANT permission to our users or group of users. In our demo we'll grant access to all our users using `account users`
# MAGIC
# MAGIC We can set multiple permissions:
# MAGIC
# MAGIC 1. READ FILES to be able to access the data
# MAGIC 1. WRITE FILES to be able to write data
# MAGIC 1. CREATE TABLE to create external table using this location
# MAGIC
# MAGIC To revoke your permissions, you can use ```REVOKE WRITE FILES ON EXTERNAL LOCATION `field_demos_external_location` FROM `account users`;```

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `field_demos_external_location` TO `account users`;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Accessing the data
# MAGIC
# MAGIC That's all we have to do! Our users can now access the folder in SQL or python:

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST 'abfss://deltalake@oneenvadls.dfs.core.windows.net/demofieldeng/external_location/'

# COMMAND ----------

# MAGIC %md we can also write data using SQL or Python API:

# COMMAND ----------

df = spark.createDataFrame([("UC", "is awesome"), ("Delta Sharing", "is magic")])
df.write.mode('overwrite').format('csv').save('abfss://deltalake@oneenvadls.dfs.core.windows.net/demofieldeng/external_location/test_write')

# COMMAND ----------

# DBTITLE 1,Reading the data using pyspark API:
spark.read.csv('abfss://deltalake@oneenvadls.dfs.core.windows.net/demofieldeng/external_location/test_write').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Setting the Permissions can also be done using the Data Explorer UI:
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/external/uc-external-location-cred2.png?raw=true" width="400" >
# MAGIC
# MAGIC *Note: because we have set all users to OWNER for the demo, all users have full READ/WRITE permissions as OWNER (even without the GRANT). In a real setup, a single admin would be the OWNER, granting specific access to group of users or specific users.*

# COMMAND ----------

# MAGIC %md ## Conclusion
# MAGIC
# MAGIC With Unity Catalog, you can easily secure access to external locations and grant access based on users/groups.
# MAGIC
# MAGIC This let you operate security at scale, cross workspace, and be ready to build data mesh setups.
