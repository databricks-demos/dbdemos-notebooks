-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # B2B Data Exchange with Delta Sharing
-- MAGIC
-- MAGIC On this notebook, we'll explore how to create a SHARE to share data with another organization.
-- MAGIC
-- MAGIC
-- MAGIC ##  Discovering the data
-- MAGIC To Illustrate let's consider us a company like **TripActions**, a Corporate Travel & Spend Management Platform. 
-- MAGIC
-- MAGIC We have already adopted a <b> Delta Lakehouse Architecture </b> for servicing all of our data internally. 
-- MAGIC
-- MAGIC A few of our largest partnered airlines, <b>American Airlines</b> & <b>Southwest</b> just let us know that they are looking to partner to add reward and recommendation programs to airline customers using TripActions data. In order to pilot this new feature, they need daily data of scheduled and results of flights taking within TripActions.
-- MAGIC
-- MAGIC We'll leverage Delta Sharing to grant data access to Americal Airlines and Southwest without data duplication and replication. 
-- MAGIC
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_sharing%2Fnotebook_provider%2Faccess&dt=FEATURE_DELTA_SHARING">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Cluster setup for UC
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
-- MAGIC
-- MAGIC
-- MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
-- MAGIC
-- MAGIC Go in the compute page, create a new cluster.
-- MAGIC
-- MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)
-- MAGIC
-- MAGIC **Make sure your cluster is using DBR 11.2+**

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Sharing
-- MAGIC
-- MAGIC Delta Sharing let you share data with external recipient without creating copy of the data. Once they're authorized, recipients can access and download your data directly.
-- MAGIC
-- MAGIC In Delta Sharing, it all starts with a Delta Lake table registered in the Delta Sharing Server by the data provider. <br/>
-- MAGIC This is done with the following steps:
-- MAGIC - Create a RECIPIENT and share activation link with your recipient 
-- MAGIC - Create a SHARE
-- MAGIC - Add your Delta tables to the given SHARE
-- MAGIC - GRANT SELECT on your SHARE to your RECIPIENT
-- MAGIC  
-- MAGIC Once this is done, your customer will be able to download the credential files and use it to access the data directly:
-- MAGIC
-- MAGIC - Client authenticates to Sharing Server
-- MAGIC - Client requests a table (including filters)
-- MAGIC - Server checks access permissions
-- MAGIC - Server generates and returns pre-signed short-lived URLs
-- MAGIC - Client uses URLs to directly read files from object storage
-- MAGIC <br>
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow.png" width="1000" />
-- MAGIC
-- MAGIC ## Unity Catalog
-- MAGIC Databricks Unity Catalog is the central place to administer your data governance and security.<br/>
-- MAGIC Unity Catalog’s security model is based on standard ANSI SQL, to grant permissions at the level of databases, tables, views, rows and columns<br/>
-- MAGIC Using Databricks, we'll leverage the Unity Catalog to easily share data with our customers.

-- COMMAND ----------

-- DBTITLE 1,Unity Catalog’s security model is based on standard ANSI SQL, to grant permissions at the level of databases, tables, views, rows and columns 
-- the catalog has been created for your user and is defined as default. All shares will be created inside.
-- make sure you run the 00-setup cell above to init the catalog to your user. 
SELECT CURRENT_CATALOG(), CURRENT_SCHEMA();

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Step 1: Create a Share
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow-1.png" width="700" style="float:right" />
-- MAGIC
-- MAGIC We'll use the UNITY catalog to create 2 shares:
-- MAGIC - One for American Airlines data
-- MAGIC - One for Southwest Airlines data

-- COMMAND ----------

-- Note: you need to be account ADMIN to create the shares or GRANT CREATE PERMISSION to another principal:
-- GRANT CREATE SHARE ON metastore TO `<my_principal@xx.com>`;
-- GRANT CREATE RECIPIENT ON metastore TO `<my_principal@xx.com>`;

CREATE SHARE IF NOT EXISTS dbdemos_americanairlines 
COMMENT 'Daily Flight Data provided by Tripactions to American Airlines for Extended Rewards';

CREATE SHARE IF NOT EXISTS dbdemos_southwestairlines 
COMMENT 'Daily Flight Data provided by Tripactions to Southwest Airlines for Extended Rewards';

-- You can grant ownership to other users. Typical deployments wouls have admin groups or similar.
-- ALTER SHARE dbdemos_americanairlines OWNER TO `<my_principal@xx.com>`;
-- ALTER SHARE dbdemos_southwestairlines OWNER TO `<my_principal@xx.com>`;

-- COMMAND ----------

-- DBTITLE 1,View a Share’s Metadata
DESCRIBE SHARE dbdemos_southwestairlines;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/delta-sharing-create-recipient.png" width="500" style="float:right" />
-- MAGIC
-- MAGIC **Did you know?** Delta Sharing isn't about SQL only. 
-- MAGIC
-- MAGIC You can visualize all your Delta Sharing Shares using Databricks Data Explorer UI!
-- MAGIC
-- MAGIC You can also create your share and recipient with just a few click.<br/>
-- MAGIC Select "Delta Sharing" in the Data Explorer menu, then "Create Share", "Create recipient" ...

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Step 2: Add the tables to the SHARES
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow-2.png" width="700" style="float:right" />
-- MAGIC
-- MAGIC We'll add our main table `airlinedata.lookupcodes` to the 2 SHARES:

-- COMMAND ----------

-- DBTITLE 1,Add our airlines tables to the SHARE
--UNCOMMENT_FOR_DEMO ALTER SHARE dbdemos_americanairlines  ADD TABLE lookupcodes ;
--UNCOMMENT_FOR_DEMO ALTER SHARE dbdemos_southwestairlines ADD TABLE lookupcodes;

-- COMMAND ----------

-- DBTITLE 1,Get In-House Unique Carrier Codes to Filter Specifically to Only the Relevant Airlines
SELECT * FROM lookupcodes WHERE Description = "Southwest Airlines Co." OR Description = "American Airlines Inc."

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sharing a subset of a table to a SHARE recipient based on dynamic properties
-- MAGIC We shouldn't share all the historical flights to all Airline. It might be private information and we don't want all our consumers accessing the entire `flights` table. 
-- MAGIC <br>
-- MAGIC #### Customizing Consumer Experience
-- MAGIC To restrict the data access, we can properties on the recipient Shares, and then create a dynamic view that will query these properties.
-- MAGIC
-- MAGIC Note: before supporting VIEW, you could also restrict the access through Delta partition, but this is much less flexible.
-- MAGIC ```
-- MAGIC  ALTER SHARE americanairlines 
-- MAGIC    ADD TABLE dbdemos_sharing_airlinedata.flights 
-- MAGIC    PARTITION (UniqueCarrier = "AA") as dbdemos_sharing_airlinedata.`aa_flights`;
-- MAGIC ```

-- COMMAND ----------

-- current_recipient('carrier_id') will be resolved to 'WN' or 'AA' based on the current recipient properties (see below to set the property value)
--UNCOMMENT_FOR_DEMO CREATE VIEW main__build.dbdemos_sharing_airlinedata.flights_protected AS
--UNCOMMENT_FOR_DEMO     SELECT * FROM main__build.dbdemos_sharing_airlinedata.flights
--UNCOMMENT_FOR_DEMO     WHERE UniqueCarrier = current_recipient('carrier_id');

-- COMMAND ----------

-- DBTITLE 1,Add Filter to Only Share a Portion of the Flights Table
--UNCOMMENT_FOR_DEMO ALTER SHARE dbdemos_americanairlines 
--UNCOMMENT_FOR_DEMO   ADD VIEW main__build.dbdemos_sharing_airlinedata.flights_protected;

--UNCOMMENT_FOR_DEMO ALTER SHARE dbdemos_southwestairlines 
--UNCOMMENT_FOR_DEMO   ADD VIEW main__build.dbdemos_sharing_airlinedata.flights_protected;

-- COMMAND ----------

-- DBTITLE 1,Display all Tables and Views Inside a Share
SHOW ALL IN SHARE dbdemos_southwestairlines;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Step 3: Create a Recipient(s)
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow-3.png" width="700" style="float:right" />
-- MAGIC
-- MAGIC Our next step is now to create the `RECIPIENT`.
-- MAGIC
-- MAGIC We can have multiple RECIPIENT, and assign them to multiple SHARE.

-- COMMAND ----------

CREATE RECIPIENT IF NOT EXISTS dbdemos_southwestairlines_recipient;
CREATE RECIPIENT IF NOT EXISTS dbdemos_americanairlines_recipient;

ALTER RECIPIENT dbdemos_southwestairlines_recipient SET PROPERTIES ('carrier_id' = 'WN');
ALTER RECIPIENT dbdemos_americanairlines_recipient SET PROPERTIES ('carrier_id' = 'AA');

-- You can set the ownership to a group of admin or similar.
-- ALTER RECIPIENT dbdemos_southwestairlines_recipient OWNER TO `<my_principal>`;
-- ALTER RECIPIENT dbdemos_americanairlines_recipient OWNER TO `<my_principal>`;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Step 4: Share the activation link with external consumers
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow-5.png" width="700" style="float:right" />
-- MAGIC
-- MAGIC Each Recipient has an activation link that the consumer can use to download it's credential.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-credential.png" width=400>
-- MAGIC
-- MAGIC The credentials are typically saved as a file containing. The Delta Server identify and authorize consumer based on these identifiants.<br/>
-- MAGIC Note that the activation link is single use. You can only access it once (it'll return null if already used)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sharing data with customers using Databricks
-- MAGIC
-- MAGIC Sharing data within Databricks is even simpler. All you need to do is get the Metastore ID from your recipient and create the share using it. <br/>
-- MAGIC You won't need any credential file doing so, Databricks Unity Catalog does all the security for you.
-- MAGIC
-- MAGIC `CREATE RECIPIENT IF NOT EXISTS southwestairlines_recipient USING ID 'aws:us-west-2:<the_reciever_recipient>' COMMENT 'Recipient for my external customer using Databricks';`
-- MAGIC
-- MAGIC For more details, open the [Sharing data within Databricks]($./04-share-data-within-databricks) demo.

-- COMMAND ----------

DESCRIBE RECIPIENT dbdemos_southwestairlines_recipient

-- COMMAND ----------

-- DBTITLE 1,Let's download our RECIPIENT authentication file under /FileStore/southwestairlines.share
-- MAGIC %python
-- MAGIC #This function just download the credential file for the RECIPIENT and save it under the given location as we'll need it next to access the data.
-- MAGIC download_recipient_credential("dbdemos_southwestairlines_recipient", "/Volumes/main__build/dbdemos_sharing_airlinedata/raw_data/southwestairlines.share")
-- MAGIC download_recipient_credential("dbdemos_americanairlines_recipient", "/Volumes/main__build/dbdemos_sharing_airlinedata/raw_data/americanairlines.share")

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Step 5: Define which Data to Share, and Access Level 
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/product_demos/delta-sharing-flow-4.png" width="600" style="float:right" />
-- MAGIC
-- MAGIC We now have RECIPIENT and SHARE.
-- MAGIC
-- MAGIC The next logical step is to make sure our RECIPIENT can have SELECT access to our SHARE.
-- MAGIC
-- MAGIC As usual, this is done using standard SQL:

-- COMMAND ----------

GRANT SELECT ON SHARE dbdemos_southwestairlines TO RECIPIENT dbdemos_southwestairlines_recipient;
GRANT SELECT ON SHARE dbdemos_americanairlines TO RECIPIENT dbdemos_americanairlines_recipient;

-- COMMAND ----------

-- DBTITLE 1,Step 9: Audit Who has Access to a Share
SHOW GRANT ON SHARE dbdemos_southwestairlines;

-- COMMAND ----------

-- DBTITLE 1,Audit Recipient Level of Access
SHOW GRANT TO RECIPIENT dbdemos_southwestairlines_recipient;

-- COMMAND ----------

-- DBTITLE 1,Revoke Access if Needed
REVOKE SELECT ON SHARE dbdemos_southwestairlines FROM RECIPIENT dbdemos_americanairlines_recipient;

-- COMMAND ----------

-- DBTITLE 1,View Shares tables
SHOW ALL IN SHARE dbdemos_southwestairlines;

-- COMMAND ----------

-- DBTITLE 1,Make sure you delete your demo catalog at the end of the demo
-- MAGIC %python
-- MAGIC #CLEANUP THE DEMO FOR FRESH START, delete all share and recipient created
-- MAGIC #cleanup_demo()

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Let's now see how a Receiver can access the data
-- MAGIC
-- MAGIC We saw how to create the 
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Next: Discover how an external [receiver can your access]($./03-receiver-delta-sharing-demo) or easily [share data within Databricks with Unity Catalog]($./04-share-data-within-databricks)
-- MAGIC
-- MAGIC [Back to Overview]($./01-Delta-Sharing-presentation)
