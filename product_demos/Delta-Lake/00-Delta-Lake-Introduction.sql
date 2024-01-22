-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Getting started with Delta Lake
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:300px; float: right"/>
-- MAGIC
-- MAGIC [Delta Lake](https://delta.io/) is an open storage format used to save your data in your Lakehouse. Delta provides an abstraction layer on top of files. It's the storage foundation of your Lakehouse.
-- MAGIC
-- MAGIC ## Why Delta Lake?
-- MAGIC
-- MAGIC Running ingestion pipeline on Cloud Storage can be very challenging. Data teams are typically facing the following challenges:
-- MAGIC
-- MAGIC * Hard to append data *(Adding newly arrived data leads to incorrect reads)*
-- MAGIC * Modification of existing data is difficult (*GDPR/CCPA requires making fine grained changes to existing data lake)*
-- MAGIC * Jobs failing mid way (*Half of the data appears in the data lake, the rest is missing)*
-- MAGIC * Real-time operations (*Mixing streaming and batch leads to inconsistency)*
-- MAGIC * Costly to keep historical versions of the data (*Regulated environments require reproducibility, auditing, governance)*
-- MAGIC * Difficult to handle large metadata (*For large data lakes the metadata itself becomes difficult to manage)*
-- MAGIC * “Too many files” problems (*Data lakes are not great at handling millions of small files)*
-- MAGIC * Hard to get great performance (*Partitioning the data for performance is error-prone and difficult to change)*
-- MAGIC * Data quality issues (*It’s a constant headache to ensure that all the data is correct and high quality)*
-- MAGIC
-- MAGIC Theses challenges have a real impact on team efficiency and productivity, spending unecessary time fixing low-level, technical issues instead on focusing on the high-level, business implementation.
-- MAGIC
-- MAGIC Because Delta Lake solves all the low level technical challenges saving PB of data in your lakehouse, it let you focus on implementing simple data pipeline while providing blazing-fast query answers for your BI & Analytics reports.
-- MAGIC
-- MAGIC In addition, Delta Lake being a fully open source project under the Linux Foundation and adopted by most of the data players, you know you own your own data and won't have vendor lock-in.
-- MAGIC
-- MAGIC ## Delta Lake capabilities?
-- MAGIC
-- MAGIC
-- MAGIC You can think about Delta as a file format that your engine can leverage to bring the following capabilities out of the box:
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-lake-acid.png" style="width:400px; float: right; margin: 0px 0px 0px 0px"/>
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC * ACID transactions
-- MAGIC * Support for DELETE/UPDATE/MERGE
-- MAGIC * Unify batch & streaming
-- MAGIC * Time Travel
-- MAGIC * Clone zero copy
-- MAGIC * Generated partitions
-- MAGIC * CDF - Change Data Flow (DBR runtime)
-- MAGIC * Blazing-fast queries
-- MAGIC * ...
-- MAGIC
-- MAGIC Let's explore these capabilities! *We'll mainly use SQL, but all the operations are available in python/scala*
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Fintro&dt=FEATURE_DELTA">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Getting started with Delta Lake
-- MAGIC
-- MAGIC Start learning about Delta Lake:
-- MAGIC
-- MAGIC * Table creation & migration
-- MAGIC * Streaming
-- MAGIC * Time Travel 
-- MAGIC * Upsert (merge)
-- MAGIC * Enforce Quality with Constraint
-- MAGIC * Clone & Restore
-- MAGIC * Advanced: 
-- MAGIC   * PK/FK support
-- MAGIC   * Share data with Delta Sharing Open Protocol
-- MAGIC   
-- MAGIC Open the first [01-Getting-Started-With-Delta-Lake notebook]($./01-Getting-Started-With-Delta-Lake) 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Speedup your queries with Liquid Clustering
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/delta/delta-liquid-1.png?raw=true" style="float: right" width="300px">
-- MAGIC
-- MAGIC Delta Lake let you add Liquid clustering column (similar to indexes).
-- MAGIC
-- MAGIC This automatically adapt your data layout accordingly and drastically accelerate your reads, providing state of the art performances.
-- MAGIC
-- MAGIC Liquid Clustering makes Hive Partitioning skew and small size a thing of the past.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open [the 02-Delta-Lake-Performance notebook]($./02-Delta-Lake-Performance) to explore Liquid Clustering.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Delta Lake Uniform (Universal Format)
-- MAGIC
-- MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/image1_5.png" width="700px" style="float: right">
-- MAGIC
-- MAGIC While Delta Lake includes unique features to simplify data management and provide the best performances (see [CIDR benchmark paper](https://petereliaskraft.net/res/cidr_lakehouse.pdf)), external systems might require to read other formats such as Iceberg or Hudi. 
-- MAGIC
-- MAGIC Because your Lakehouse is open, Delta Lake let you write your Delta tables with metadata to support these formats.
-- MAGIC
-- MAGIC **This makes Delta Lake the de-facto standard for all your lakehouse tables, leveraging its unique capabilities such as Liquid Clustering to speed up queries, while making sure you won't have any lock-in.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For more details, open [the 03-Delta-Lake-Uniform notebook]($./03-Delta-Lake-Uniform) to explore Delta Lake Uniform.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 4/ Change Data Capture with Delta Lake CDF
-- MAGIC
-- MAGIC Delta Lake makes it easy to capture changes on a table. 
-- MAGIC
-- MAGIC External users can stream the row modifications, making it easy to capture UPDATE, APPEND or DELETE and apply these changes downstream. 
-- MAGIC
-- MAGIC This is key to share data across organization and building Delta Mesh, including DELETE propagation to support GDPR compliance.
-- MAGIC
-- MAGIC CDC is also available through Delta Sharing, making it easy to share data with external organizations.
-- MAGIC
-- MAGIC
-- MAGIC For more details, open [the 04-Delta-Lake-CDF notebook]($./04-Delta-Lake-CDF) to Capture your Delta Lake changes.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 5/ Explore advanced Delta Lake internal
-- MAGIC
-- MAGIC Want to know more about Delta Lake and its underlying metadata? 
-- MAGIC
-- MAGIC Open [the 05-Advanced-Delta-Lake-Internal notebook]($./05-Advanced-Delta-Lake-Internal) for more details.
