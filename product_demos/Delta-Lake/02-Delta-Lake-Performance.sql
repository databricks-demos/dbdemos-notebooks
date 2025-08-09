-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-lake-perf-bench.png" width="500" style="float: right; margin-left: 50px"/>
-- MAGIC
-- MAGIC # Delta Lake: Performance made simple
-- MAGIC
-- MAGIC ## Blazing fast query at scale
-- MAGIC
-- MAGIC Delta Lake saves all your table metadata in an efficient format, ranging from efficient queries on small tables (GB) to massive PB-scale tables. 
-- MAGIC
-- MAGIC Delta Lake is designed to be smart and do all the hard job for you. It'll automatically tune your table and read the minimum data required to be able to satisfied your query.
-- MAGIC
-- MAGIC This result in **fast read query**, even with a growing number of data/partitions!
-- MAGIC
-- MAGIC
-- MAGIC In this notebook, we'll see how we can leverage Delta Lake unique capabilities to speedup requests and simplify maintenance operation. For more details, we recommend to read the [documentation](https://docs.databricks.com/delta/file-mgmt.html).
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Fperf&dt=FEATURE_DELTA">
-- MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- DBTITLE 1,Init the demo data under ${raw_data_location}/user_parquet.
-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ##![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Liquid Clustering
-- MAGIC
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/delta/delta-liquid-1.png?raw=true" style="float: right" width="450px">
-- MAGIC
-- MAGIC Data Layout is key to increase performance and query speed. Manual tuning trough hive-style partitioning is not efficient (creating too big or small partitions) and hard to maintain.
-- MAGIC
-- MAGIC To solve this issue, Delta Lake released Liquid Clustering. Liquid will automatically adjusts the data layout based on clustering keys, which helps to avoid the over or under-partitioning problems that can occur with Hive partitioning.
-- MAGIC
-- MAGIC Liquid clustering can be specified on any columns to provide fast access, including high cardinality or data skew. 
-- MAGIC
-- MAGIC * **Liquid is simple**: You set Liquid clustering keys on the columns that are most often queried - no more worrying about traditional considerations like column cardinality, partition ordering, or creating artificial columns that act as perfect partitioning keys.
-- MAGIC * **Liquid is efficient**: It incrementally clusters new data, so you don't need to trade off between improving performance with reducing cost/write amplification.
-- MAGIC * **Liquid is flexible**: You can quickly change which columns are clustered by Liquid without rewriting existing data.
-- MAGIC
-- MAGIC **Delta Liquid Clustering requires DBR 13.2**
-- MAGIC
-- MAGIC For more details, [please read the documentation](https://docs.databricks.com/delta/clustering.html)

-- COMMAND ----------

-- Liquid will properly layout the data to speedup queries by firstname or lastname.
-- this is done by adding the CLUSTER BY keyword during your standard table creation. Clustered table can't have partitions.
CREATE OR REPLACE TABLE user_clustering CLUSTER BY (firstname, lastname)
  AS SELECT * FROM user_delta;

-- COMMAND ----------

-- review the table definition, Liquid Clustering appears under "Clustering Information"
DESCRIBE TABLE user_clustering;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### How to trigger liquid clustering
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/delta/delta-liquid-2.png?raw=true" style="float: right" width="400px">
-- MAGIC
-- MAGIC
-- MAGIC Liquid clustering is incremental, meaning that data is only rewritten as necessary to accommodate data that needs to be clustered.
-- MAGIC
-- MAGIC For best performance, Databricks recommends scheduling regular OPTIMIZE jobs to cluster data. 
-- MAGIC
-- MAGIC For tables experiencing many updates or inserts, Databricks recommends scheduling an OPTIMIZE job every one or two hours. 
-- MAGIC
-- MAGIC Because liquid clustering is incremental, most OPTIMIZE jobs for clustered tables run quickly. No need to specify any ZORDER columns.
-- MAGIC
-- MAGIC *Note: Liquid clustering will automatically re-arrange your data during writes above a given threshold. As with all indexes, this will add a small write cost.*

-- COMMAND ----------

-- DBTITLE 1,Trigger liquid clustering
OPTIMIZE user_clustering;

-- COMMAND ----------

-- DBTITLE 1,Don't forget to the table periodically to remove your history and previous files
VACUUM user_clustering;

-- COMMAND ----------

-- DBTITLE 1,Our requests using firstname and lastname are now super fast!
SELECT * FROM user_clustering where firstname = 'Teresa'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dynamically changing your clustering columns
-- MAGIC
-- MAGIC Liquid table are flexible, you can change your clustering columns without having to re-write all your data. 
-- MAGIC
-- MAGIC Let's make sure our table provides fast queries for ID:

-- COMMAND ----------

-- DBTITLE 1,Dynamically changing your clustering columns
ALTER TABLE user_clustering CLUSTER BY (id, firstname, lastname);

-- COMMAND ----------

-- DBTITLE 1,Disabling liquid clustering:
-- Disable liquid clustering:
ALTER TABLE user_clustering CLUSTER BY NONE;
-- Note: this does not rewrite data that has already been clustered, but prevents future OPTIMIZE operations from using clustering keys.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cluster by Auto
-- MAGIC
-- MAGIC In Databricks Runtime 15.4 LTS and above, you can enable automatic liquid clustering for Unity Catalog managed Delta tables. With automatic liquid clustering enabled, Databricks intelligently chooses clustering keys to optimize query performance. You enable automatic liquid clustering using the CLUSTER BY AUTO clause.
-- MAGIC
-- MAGIC When enabled, automatic key selection and clustering operations run asynchronously as a maintenance operation and require that predictive optimization is enabled for the table.

-- COMMAND ----------

-- DBTITLE 1,Using CLUSTER BY AUTO
ALTER TABLE user_clustering CLUSTER BY AUTO;

-- COMMAND ----------

-- DBTITLE 1,Ensuring That predeitive optimization is enabled at UC
DESCRIBE EXTENDED user_clustering

--Predictive Optimization	ENABLE (inherited from METASTORE unity-catalog-demo)

-- COMMAND ----------

-- DBTITLE 1,Do the first time manual clustering/optimize
OPTIMIZE user_clustering;

-- COMMAND ----------

VACUUM user_clustering;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Auto Liquid Clustering will dynamically change the cluster keys based on the read/write pattern on the table based on column filters, merge keys etc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Compacting without Liquid Clustering
-- MAGIC
-- MAGIC While recommended to accelerate your queries, some tables might not always have Liquid Clustering enabled.
-- MAGIC
-- MAGIC Adding data to the table results in new file creation, and your table can quickly have way too many small files which is going to impact performances over time.
-- MAGIC
-- MAGIC This becomes expecially true with streaming operation where you add new data every few seconds, in near realtime.
-- MAGIC
-- MAGIC Just like for Liquid Clusteing, Delta Lake solves this operation with the `OPTIMIZE` command, which is going to optimize the file layout for you, picking the proper file size based on heuristics. As no Cluster are defined, this will simply compact the files.

-- COMMAND ----------

-- let's compact our table. Note that the engine decided to compact 8 files into 1 ("numFilesAdded": 1, "numFilesRemoved": 8)
OPTIMIZE user_delta 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC These maintenance operation have to be triggered frequently to keep our table properly optimized.
-- MAGIC
-- MAGIC Using Databricks, you can have your table automatically optimized out of the box, without having you to worry about it. All you have to do is set the [proper table properties](https://docs.databricks.com/optimizations/auto-optimize.html), and the engine will optimize your table when needed, without having you to run manual OPTIMIZE operation.
-- MAGIC
-- MAGIC We strongly recommend to enable this option for all your tables.

-- COMMAND ----------

ALTER TABLE user_delta SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Note: Auto Optimize with Liquid Clustering
-- MAGIC
-- MAGIC Liquid Clustering will automatically kick off eager optimization starting from a given write size, based on heuristic. 
-- MAGIC You can also turn on `delta.autoOptimize.optimizeWrite = true` on your liquid table starting from DBR 13.3 to make sure all writes will be optimized. While you can enable `delta.autoOptimize.autoCompact = true`, it won't have any effect for now (as of DBR 13.3, this might change in the future).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Legacy file layout optimizations
-- MAGIC
-- MAGIC Liquid Clustering is the future of Delta Lake optimization and query speedup, and we now recommend starting with Liquid Clustering.
-- MAGIC
-- MAGIC Below are previous Delta Lake optimization leveraging Zordering and Partitioning techniques. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ZORDER
-- MAGIC
-- MAGIC
-- MAGIC ZORDER will optimize the file layout by multiple columns, but it's often used in addition to partitioning and is not as efficient as Liquid Clustering. It'll increase the write amplification and won't solve your small partitions issues.
-- MAGIC
-- MAGIC Below are a few examples on how you can leverage ZORDER, but we strongly recommend switching to Liquid Tables instead.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Adding indexes (ZORDER) to your table
-- MAGIC
-- MAGIC If you request your table using a specific predicat (ex: username), you can speedup your request by adding an index on these columns. We call this operation ZORDER.
-- MAGIC
-- MAGIC You can ZORDER on any column, especially the one having high cardinality (id, firstname etc). 
-- MAGIC
-- MAGIC *Note: We recommand to stay below 4 ZORDER columns for better query performance.*

-- COMMAND ----------

OPTIMIZE user_delta ZORDER BY (id, firstname);

-- our next queries using a filter on id or firstname will be much faster
SELECT * FROM user_delta where id = 4 or firstname = 'Quentin';

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Generated columns for dynamic partitions
-- MAGIC
-- MAGIC Adding partitions to your table is a way of saving data having the same column under the same location. Our engine will then be able to read less data and have better read performances.
-- MAGIC
-- MAGIC Using Delta Lake, partitions can be generated based on expression, and the engine will push-down your predicate applying the same expression even if the request is on the original field.
-- MAGIC
-- MAGIC A typical use-case is to partition per a given time (ex: year, month or even day). 
-- MAGIC
-- MAGIC Our user table has a `creation_date` field. We'll generate a `creation_day` field based on an expression and use it as partition for our table with `GENERATED ALWAYS`.
-- MAGIC
-- MAGIC In addition, we'll let the engine generate incremental ID.
-- MAGIC
-- MAGIC *Note: Remember that partition will also create more files under the hood. You have to be careful using them. Make sure you don't over-partition your table (aim for 100's of partition max, having at least 1GB of data). We don't recommend creating partition on table smaller than 1TB. Use LIQUID CLUSTERING instead.*

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_delta_partition (
  id BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 10000 INCREMENT BY 1 ), 
  firstname STRING, 
  lastname STRING, 
  email STRING, 
  address STRING, 
  gender INT, 
  age_group INT,
  creation_date timestamp, 
  creation_day date GENERATED ALWAYS AS ( CAST(creation_date AS DATE) ) )
PARTITIONED BY (creation_day);

-- COMMAND ----------

-- Note that we don't insert data for the creation_day field or id. The engine will handle that for us:
INSERT INTO user_delta_partition (firstname, lastname, email, address, gender, age_group, creation_date) SELECT
  firstname,
  lastname,
  email,
  address,
  gender,
  age_group,
  creation_date
FROM user_delta;

-- COMMAND ----------

SELECT * FROM user_delta_partition where creation_day = CAST(NOW() as DATE) ;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC That's it! You know how to have super fast queries on top of your Delta Lake tables!
-- MAGIC
-- MAGIC
-- MAGIC Next: Discover how Delta Lake is an Universal Format with [the 03-Delta-Lake-Uniform notebook]($./03-Delta-Lake-Uniform) or go back to [00-Delta-Lake-Introduction]($./00-Delta-Lake-Introduction).
-- MAGIC
