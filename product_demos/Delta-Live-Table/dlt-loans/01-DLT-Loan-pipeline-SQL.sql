-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify ETL with Delta Live Table
-- MAGIC
-- MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
-- MAGIC
-- MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-1.png" width="700"/>
-- MAGIC
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC
-- MAGIC ## Our Delta Live Table pipeline
-- MAGIC
-- MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
-- MAGIC
-- MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
-- MAGIC
-- MAGIC **Your DLT Pipeline is ready!** Your pipeline was started using this notebook and is <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/460f840c-9ecc-4d19-a661-f60fd3a88297">available here</a>.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=01-DLT-Loan-pipeline-SQL&demo_name=dlt-loans&event=VIEW">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC Our datasets are coming from 3 different systems and saved under a cloud storage folder (S3/ADLS/GCS): 
-- MAGIC
-- MAGIC * `loans/raw_transactions` (loans uploader here in every few minutes)
-- MAGIC * `loans/ref_accounting_treatment` (reference table, mostly static)
-- MAGIC * `loans/historical_loans` (loan from legacy system, new data added every week)
-- MAGIC
-- MAGIC Let's ingest this data incrementally, and then compute a couple of aggregates that we'll need for our final Dashboard to report our KPI.

-- COMMAND ----------

-- DBTITLE 1,Let's review the incoming data
-- %fs ls /Volumes/main__build/dbdemos_dlt_loan/raw_data/raw_transactions

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-2.png" width="600"/>
-- MAGIC
-- MAGIC Our raw data is being sent to a blob storage. 
-- MAGIC
-- MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
-- MAGIC
-- MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
-- MAGIC
-- MAGIC For more detail on Autoloader, you can see `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC #### STREAMING LIVE TABLE 
-- MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. Without `STREAMING`, you will scan and ingest all the data available at once. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) for more details

-- COMMAND ----------

-- DBTITLE 1,Capture new incoming transactions
CREATE STREAMING TABLE raw_txs
  COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
AS SELECT * FROM cloud_files('/Volumes/main__build/dbdemos_dlt_loan/raw_data/raw_transactions', 'json', map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,Reference table - metadata (small & almost static)
CREATE LIVE TABLE ref_accounting_treatment
  COMMENT "Lookup mapping for accounting codes"
AS SELECT * FROM delta.`/Volumes/main__build/dbdemos_dlt_loan/raw_data/ref_accounting_treatment`

-- COMMAND ----------

-- DBTITLE 1,Historical transaction from legacy system
-- as this is only refreshed at a weekly basis, we can lower the interval
CREATE STREAMING TABLE raw_historical_loans
  TBLPROPERTIES ("pipelines.trigger.interval"="6 hour")
  COMMENT "Raw historical transactions"
AS SELECT * FROM cloud_files('/Volumes/main__build/dbdemos_dlt_loan/raw_data/historical_loans', 'csv', map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-3.png" width="600"/>
-- MAGIC
-- MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
-- MAGIC
-- MAGIC To consume only increment from the Bronze layer like `BZ_raw_txs`, we'll be using the `stream` keyworkd: `stream(LIVE.BZ_raw_txs)`
-- MAGIC
-- MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
-- MAGIC
-- MAGIC #### Expectations
-- MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

-- COMMAND ----------

-- DBTITLE 1,enrich transactions with metadata
CREATE STREAMING LIVE VIEW new_txs 
  COMMENT "Livestream of new transactions"
AS SELECT txs.*, ref.accounting_treatment as accounting_treatment FROM stream(LIVE.raw_txs) txs
  INNER JOIN live.ref_accounting_treatment ref ON txs.accounting_treatment_id = ref.id

-- COMMAND ----------

-- DBTITLE 1,Keep only the proper transactions. Fail if cost center isn't correct, discard the others.
CREATE STREAMING TABLE cleaned_new_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
  COMMENT "Livestream of new transactions, cleaned and compliant"
AS SELECT * from STREAM(live.new_txs)

-- COMMAND ----------

-- DBTITLE 1,Let's quarantine the bad transaction for further analysis
-- This is the inverse condition of the above statement to quarantine incorrect data for further analysis.
CREATE STREAMING TABLE quarantine_bad_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date <= date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance <= 0 OR arrears_balance <= 0) ON VIOLATION DROP ROW
)
  COMMENT "Incorrect transactions requiring human analysis"
AS SELECT * from STREAM(live.new_txs)

-- COMMAND ----------

-- DBTITLE 1,Enrich all historical transactions
CREATE LIVE TABLE historical_txs
  COMMENT "Historical loan transactions"
AS SELECT l.*, ref.accounting_treatment as accounting_treatment FROM LIVE.raw_historical_loans l
  INNER JOIN LIVE.ref_accounting_treatment ref ON l.accounting_treatment_id = ref.id

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC ## Gold layer
-- MAGIC
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-4.png" width="600"/>
-- MAGIC
-- MAGIC Our last step is to materialize the Gold Layer.
-- MAGIC
-- MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per cost location
CREATE LIVE TABLE total_loan_balances
  COMMENT "Combines historical and new loan data for unified rollup of loan balances"
  TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols" = "location_code")
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.historical_txs  GROUP BY addr_state
  UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per cost center
CREATE LIVE TABLE new_loan_balances_by_cost_center
  COMMENT "Live table of new loan balances for consumption by different cost centers"
AS SELECT sum(balance) as sum_balance, cost_center_code FROM live.cleaned_new_txs
  GROUP BY cost_center_code

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per country
CREATE LIVE TABLE new_loan_balances_by_country
  COMMENT "Live table of new loan balances per country"
AS SELECT sum(count) as sum_count, country_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

-- MAGIC %md ## Next steps
-- MAGIC
-- MAGIC Your DLT pipeline is ready to be started. <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/460f840c-9ecc-4d19-a661-f60fd3a88297">Click here to access the pipeline</a> created for you using this notebook.
-- MAGIC
-- MAGIC To create a new one, Open the DLT menu, create a pipeline and select this notebook to run it. To generate sample data, please run the [companion notebook]($./_resources/00-Loan-Data-Generator) (make sure the path where you read and write the data are the same!)
-- MAGIC
-- MAGIC Datas Analyst can start using DBSQL to analyze data and track our Loan metrics.  Data Scientist can also access the data to start building models to predict payment default or other more advanced use-cases.

-- COMMAND ----------

-- MAGIC %md ## Tracking data quality
-- MAGIC
-- MAGIC Expectations stats are automatically available as system table.
-- MAGIC
-- MAGIC This information let you monitor your data ingestion quality. 
-- MAGIC
-- MAGIC You can leverage DBSQL to request these table and build custom alerts based on the metrics your business is tracking.
-- MAGIC
-- MAGIC
-- MAGIC See [how to access your DLT metrics]($./03-Log-Analysis)
-- MAGIC
-- MAGIC <img width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC
-- MAGIC <a dbdemos-dashboard-id="dlt-expectations" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1' target="_blank">Data Quality Dashboard example</a>
