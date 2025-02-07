# Databricks notebook source
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Building our Features for Credit Default risks
# MAGIC
# MAGIC To build our model predicting credit default risks, we'll need a buch of features. To improve our governance and centralize our data for multiple ML project, we can save our ML features using a Feature Store.

# COMMAND ----------

# DBTITLE 1,Read the customer table
customer_gold_features = (spark.table("customer_gold")
                               .withColumn('age', int(date.today().year) - col('birth_year'))
                               .select('cust_id', 'education', 'marital_status', 'months_current_address', 'months_employment', 'is_resident',
                                       'tenure_months', 'product_cnt', 'tot_rel_bal', 'revenue_tot', 'revenue_12m', 'income_annual', 'tot_assets', 
                                       'overdraft_balance_amount', 'overdraft_number', 'total_deposits_number', 'total_deposits_amount', 'total_equity_amount', 
                                       'total_UT', 'customer_revenue', 'age', 'avg_balance', 'num_accs', 'balance_usd', 'available_balance_usd')).dropDuplicates(['cust_id'])
display(customer_gold_features)

# COMMAND ----------

# DBTITLE 1,Read the telco table
telco_gold_features = (spark.table("telco_gold")
                            .select('cust_id', 'is_pre_paid', 'number_payment_delays_last12mo', 'pct_increase_annual_number_of_delays_last_3_year', 'phone_bill_amt', \
                                    'avg_phone_bill_amt_lst12mo')).dropDuplicates(['cust_id'])
display(telco_gold_features)

# COMMAND ----------

# DBTITLE 1,Adding some additional features on transactional trends
fund_trans_gold_features = spark.table("fund_trans_gold").dropDuplicates(['cust_id'])

for c in ['12m', '6m', '3m']:
  fund_trans_gold_features = fund_trans_gold_features.withColumn('tot_txn_cnt_'+c, col('sent_txn_cnt_'+c)+col('rcvd_txn_cnt_'+c))\
                                                     .withColumn('tot_txn_amt_'+c, col('sent_txn_amt_'+c)+col('rcvd_txn_amt_'+c))

fund_trans_gold_features = fund_trans_gold_features.withColumn('ratio_txn_amt_3m_12m', F.when(col('tot_txn_amt_12m')==0, 0).otherwise(col('tot_txn_amt_3m')/col('tot_txn_amt_12m')))\
                                                   .withColumn('ratio_txn_amt_6m_12m', F.when(col('tot_txn_amt_12m')==0, 0).otherwise(col('tot_txn_amt_6m')/col('tot_txn_amt_12m')))\
                                                   .na.fill(0)
display(fund_trans_gold_features)

# COMMAND ----------

# DBTITLE 1,Consolidating all the features
feature_df = customer_gold_features.join(telco_gold_features.alias('telco'), "cust_id", how="left")
feature_df = feature_df.join(fund_trans_gold_features, "cust_id", how="left")
display(feature_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Databricks Feature Store
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="650" />
# MAGIC
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. 
# MAGIC
# MAGIC Under the hood, feature store are backed by a Delta Lake table. This will allow discoverability and reusability of our feature across our organization, increasing team efficiency.
# MAGIC
# MAGIC
# MAGIC Databricks Feature Store brings advanced capabilities to accelerate and simplify your ML journey, such as point in time support and online-store, fetching your features within ms for real time Serving. 
# MAGIC
# MAGIC ### Why use Databricks Feature Store?
# MAGIC
# MAGIC Databricks Feature Store is fully integrated with other components of Databricks.
# MAGIC
# MAGIC * **Discoverability**. The Feature Store UI, accessible from the Databricks workspace, lets you browse and search for existing features.
# MAGIC
# MAGIC * **Lineage**. When you create a feature table with Feature Store, the data sources used to create the feature table are saved and accessible. For each feature in a feature table, you can also access the models, notebooks, jobs, and endpoints that use the feature.
# MAGIC
# MAGIC * **Batch and Online feature lookup for real time serving**. When you use features from Feature Store to train a model, the model is packaged with feature metadata. When you use the model for batch scoring or online inference, it automatically retrieves features from Feature Store. The caller does not need to know about them or include logic to look up or join features to score new data. This makes model deployment and updates much easier.
# MAGIC
# MAGIC * **Point-in-time lookups**. Feature Store supports time series and event-based use cases that require point-in-time correctness.
# MAGIC
# MAGIC
# MAGIC For more details about Databricks Feature Store, run `dbdemos.install('feature-store')`

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Drop the fs table if it was already existing to cleanup the demo state
fe.drop_table(name=f"{catalog}.{db}.credit_decisioning_features")

# Create feature table with `cust_id` as the primary key.
fe.create_table(
    name=f"{catalog}.{db}.credit_decisioning_features",
    primary_keys="cust_id",
    schema=feature_df.schema,
    description="Features for Credit Decisioning.")

fe.write_table(
  name=f"{catalog}.{db}.credit_decisioning_features",
  df = feature_df,
  mode = 'merge'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transparency
# MAGIC
# MAGIC Databricks Unity Catalog's **automated data lineage** feature tracks and visualizes data flow from ingestion to consumption. It ensures transparency in feature engineering for machine learning by capturing transformations, dependencies, and usage across notebooks, workflows, and dashboards. This enhances reproducibility, debugging, compliance, and model accuracy.
# MAGIC
# MAGIC Accessing lineage via [system tables](https://docs.databricks.com/en/admin/system-tables/lineage.html) allows users to query lineage metadata, track data dependencies, and analyze usage patterns. This structured approach aids auditing, debugging, and optimizing workflows, ensuring data quality across analytics and ML pipelines.
# MAGIC
# MAGIC Next, we'll import lineage data for the feature tables created in this Notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from system.access.table_lineage
# MAGIC where target_table_catalog=current_catalog() 
# MAGIC and target_table_schema=current_schema() 
# MAGIC and target_table_name = 'credit_decisioning_features';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation of the data 
# MAGIC
# MAGIC In Databricks, the AI-generated column and table description feature leverages machine learning models to automatically analyze and generate meaningful metadata descriptions for tables and columns within a dataset. This functionality improves data discovery and understanding by providing natural language descriptions, helping users quickly interpret data without needing to manually write documentation. The AI can identify patterns, data types, and relationships between columns, offering suggestions that can enhance data governance, streamline collaboration, and make datasets more accessible, especially for those unfamiliar with the underlying schema. This feature is part of Databricks' broader effort to simplify data exploration and enhance productivity within its unified data analytics platform.

# COMMAND ----------

table_name = "credit_decisioning_features"

table_description = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").filter("col_name = 'Comment'").select("data_type").collect()[0][0]

column_descriptions = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").filter("col_name != ''").select("col_name", "comment").collect()
