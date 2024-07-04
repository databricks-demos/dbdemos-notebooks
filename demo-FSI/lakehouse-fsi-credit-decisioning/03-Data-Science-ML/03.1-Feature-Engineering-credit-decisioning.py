# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ML: predict credit owners with high default probability
# MAGIC
# MAGIC Once all data is loaded and secured (the **data unification** part), we can proceed to exploring, understanding, and using the data to create actionable insights - **data decisioning**.
# MAGIC
# MAGIC
# MAGIC As outlined in the [introductory notebook]($../00-Credit-Decisioning), we will build machine learning (ML) models for driving three business outcomes:
# MAGIC 1. Identify currently underbanked customers with high credit worthiness so we can offer them credit instruments,
# MAGIC 2. Predict current credit owners with high probability of defaulting along with the loss-given default, and
# MAGIC 3. Offer instantaneous micro-loans (Buy Now, Pay Later) when a customer does not have the required credit limit or account balance to complete a transaction.
# MAGIC
# MAGIC Here is the flow we'll implement: 
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-0.png" width="1200px">
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03.1-Feature-Engineering-credit-decisioning&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## The need for Enhanced Collaboration
# MAGIC
# MAGIC Feature Engineering is an iterative process - we need to quickly generate new features, test the model, and go back to feature selection and more feature engineering - many many times. The Databricks Lakehouse enables data teams to collaborate extremely effectively through the following Databricks Notebook features:
# MAGIC 1. Sharing and collaborating in the same Notebook by any team member (with different access modes),
# MAGIC 2. Ability to use python, SQL, and R simultaneously in the same Notebook on the same data,
# MAGIC 3. Native integration with a Git repository (including AWS Code Commit, Azure DevOps, GitLabs, Github, and others), making the Notebooks tools for CI/CD,
# MAGIC 4. Variables explorer,
# MAGIC 5. Automatic Data Profiling (in the cell below), and
# MAGIC 6. GUI-based dashboards (in the cell below) that can also be added to any Databricks SQL Dashboard.
# MAGIC
# MAGIC These features enable teams within FSI organizations to become extremely fast and efficient in building the best ML model at reduced time, thereby making the most out of market opportunities such as the raising interest rates.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Data exploration & Features creation
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-1.png" style="float: right" width="800px">
# MAGIC
# MAGIC <br/><br/>
# MAGIC The first step as Data Scientist is to explore our data and understand it to create Features.
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC This where we use our existing tables and transform the data to be ready for our ML models. These features will later be stored in Databricks Feature Store (see below) and used to train the aforementioned ML models.
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC Let's start with some data exploration. Databricks comes with built-in Data Profiling to help you bootstrap that.

# COMMAND ----------

# DBTITLE 1,Use SQL to explore your data
# MAGIC %sql
# MAGIC SELECT * FROM customer_gold WHERE tenure_months BETWEEN 10 AND 150

# COMMAND ----------

# DBTITLE 1,Our any of your usual python libraries for analysis
data = spark.table("customer_gold") \
              .where("tenure_months BETWEEN 10 AND 150") \
              .groupBy("tenure_months", "education").sum("income_monthly") \
              .orderBy('education').toPandas()

px.bar(data, x="tenure_months", y="sum(income_monthly)", color="education", title="Wide-Form Input")

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

fs = feature_store.FeatureStoreClient()

# Drop the fs table if it was already existing to cleanup the demo state
drop_fs_table(f"{catalog}.{db}.credit_decisioning_features")
  
fs.create_table(
    name=f"{catalog}.{db}.credit_decisioning_features",
    primary_keys=["cust_id"],
    df=feature_df,
    description="Features for Credit Decisioning.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After creating our features and storing them in the Databricks Feature Store, we can now proceed to the [03.2-AutoML-credit-decisioning]($./03.2-AutoML-credit-decisioning) and build out credit decisioning model.
