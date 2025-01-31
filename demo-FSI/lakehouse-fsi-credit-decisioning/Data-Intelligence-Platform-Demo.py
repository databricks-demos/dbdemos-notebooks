# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Data Intelligence Platform for Financial Services - Serving the Underbanked population with the Databricks Lakehouse
# MAGIC <br />
# MAGIC <img src="https://raw.githubusercontent.com/borisbanushev/CAPM_Databricks/main/lakehouseDAIWTusecases.jpg" style="float: left; margin-right: 30px" width="650px" />
# MAGIC
# MAGIC ## What is The Databricks Data Intelligence Platform for Financial Services?
# MAGIC
# MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, on any workload to always offer more engaging customer experiences driven by real-time data, at the lowest cost. The Data Intelligence Platform is built on the following three pillars:
# MAGIC
# MAGIC ### 1. Simple
# MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
# MAGIC
# MAGIC ### 2. Open
# MAGIC   Built on open source and open standards. You own your data and prevent vendor lock-in, with easy integration with external solution. Being open also lets you share your data with any external organization, regardless of their data stack/vendor.
# MAGIC
# MAGIC ### 3. Multicloud
# MAGIC   Multicloud is one of the key strategic considerations for FS companies, 88% of single-cloud FS customers are adopting a multi-cloud architecture (<a href="https://www.fstech.co.uk/fst/88_Of_FIs_Considering_Multicloud.php">reference</a>). Multicloud architecture, however can be expensive and difficult to build. The Lakehouse provides one consistent data platform across clouds and gives companies the ability to process your data where your need.
# MAGIC  
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=00-Credit-Decisioning&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Raising Interest Rates - an opportunity or a threat?
# MAGIC
# MAGIC The current raising interest rates can be both a great opportunity for retail banks and other FS organizations to increase their revenues from their credit instruments (such as loans, mortgages, and credit cards) but also a risk for larger losses as customers might end up unable to repay credits with higher rates. In the current market conditions, FS companies need better credit scoring and decisioning models and approaches. Such models, however, are very difficult to achieve as financial information might be insufficient or siloed. <br />
# MAGIC #### <p><strong>In its essence, good credit decisioning is a massive data curation exercise.</strong></p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## DEMO: upsell your underbanked customers and reduce your risk through better credit scoring models
# MAGIC
# MAGIC In this demo, we'll step in the shoes of a retail bank trying to utilize the current interest rates hike and enhance it's bottom line performance (both increasing revenue and decreasing costs).
# MAGIC
# MAGIC The business has determined that the bank's focus is at improving the credit evaluation of current and potential credit customers. We're asked to:
# MAGIC
# MAGIC * Identify customers who do not currently have a credit instrument but would have a good credit score (upselling the underbanked clients to increase revenue),
# MAGIC * Evaluate the possibility of default of the bank's current credit holders (reducing risk thereby reducing loss).
# MAGIC
# MAGIC
# MAGIC ### What we will build
# MAGIC
# MAGIC To do so, we'll build an end-to-end solution with the Lakehouse. To be able to properly analyze and predict creditworthiness, we need information coming from different external and internal systems: credit bureau data, internal banking data (such as accounts, KYC, collections, applications, and relationship), real-time fund and credit card transactions, as well as partner data (in this case, telecom data) to enhance our internal information.
# MAGIC
# MAGIC At a high level, we will implement the following flow:
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>
# MAGIC
# MAGIC
# MAGIC <div style="margin-left: 20px">
# MAGIC   <div class="badge_b"><div class="badge">1</div> Ingest all the various sources of data and create our credit decisioning database (<strong>unification of data</strong>) </div>
# MAGIC   <div class="badge_b"><div class="badge">2</div>  Secure data and grant read access to the Data Analyst and Data Science teams, including row- and column-level filtering, PII data masking, and others (<strong>data security and control</strong>)</div>
# MAGIC   <div class="badge_b"><div class="badge">3</div> Use the Databricks unified <strong>data lineage</strong> to understand how your data flows and is used in your organisation</div>
# MAGIC   <div class="badge_b"><div class="badge">4</div> Run BI  queries and EDA to analyze existing credit risk</div>
# MAGIC   <div class="badge_b"><div class="badge">5</div>  Build ML model to <strong>predict credit worthiness</strong> of underbanked customers, evaluate the risk of current debt-holders, and deploy ML models for real-time serving in order to enable Buy Now, Pay Later use cases</div>
# MAGIC   <div class="badge_b"><div class="badge">6</div> <strong>Visualise your business</strong> models along with all actionable insights coming from machine learning</div>
# MAGIC   <div class="badge_b"><div class="badge">7</div>Provide an easy and simple way to securely share these insights to non-data users, such as bank tellers, call center agents, or credit agents (<strong>data democratization</strong>)</div>
# MAGIC </div>
# MAGIC <br/><br/>
# MAGIC <img width="1250px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_flow.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Setting the Foundations in Financial Services
# MAGIC
# MAGIC Governance is top of mind for every financial services institution today. This focus is largely due to multi-cloud trends, proliferation of sensitive data from digital data sources, and the explosion of data products and data sharing needs. Our governance solution provides 3 key capabilities that we will focus on:
# MAGIC
# MAGIC <p></p>
# MAGIC
# MAGIC 1. **Data Security**: Databricks has [Enhanced Security and Compliance](https://www.databricks.com/trust/protect-your-data-with-enhanced-security-and-compliance) to help any FSI monitor and keep their platform secure. Databricks also has built-in encryption functions, customer-managed keys, and Private Link across clouds to provide advanced security expected by FSIs.
# MAGIC 2. **Lineage and Discoverability**: When handling sensitive data, understanding lineage for audit and compliance is critical. Discoverability means any downstream consumer has insight into trusted data.
# MAGIC 3. **Sharing data with non-data users**: Data Sharing has arisen from needs to access data without copying vast amounts of data. As the Financial Services industry moves toward multi-cloud and develops products in open banking, consumer lending and fraud, sharing data with open standards provides the right foundation to avoid cloud lock-in and help FSIs democratize data.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1: Ingesting data & building our FS Lakehouse Database
# MAGIC
# MAGIC First, we need to ingest and transform customer, credit, and payments data sources with the [Delta Live Table SQL Pipeline notebook]($./01-Data-Ingestion/01-DLT-Internal-Banking-Data-SQL). This will create a <a href="/" dbdemos-pipeline-id="dlt-fsi-credit-decisioning" target="_blank">DLT Pipeline</a> running in batch or streaming, and saving our data within Unity Catalog as Delta tables.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2: Governance
# MAGIC
# MAGIC Now that our data has been ingested, we can explore the catalogs and schemas created using the [Data Explorer](/explore/data/dbdemos/fsi_credit_decisioning). 
# MAGIC
# MAGIC To leverage our data assets across the entire organization, we need:
# MAGIC
# MAGIC * Fine grained ACLs for our Analysts & Data Scientists teams
# MAGIC * Lineage between all our data assets
# MAGIC * real-time PII data encryption 
# MAGIC * Audit logs
# MAGIC * Data Sharing with external organization 
# MAGIC
# MAGIC Discover how to do that with the [02-Data-Governance-credit-decisioning]($./02-Data-Governance/02-Data-Governance-credit-decisioning) notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3: ML - Building a credit scoring model to predict payment defaults, reduce loss, and upsell
# MAGIC
# MAGIC Now that our data is ready and secured, let's create a model to predict the risk and potential default of current creditholders and potential new customers.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-ml-usecase.png" width="800px" style="float: right">
# MAGIC
# MAGIC To do that, we'll leverage the data previously ingested, analyze, and save the features set within Databricks feature store.
# MAGIC
# MAGIC Databricks AutoML will then accelerate our ML journey by creating state of the art Notebooks that we'll use to deploy our model in production within MLFlow Model registry.
# MAGIC
# MAGIC
# MAGIC Once our is model ready, we'll leverage it to:
# MAGIC
# MAGIC <div style="padding-left: 20px;">
# MAGIC
# MAGIC   <h4> Score, analyze, and target our existing customer database </h4>
# MAGIC
# MAGIC   Once our model is created and deployed in production within Databricks Model registry, we can use it to run batch inference.
# MAGIC
# MAGIC   The model outcome will be available for our Analysts to implement new use cases, such as upsell or risk exposure analysis (see below for more details).
# MAGIC
# MAGIC   <h4>   Deploy our model for Real time model serving, allowing Buy Now, Pay Later (BNPL)</h4>
# MAGIC
# MAGIC   Leveraging Databricks Lakehouse, we can deploy our model to provide real-time serving over REST API. <br>
# MAGIC   This will allow us to give instant results on credit requests, allowing customers to automatically open a new credit while reducing payment default risks. 
# MAGIC
# MAGIC
# MAGIC   <h4> Ensure model Explainability and Fairness</h4>
# MAGIC
# MAGIC   An important part of every regulated industry use case is the ability to explain the decisions taken through data and AI; and to also be able to evaluate the fairness of the model and identify whether it disadvantages certain people or groups of people.
# MAGIC
# MAGIC   The demo shows how to add explainability and fairness to the final dashboard:
# MAGIC
# MAGIC   1. Feature importance and SHAP ratios charts are added to provide an overall understanding as to what are the drivers and root causes behind credit worthiness and defaults, so the bank can take appropriate measures,
# MAGIC   2. Detailed fairness like gender fairness score and a breakdown of different demographic features, such as education, marital status, and residency.
# MAGIC   
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Machine Learning next steps:
# MAGIC
# MAGIC * [03.1-Feature-Engineering-credit-decisioning]($./03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning): Open the first notebook to analyze our data and start building our model leveraging Databricks Feature Store and AutoML.
# MAGIC * [03.2-AutoML-credit-decisioning]($./03-Data-Science-ML/03.2-AutoML-credit-decisioning): Leverage AutoML to accelerate your model creation.
# MAGIC * [03.3-Batch-Scoring-credit-decisioning]($./03-Data-Science-ML/03.3-Batch-Scoring-credit-decisioning): score our entire dataset and save the result as a new delta table for downstream usage.
# MAGIC * [03.4-model-serving-BNPL-credit-decisioning]($./03-Data-Science-ML/03.4-model-serving-BNPL-credit-decisioning): leverage Databricks Serverless model serving to deploy a Buy Now Pay Later offers (including AB testing).
# MAGIC * [03.5-Explainability-and-Fairness-credit-decisioning]($./03-Data-Science-ML/03.5-Explainability-and-Fairness-credit-decisioning): Explain your model and review fairness.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### 4: Building SQL Dashboard Leveraging our model to deliver real business value
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi-credit-decisioning-dashboard.png" width="800px" style="float: right">
# MAGIC
# MAGIC #### Target customers without credit and suggest new offers to the less exposed
# MAGIC
# MAGIC
# MAGIC Using the model we built, we can score current and new customers to offer credit instruments (such as credit cards, loans, and mortgages). 
# MAGIC
# MAGIC We'll use our model outcome to only upsell customer filter our customers and build a dashboard including all this information:
# MAGIC 1. Evaluates the credit worthiness of individuals without financial history,
# MAGIC 2. Estimates the ROI and revenue from upselling current customers who do not have credit instruments,
# MAGIC 3. Provides explainability on an individual level, making it easier to create targeted offers to each customer.
# MAGIC
# MAGIC Such dashboards can be very easily built in the Databricks Lakehouse and used by marketing teams and credit departments to contact upsell prospects (including a detailed reason for creditworthiness).
# MAGIC
# MAGIC
# MAGIC #### Score our existing credit database to analyze our customers
# MAGIC
# MAGIC 1. Predicts the current credit owners likely to default on their credit card or loan and the "Loss given default",
# MAGIC 2. Gives an overview of the current risk profile, the number of defaulted customers, and the overall loss,
# MAGIC 3. A breakdown of loss per different credit instruments, such as credit cards, loans, and mortgages.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open the [04-BI-Data-Warehousing-credit-decisioning]($./04-BI-Data-Warehousing/04-BI-Data-Warehousing-credit-decisioning) notebook to know more about Databricks SQL Warehouse.
# MAGIC
# MAGIC Directly open the final <a dbdemos-dashboard-id="credit-decisioning" href='/sql/dashboardsv3/01ef3a3dcb7216b29bf2e778d084fa7c' target="_blank">dashboard</a> depicting the business outcomes of building the credit decisioning on the Databricks Lakehouse, including:
# MAGIC - Upselling and serving the underbanked customers,
# MAGIC - Reducing risk and predicting default and potential losses,
# MAGIC - Turning the data into actionable and explainable insights.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5: Workflow orchestration
# MAGIC
# MAGIC Now that all our lakehouse pipeline is working, let's review how we can leverage Databricks Workflows to orchestrate our tasks and link them together: [05-Workflow-Orchestration-credit-decisioning]($./05-Workflow-Orchestration/05-Workflow-Orchestration-credit-decisioning).
