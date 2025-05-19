# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Data Intelligence Platform for Financial Services - Serving the Underbanked population
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
# MAGIC   <div class="badge_b"><div class="badge">5</div> Build and deploy <strong>responsibly governed ML models</strong> that assess creditworthiness of underbanked customers and evaluate the risk of existing debt-holders. Ensure real-time, trustworthy decisioning for Buy Now, Pay Later-like use cases through unified model training, monitoring, and governance—promoting fairness, transparency, and compliance across the AI lifecycle.</div>
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

# MAGIC %md
# MAGIC ## 3: Responsible AI with the Databricks Data Intelligence Platform
# MAGIC
# MAGIC Databricks emphasizes trust in AI by enabling organizations to maintain ownership and control over their data and models. [Databricks Data Intelligence Platform](https://www.databricks.com/product/data-intelligence-platform) unifies data, model training, management, monitoring, and governance across the AI lifecycle. This approach ensures that systems are **high-quality**, **safe**, and **well-governed**, helping organizations implement responsible AI practices effectively while fostering trust in intelligent applications.</br>
# MAGIC
# MAGIC To accomplish these objectives responsibly, we’ll construct an end-to-end solution on the Databricks Data Intelligence platform, with a strong focus on transparency, fairness, and governance at each step. Specifically, we will:
# MAGIC
# MAGIC - Run exploratory analysis to identify anomalies, biases, and data drift early, paving the way for responsible feature engineering.
# MAGIC - Continuously update features and log transformations for full lineage and compliance.
# MAGIC - Train and evaluate models against fairness and accuracy criteria, logging artifacts for reproducibility.
# MAGIC - Validate models by performing compliance checks and pre-deployment tests, ensuring alignment with Responsible AI standards.
# MAGIC - Integrate champion and challenger models to deploy the winning solution, maintaining traceability and accountability at each decision point.
# MAGIC - Provide batch or real-time inference with robust explainability.
# MAGIC - Monitor production models to detect data drift, alert on performance decay, and trigger recalibration workflows whenever the model’s effectiveness declines.
# MAGIC
# MAGIC <center><img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/_resources/images/architecture_0.png?raw=true" 
# MAGIC      style="width: 1200px; height: auto; display: block; margin: 0;" /></center>
# MAGIC
# MAGIC With this approach, we can confidently meet regulatory and ethical benchmarks while improving organizational outcomes—demonstrating that Responsible AI is not just about checking boxes but about building trust and value across the entire ML lifecycle.
# MAGIC
# MAGIC Below is a concise overview of each notebook’s role in the Responsible AI pipeline, as depicted in the architecture diagram. Together, they illustrate how Databricks supports transparency (explainability), effectiveness (model performance and bias control), and reliability (ongoing monitoring) throughout the model lifecycle.
# MAGIC
# MAGIC * [03.1-Exploratory-Analysis]($./03-Data-Science-ML/03.1-Exploratory-Analysis-credit-decisioning): Examines data distributions and identifies biases or anomalies, providing transparency early in the lifecycle. Sets the stage for responsible feature selection and model design.
# MAGIC * [03.2-Feature-Updates]($./03-Data-Science-ML/03.2-Feature-Updates-credit-decisioning): Continuously ingests new data, refreshes features, and logs transformations, ensuring model effectiveness. Maintains transparency around feature lineage for compliance and traceability.
# MAGIC * [03.3-Model-Training]($./03-Data-Science-ML/03.3-Model-Training-credit-decisioning): Trains, evaluates, and documents candidate models, tracking performance and fairness metrics. Stores artifacts for reproducibility, aligning with responsible AI goals.
# MAGIC * [03.4-Model-Validation]($./03-Data-Science-ML/03.4-Model-Validation-credit-decisioning): Performs compliance checks, pre-deployment tests, and fairness evaluations. Verifies reliability and transparency standards before the model progresses to production.
# MAGIC * [03.5-Model-Integration]($./03-Data-Science-ML/03.5-Model-Integration-credit-decisioning): Compares champion and challenger models, enabling human oversight for final selection. Deploys the chosen pipeline responsibly, ensuring accountability at each handoff.
# MAGIC * [03.6-Model-Inference]($./03-Data-Science-ML/03.6-Model-Inference-credit-decisioning): Executes batch or real-time predictions using the deployed model. Ensures outputs remain consistent and explainable for responsible decision-making.
# MAGIC * [03.7-Model-Monitoring]($./03-Data-Science-ML/03.7-Model-Monitoring-credit-decisioning): Continuously tracks data drift, prediction stability, and performance degradation. Generates alerts for timely retraining, preserving reliability and trustworthiness across the model’s lifecycle.

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
