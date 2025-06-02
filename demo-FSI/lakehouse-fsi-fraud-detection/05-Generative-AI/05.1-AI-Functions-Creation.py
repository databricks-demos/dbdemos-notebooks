# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 🔍 Gen AI-Powered Fraud Detection in Modern Banking
# MAGIC Banks today face unprecedented fraud risks as criminals use advanced digital tactics to exploit vulnerabilities in online banking. Manual reviews are no longer enough-they’re slow, inconsistent, and often miss complex fraud schemes. As fraudsters leverage technology to scale their attacks, banks must adopt AI-powered solutions that analyze massive data in real time, quickly detect suspicious activity, and reduce false positives. Modern fraud detection now requires the speed, accuracy, and adaptability that only GenAI and machine learning can deliver.
# MAGIC
# MAGIC
# MAGIC <div style="background: #f7f7f7; border-left: 5px solid #ff5f46; padding: 20px; margin: 20px 0; font-size: 18px;">
# MAGIC   "Fraud scams and bank fraud schemes resulted in <b>$485.6 billion</b> in losses globally last year, with criminals using advanced techniques to evade traditional controls." - 
# MAGIC   <a href="https://www.nasdaq.com/global-financial-crime-report" target="_blank">Nasdaq 2024 Global Financial Crime Report</a>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #f7f7f7; border-left: 5px solid green; padding: 20px; margin: 20px 0; font-size: 18px;">
# MAGIC   "Studies have shown that AI-driven fraud detection can reduce false positives by up to <b>90%</b> compared to traditional methods." - 
# MAGIC   <a href="https://quantumzeitgeist.com/ai-driven-fraud-detection-in-banking-and-financial-services/" target="_blank">Quantum Zeitgeist: AI-driven Fraud Detection In Banking</a>
# MAGIC </div>
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" style="width: 100px; vertical-align: middle; margin-right: 10px; float: left;" />
# MAGIC <br>
# MAGIC Liza, a Data Engineer, is using Databricks and generative AI to modernize fraud detection. Her workflow:
# MAGIC
# MAGIC <div style="background: #e0f7fa; border-left: 5px solid #00796b; padding-left: 10px; margin: 0 0; margin-left: 120px">
# MAGIC   <ol style="text-align: left;">
# MAGIC     <li>Fetch customer details for flagged transactions with AI functions</li>
# MAGIC     <li>Generate structured fraud reports using generative AI</li>
# MAGIC     <li>Log all AI functions and outputs in Unity Catalog for compliance</li>
# MAGIC     <li>Apply these AI functions in batch to analyze all potentially fraudulent transactions</li>
# MAGIC   </ol>
# MAGIC </div>
# MAGIC
# MAGIC This demo shows how banks can use Databricks and AI to automate fraud detection, improve accuracy, and streamline compliance.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05.1-AI-Functions-Creation&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## The Demo: Automate Suspicious Transaction Fraud Reports with Databricks GenAI
# MAGIC
# MAGIC Databricks Generative AI is transforming banking fraud detection by enabling organizations to automate manual workflows, quickly identify suspicious activity, and unlock insights from vast amounts of transaction data. Where traditional fraud review can be slow and resource-intensive, Databricks empowers banks to act faster and more accurately.
# MAGIC
# MAGIC In this demo, we showcase a practical use case for Databricks to improve fraud management in modern banking. We will create an AI agent using the following functions, defined in the subsequent cells:
# MAGIC
# MAGIC 1. **Customer Information Retriever**: This function allows our GenAI agent to fetch detailed customer information for any given transaction, pulling structured and unstructured data from our Databricks Intelligence Platform.
# MAGIC
# MAGIC 2. **Fraud Report Generator**: Using generative AI models, this function creates clear, structured fraud reports for each suspicious transaction, highlighting risk factors, behavioral anomalies, and relevant customer context for further review.
# MAGIC
# MAGIC Finally, we will demonstrate how these functions can be applied in batch to analyze all potentially fraudulent transactions. By automating fraud report generation and enabling real-time access to enriched data, banks can improve operational efficiency, accelerate investigations, and enhance compliance.
# MAGIC
# MAGIC By leveraging Generative AI, banks can move beyond manual processes to **streamline workflows, reduce costs, and significantly strengthen fraud detection capabilities**.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📊 Function 1: Get Customer Details
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;"> <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" style="width: 100px; border-radius: 10px;" /> <div> Liza's first step in automating fraud detection is to retrieve all relevant information for a given customer or transaction.<br> This function, <code>get_customer_details</code>, takes in a customer or transaction ID and returns the necessary data to generate a comprehensive fraud report.<br> The retrieved data includes customer profile information, account activity, transaction history, device and location metadata, and any related documents-providing the foundation for accurate and efficient fraud analysis. </div> </div>
# MAGIC
# MAGIC ### 🛠️ Next Step
# MAGIC In the following section, we define the <code>get_customer_details</code> function, which retrieves structured and unstructured data from the Databricks Lakehouse to enable automated fraud report generation.

# COMMAND ----------

# DBTITLE 1,Retrieve Customer Details by Transaction ID
# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS get_customer_details;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION get_customer_details (
# MAGIC   tran_id STRING COMMENT 'Transaction ID of the customer to be searched'
# MAGIC ) 
# MAGIC RETURNS TABLE(
# MAGIC   id STRING, 
# MAGIC   is_fraud BOOLEAN, 
# MAGIC   amount DOUBLE, 
# MAGIC   customer_id STRING, 
# MAGIC   nameDest STRING,
# MAGIC   nameOrig STRING, 
# MAGIC   type STRING, 
# MAGIC   firstname STRING, 
# MAGIC   lastname STRING, 
# MAGIC   email STRING, 
# MAGIC   address STRING, 
# MAGIC   country STRING,
# MAGIC   creation_date STRING, 
# MAGIC   age_group DOUBLE, 
# MAGIC   countryOrig_name STRING,
# MAGIC   countryDest_name STRING
# MAGIC )
# MAGIC COMMENT "This function returns the customer details for a given Transaction ID, along with their transaction details" 
# MAGIC RETURN (
# MAGIC   SELECT
# MAGIC     id, 
# MAGIC     is_fraud, 
# MAGIC     amount, 
# MAGIC     customer_id,  
# MAGIC     nameDest, 
# MAGIC     nameOrig, 
# MAGIC     type, 
# MAGIC     firstname, 
# MAGIC     lastname, 
# MAGIC     email, 
# MAGIC     address, 
# MAGIC     country, 
# MAGIC     creation_date, 
# MAGIC     age_group,
# MAGIC     countryOrig_name, 
# MAGIC     countryDest_name
# MAGIC   FROM 
# MAGIC     gold_transactions
# MAGIC   WHERE
# MAGIC     id = tran_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC Let's ensure that our new tool works. Here we look up a transaction that our AI system (_Section 4 of this demo_) knows is fraudulent:

# COMMAND ----------

# DBTITLE 1,Retrieve Customer Info Using Unique ID
# MAGIC %sql
# MAGIC SELECT * FROM get_customer_details(
# MAGIC     '001df0ff-a9a6-4b94-a548-bfb6d5393698'
# MAGIC ) AS prediction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC And now, let's look at a transaction that we know is not fraudulent!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM get_customer_details(
# MAGIC     '3d1dd327-0120-495e-bb37-34008d7587a9'
# MAGIC ) AS prediction

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 📝 Function 2: Generate Fraud Report
# MAGIC <div style="display: flex; align-items: center; gap: 15px;"> <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" style="width: 100px; border-radius: 10px;" /> <div> After gathering customer and transaction details, Liza’s next step is to automate the creation of professional fraud reports.<br> The <code>fraud_report_generator</code> function leverages generative AI to analyze transaction risk and produce clear, actionable documentation for both internal teams and customers.<br> This function: <ul> <li>Predicts whether a transaction is fraudulent using advanced AI models.</li> <li>Automatically generates a formal internal fraud report for the bank, summarizing key transaction details and investigation findings.</li> <li>Prepares a customer notification email with clear guidance and recommendations, personalized for the customer’s language and location when needed.</li> <li>Ensures all communications follow professional standards, with proper formatting and attention to compliance requirements.</li> </ul> By automating these steps, banks can accelerate fraud response, improve communication, and ensure consistent, audit-ready documentation for every flagged transaction. </div> </div>
# MAGIC
# MAGIC ### 🛠️ Next Step
# MAGIC In the following section, we define the <code>fraud_report_generator</code> function, which will chain off of the <code>get_customer_details</code> function, in order to build an internal _and_ external fraud report both in English and in the given customer's natural language based on their demographic information.

# COMMAND ----------

# DBTITLE 1,Fraud Detection and Customer Notification Guidelines
prompt = """
You are a fraud detection assistant for banks, helping to identify and report potentially fraudulent transactions.

### Tools Available:
1. **get_customer_details** – Retrieves customer KYC profile, transaction history, and risk indicators for a given **Transaction ID**.

### Your Role:
- **If a transaction is fraudulent:**
    - **Internal Fraud Report:** Generate a professional fraud report for internal banking use, including:
        - Transaction ID, Customer ID, date/time, transaction amount, channel (e.g., online, ATM), and relevant risk indicators (e.g., velocity, geo-location anomaly, device mismatch).
        - Summary of investigation findings (e.g., pattern detected, rule triggered, behavioral anomaly).
        - Recommended actions (e.g., file SAR, freeze account, escalate to compliance).
        - Use clear, structured language and banking terminology.
    - **Customer Notification:** Draft a formal email to the customer:
        - Greet customer by first name.
        - Clearly state that suspicious activity was detected on their account.
        - Advise next steps (e.g., contact fraud team, monitor account, card reissue).
        - If the customer’s country is not English-speaking, also provide the email in their native language.
        - Use a professional, reassuring tone and proper formatting.
        - Do not include a closing.
    - Ensure all details match the provided **Transaction ID** and **Customer ID** before proceeding.

- **If the transaction is not fraudulent:**  
    - Briefly confirm that the transaction is legitimate and can be processed.

- Do not provide your reasoning step; just give the response.
"""

# COMMAND ----------

# DBTITLE 1,Create Fraud Report Function with Transaction Details
spark.sql("DROP FUNCTION IF EXISTS fraud_report_generator")

spark.sql(f"""
  CREATE OR REPLACE FUNCTION fraud_report_generator(
    id STRING, 
    is_fraud BOOLEAN, 
    amount DOUBLE, 
    customer_id STRING, 
    nameDest STRING, 
    nameOrig STRING, 
    type STRING, 
    firstname STRING, 
    lastname STRING, 
    email STRING, 
    address STRING, 
    country STRING, 
    creation_date STRING, 
    age_group DOUBLE,  
    countryOrig_name STRING, 
    countryDest_name STRING 
  )
  RETURNS STRING
  LANGUAGE SQL
  COMMENT "This function generates a fraud report based on the transaction details and customer information."
  RETURN (
    SELECT ai_query(
      'databricks-meta-llama-3-1-405b-instruct', 
      CONCAT(
        "{prompt}",
        'Transaction and customer details: ', 
        TO_JSON(NAMED_STRUCT(
          'id', id, 
          'is_fraud', is_fraud, 
          'amount', amount, 
          'customer_id', customer_id, 
          'nameDest', nameDest, 
          'nameOrig', nameOrig,  
          'type', type, 
          'firstname', firstname, 
          'lastname', lastname, 
          'email', email, 
          'address', address, 
          'country', country, 
          'creation_date', creation_date,
          'age_group', age_group, 
          'countryOrig_name', countryOrig_name, 
          'countryDest_name', countryDest_name
        ))
      )
    )
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC Let's generate a report for a fraudulent transaction.

# COMMAND ----------

# DBTITLE 1,Fraud Report and Prediction Analysis
# MAGIC %sql
# MAGIC SELECT fraud_report_generator(
# MAGIC     '001df0ff-a9a6-4b94-a548-bfb6d5393698', true,	400000,	'80a60ee7-a482-4e69-aa8f-2914f051f5b5',	'CC0290993355',	'C0047984490',	'TRANSFER',	'Angela',	'Tran',	'eburch@smith.com',	'282 Amanda Road Apt. 209 Matthewview, GU 81248' ,	'GRC',	'2021-11-05',	6,	'France',	'Nigeria'
# MAGIC   ) as transaction_report

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC Now, let's generate a report for a transaction that is not fraudulent. Unhappy with output? Modify the prompt in cell 11!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fraud_report_generator(
# MAGIC     '3d1dd327-0120-495e-bb37-34008d7587a9', false, 145273.89, '5d64d91f-3d4d-4245-898a-390004585619', 'M7423407666', 'C5687240533', 'CASH_IN', 'Suzanne', 'Nixon', 'benitezkatherine@avila.biz', '66602 Webb Land Lynnberg, UT 76294', 'AND', '2022-11-06', 1, 'Qatar', 'France'
# MAGIC ) as transaction_report

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🚀 Operation 3: Apply Our Functions in Batch to Suspicious Transactions  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     Now that Liza has implemented the <code>get_customer_details</code> and <code>fraud_report_generator</code> functions, the next step is to scale these operations by applying them in batch to a dataset of suspicious transactions.  
# MAGIC     This operation enables organizations to:
# MAGIC     <ul>
# MAGIC       <li>Process large volumes of historical or real-time transactions efficiently</li>
# MAGIC       <li>Generate detailed reports for flagged transactions based on predefined criteria (e.g., unusual amounts or behavioral anomalies)</li>
# MAGIC       <li>Democratize access to enriched transaction data for auditors and fraud investigators</li>
# MAGIC       <li>Identify patterns across multiple transactions for fraud detection and risk analysis</li>
# MAGIC     </ul>
# MAGIC     By leveraging Databricks' scalable compute capabilities, this batch processing operation ensures that even large datasets can be analyzed quickly and effectively.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Next Step  
# MAGIC In the following section, we demonstrate how to apply the <code>get_customer_details</code> and <code>fraud_report_generator</code> functions in batch mode to process suspicious transactions. This includes:
# MAGIC 1. Loading a dataset of flagged transactions from Unity Catalog.
# MAGIC 2. Iteratively applying the functions to generate reports for each transaction.
# MAGIC 3. Storing the enriched reports back into Unity Catalog for downstream analysis.
# MAGIC
# MAGIC <small>💡 Batch processing unlocks insights from historical transaction data, enabling proactive fraud detection and improved operational efficiency.</small>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fraudulent_transactions_report_table AS
# MAGIC SELECT 
# MAGIC   id AS id,
# MAGIC   is_fraud,
# MAGIC   fraud_report_generator(
# MAGIC     id, 
# MAGIC     is_fraud, 
# MAGIC     amount, 
# MAGIC     customer_id, 
# MAGIC     nameDest, 
# MAGIC     nameOrig, 
# MAGIC     type, 
# MAGIC     firstname, 
# MAGIC     lastname, 
# MAGIC     email, 
# MAGIC     address, 
# MAGIC     country, 
# MAGIC     creation_date, 
# MAGIC     age_group,  
# MAGIC     countryOrig_name, 
# MAGIC     countryDest_name
# MAGIC   ) AS claim_report
# MAGIC FROM (
# MAGIC   SELECT *
# MAGIC   FROM gold_transactions
# MAGIC   WHERE is_fraud = true
# MAGIC   LIMIT 5
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example
# MAGIC
# MAGIC Below are fraud reports for five fraudulent transactions. Want to apply the report generator to all of your data? Modify the previous table generation cell. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fraudulent_transactions_report_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to notebook **05.2-Agent-Creation-Guide** in order to package the above functions into an implementable AI Agent with Databricks!
