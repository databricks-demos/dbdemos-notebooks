# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 🔍 From Manual Reviews to AI-Powered Claims Summaries & Fraud Detection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC The financial services industry faces mounting pressure to improve claims processing efficiency while combating fraud. Manual claim summarization processes lead to inconsistent documentation, delayed fraud detection, and inefficient archival of suspicious cases—resulting in operational bottlenecks and increased financial exposure.
# MAGIC
# MAGIC <div style="background: #f7f7f7; border-left: 5px solid #ff5f46; padding: 20px; margin: 20px 0; font-size: 18px;">
# MAGIC "Industry benchmarks reveal <b>30-day average resolution times</b> for complex claims, with manual processes accounting for <b>40% of processing time</b>—far exceeding the <b>7-10 day ideal</b> for high-risk cases requiring detailed documentation."[^1]
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #f7f7f7; border-left: 5px solid green; padding: 20px; margin: 20px 0; font-size: 18px;">
# MAGIC "Generative AI reduces claims processing time by <b>50%</b> through automated summarization while improving fraud detection accuracy by <b>35%</b> through pattern recognition in historical data."[^2]
# MAGIC </div>
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" style="width: 100px; vertical-align: middle; margin-right: 10px; float: left;" />
# MAGIC <br>
# MAGIC Liza, a Generative AI Engineer, is implementing Databricks AI to transform claims management. Her system will:
# MAGIC <div style="background: #e0f7fa; border-left: 5px solid #00796b; padding-left: 10px; margin: 0 0; margin-left: 120px">
# MAGIC   <ol style="text-align: left;">
# MAGIC     <li>Retrieve all information pertaining to a certain claim id</li>
# MAGIC     <li>Automate structured claim summaries using GenAI</li>
# MAGIC     <li>Archive enriched case files to Unity Catalog for auditor review</li>
# MAGIC   </ol>
# MAGIC </div>
# MAGIC
# MAGIC <!-- [^1]: MD Clarity Industry Benchmark Report 2024, Shipping Insurance Processing Metrics  
# MAGIC [^2]: Ricoh USA Claims Automation Study 2023, Ziffity AI Implementation Case Studies  
# MAGIC [^3]: Convin AI Fraud Detection Whitepaper 2024: Anomaly Detection in Financial Documents -->
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.1-AI-Functions-Creation&demo_name=lakehouse-fsi-smart-claims&event=VIEW">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Demo: Automate Suspicious Claims Summaries with Databricks GenAI
# MAGIC <!-- 
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/retail/lakehouse-churn/lakehouse-retail-c360-agent-functions.png" 
# MAGIC      style="float: right; margin-top: 10px; width: 700px; display: block;" /> -->
# MAGIC
# MAGIC Databricks Generative AI is revolutionizing claims processing by enabling organizations to automate manual workflows, detect fraud, and unlock insights from previously siloed claims data. Where traditional claims processing can be a time-intensive endeavor, Databricks allows 
# MAGIC
# MAGIC In this demo, we showcase a practical use case for Databricks to improve claims management. We will create an AI agent using the following functions, defined in the subsequent cells:
# MAGIC
# MAGIC 1. **Claims Data Retriever**: This function allows our GenAI agent to fetch claims data based on a given `claim_id`, pulling structured and unstructured data from our Databricks Lakehouse.
# MAGIC 2. **Claims Summary Generator**: Using generative AI models, this function generates concise summaries of claims, including key details such as incident descriptions, policyholder information, and flagged anomalies for further review.
# MAGIC
# MAGIC Finally, we will demonstrate how these functions can be applied in batch processing to unlock insights from archived claims data. By automating claims summarization and enabling real-time access to enriched data, insurers can improve operational efficiency and expedite fraud detection.
# MAGIC
# MAGIC By leveraging Generative AI, insurers can move beyond manual processes to actively **streamline workflows, reduce costs, and enhance fraud detection capabilities**.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuration Step - Run This Cell!
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📊 Function 1: Get Claim Details  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     Liza's first step in automating claims processing is to retrieve all relevant information for a given claim.  
# MAGIC     This function, `get_claim_details`, takes in a claim ID and returns the necessary data to generate a comprehensive claim summary.  
# MAGIC     The retrieved data includes incident descriptions, policyholder information, claim history, and any associated documents—providing the foundation for accurate and efficient summarization.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Next Step  
# MAGIC In the following section, we define the `get_claim_details` function, which retrieves structured and unstructured data from the Databricks Lakehouse to enable automated claims summarization.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_claim_details (
# MAGIC   claim_id STRING
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   claim_no STRING,
# MAGIC   policy_no STRING,
# MAGIC   chassis_number STRING,
# MAGIC   claim_amount_total BIGINT,
# MAGIC   incident_type STRING,
# MAGIC   incident_date DATE,
# MAGIC   incident_severity STRING,
# MAGIC   driver_age DOUBLE,
# MAGIC   suspicious_activity BOOLEAN,
# MAGIC   make STRING,
# MAGIC   model STRING,
# MAGIC   model_year BIGINT,
# MAGIC   policytype STRING,
# MAGIC   sum_insured DOUBLE,
# MAGIC   borough STRING,
# MAGIC   neighborhood STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns essential claim details for the given claim ID'
# MAGIC RETURN(
# MAGIC   SELECT
# MAGIC     claim_no,
# MAGIC     policy_no,
# MAGIC     chassis_no AS chassis_number,
# MAGIC     claim_amount_total,
# MAGIC     incident_type,
# MAGIC     incident_date,
# MAGIC     incident_severity,
# MAGIC     driver_age,
# MAGIC     suspicious_activity,
# MAGIC     make,
# MAGIC     model,
# MAGIC     model_year,
# MAGIC     policytype,
# MAGIC     sum_insured,
# MAGIC     borough,
# MAGIC     neighborhood
# MAGIC   FROM claim_policy_telematics_ml
# MAGIC   WHERE claim_no = claim_id
# MAGIC   LIMIT 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC Let's ensure that our new tool works. Here we look up a claim that we know is suspicious for testing:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM get_claim_details("5627e63a-f54b-4316-8abe-a5c5c3cfc7e0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC And, let's look up a claim we know is not suspicious!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM get_claim_details("6a365ec8-4def-4b4e-910f-0a9a033aab82")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📝 Function 2: Generate Claim Summary  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     After retrieving the necessary claim details using the `get_claim_details` function, the next step is to generate a concise and structured summary of the claim.  
# MAGIC     The `generate_claim_summary` function uses Generative AI to process the retrieved data and produce a human-readable summary, which includes:
# MAGIC     <ul>
# MAGIC       <li>Incident description</li>
# MAGIC       <li>Policyholder information</li>
# MAGIC       <li>Claim amount and status</li>
# MAGIC       <li>Relevant notes or flagged items for further review</li>
# MAGIC     </ul>
# MAGIC     This summary helps streamline claims processing and provides a clear overview for auditors or claims adjusters.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Next Step  
# MAGIC In the following section, we define the `generate_claim_summary` function, which takes the output from `get_claim_details` and applies Generative AI to create structured summaries for individual claims. This enables faster decision-making and improved efficiency in claims management workflows.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Define the model prompt
# MAGIC
# MAGIC This function will utilize Perplexity by-default for creating customized claims summaries.
# MAGIC
# MAGIC Feel free to manipulate and change the below prompt in order to generate whatever kind of claims summary you think would be most impactful!

# COMMAND ----------

prompt = """
You are an expert insurance claim analyst with 25 years of experience in the auto insurance industry. Your task is to generate a comprehensive, professional claim summary report.

## Output Format:
Create a well-structured report with these clearly labeled sections:
1. **CLAIM SUMMARY**: A concise 3-4 sentence overview of the incident
2. **CLAIM INFORMATION**: Policy details and financial impact
3. **INCIDENT DETAILS**: Date, location, type, and severity
4. **VEHICLE INFORMATION**: Make, model, and specifications
5. **RISK ASSESSMENT**: Analysis of any suspicious activity and next steps

## Guidelines:
- Maintain professional, formal language suitable for insurance industry standards
- Be precise with facts, dates, and monetary values
- If suspicious activity is flagged, note potential investigation requirements
- The tone should be objective and authoritative
- Total report length should be approximately 250-300 words
- Format monetary values appropriately (e.g., $10,000.00)
- Use insurance industry terminology where appropriate

Analyze the following claim data and provide your expert assessment:
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Function definition
# MAGIC
# MAGIC Next, like before, we will utilize `ai_query` to define our function, leveraging the prompt from our previous cell as well as the previous two functions. 

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION generate_claim_summary(
    claim_no STRING,
    policy_no STRING,
    chassis_number STRING,
    claim_amount_total STRING,
    incident_type STRING,
    incident_date STRING,
    incident_severity STRING,
    driver_age STRING,
    suspicious_activity STRING,
    make STRING,
    model STRING,
    model_year STRING,
    policytype STRING,
    sum_insured STRING,
    borough STRING,
    neighborhood STRING
  )
  RETURNS STRING
  LANGUAGE SQL
  COMMENT "Generates a professional insurance claim summary using AI based on the provided claim details. This function produces a well-structured report with sections for Claim Information, Incident Details, Vehicle Information, and Policy Details."
  RETURN (
    SELECT ai_query(
      'databricks-mixtral-8x7b-instruct',
      CONCAT(
        "{prompt}"
        'Claim details: ', 
        TO_JSON(NAMED_STRUCT('claim_no', claim_no, 'policy_no', policy_no, 'chassis_number', chassis_number, 'claim_amount_total', claim_amount_total, 'incident_type', incident_type, 'incident_date', incident_date, 'incident_severity', incident_severity, 'driver_age', driver_age, 'suspicious_activity', suspicious_activity, 'make', make, 'model', model, 'model_year', model_year, 'policytype', policytype, 'sum_insured', sum_insured, 'borough', borough, 'neighborhood', neighborhood))
      )
    )
  )
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Step 3a: Suspicious Claim Example:
# MAGIC Now, let's make sure that this works. Let's manually input claims data pertaining to a claim we know is suspicious, and see how the summary looks! Feel free to modify the prompt above if you prefer a different output!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT generate_claim_summary("5627e63a-f54b-4316-8abe-a5c5c3cfc7e0", "102085057", "JN1CC11C37T012187", "61500", "Single Vehicle Collision", "2016-04-20", "Minor Damage", "47", "true", "NISSAN", "TIIDA", "2007", "COMP", "10000", "QUEENS", "WEST CENTRAL QUEENS") AS claim_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Step 3b: Non-Suspicious Claim Example:
# MAGIC Now, let's make sure that this works. Let's manually input claims data pertaining to a claim we know is suspicious, and see how the summary looks! Feel free to modify the prompt above if you prefer a different output!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT generate_claim_summary("6a365ec8-4def-4b4e-910f-0a9a033aab82", "102085055", "VWPW11K78W108679", "59700", "Multi-vehicle Collision", "2016-10-20", "Major Damage", "35", "true", "VOLKSWAGEN", "GOLF", "2008", "COMP", "30000", "MANHATTAN", "LOWER MANHATTAN") AS claim_summary

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🚀 Operation 3: Apply Our Functions in Batch to Suspicious Claims  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     Now that Liza has implemented the `get_claim_details` and `generate_claim_summary` functions, the next step is to scale these operations by applying them in batch to a dataset of suspicious claims.  
# MAGIC     This operation enables organizations to:
# MAGIC     <ul>
# MAGIC       <li>Process large volumes of archived claims efficiently</li>
# MAGIC       <li>Generate summaries for flagged claims based on predefined criteria (e.g., high claim amounts or anomalies)</li>
# MAGIC       <li>Democratize access to enriched claim data for auditors and fraud investigators</li>
# MAGIC       <li>Identify patterns across multiple claims for fraud detection and risk analysis</li>
# MAGIC     </ul>
# MAGIC     By leveraging Databricks' scalable compute capabilities, this batch processing operation ensures that even large datasets can be analyzed quickly and effectively.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Next Step  
# MAGIC In the following section, we demonstrate how to apply the `get_claim_details` and `generate_claim_summary` functions in batch mode to process suspicious claims. This includes:
# MAGIC 1. Loading a dataset of flagged claims from Unity Catalog.
# MAGIC 2. Iteratively applying the functions to generate summaries for each claim.
# MAGIC 3. Storing the enriched summaries back into Unity Catalog for downstream analysis.
# MAGIC
# MAGIC <small>💡 Batch processing unlocks insights from historical claims data, enabling proactive fraud detection and improved operational efficiency.</small>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC <div style="background: #ffe6e6; border-left: 5px solid #ff5f46; padding: 15px; margin: 20px 0; font-size: 16px;">
# MAGIC 🚨 <b>Notice:</b> Uncomment the following block of code to generate claim summaries in batch!  
# MAGIC
# MAGIC
# MAGIC ⚠️ <i>Note:</i> This operation may take some time depending on the size of your dataset and the size of Databricks compute that you utilize.
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE suspicious_claim_report_table AS
# MAGIC SELECT 
# MAGIC   claim_no, 
# MAGIC   suspicious_activity, 
# MAGIC   generate_claim_summary(
# MAGIC     claim_no, 
# MAGIC     policy_no, 
# MAGIC     CHASSIS_NO, 
# MAGIC     claim_amount_total, 
# MAGIC     incident_type, 
# MAGIC     incident_date, 
# MAGIC     incident_severity, 
# MAGIC     driver_age, 
# MAGIC     suspicious_activity, 
# MAGIC     MAKE, 
# MAGIC     MODEL, 
# MAGIC     MODEL_YEAR, 
# MAGIC     POLICYTYPE, 
# MAGIC     SUM_INSURED, 
# MAGIC     BOROUGH, 
# MAGIC     NEIGHBORHOOD
# MAGIC   ) AS claim_report 
# MAGIC FROM (
# MAGIC   SELECT * 
# MAGIC   FROM claim_policy_telematics_ml
# MAGIC   WHERE suspicious_activity = true
# MAGIC   -- REMOVE LIMIT if you would like to apply this summarization to more claims!
# MAGIC   LIMIT 10
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM suspicious_claim_report_table LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to notebook [04.2-Agent-Creation-Guide]($./04.2-Agent-Creation-Guide) in order to package the above functions into an implementable AI Agent with Databricks!
