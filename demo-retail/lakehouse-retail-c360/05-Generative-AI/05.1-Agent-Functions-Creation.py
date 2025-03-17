# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 🧠 From Predicting to Preventing Customer Churn with Gen AI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Retailers face increasing customer churn, rising acquisition costs, and fierce competition, making customer retention more critical than ever.
# MAGIC Despite investments in loyalty programs and CRM systems, many struggle with fragmented customer data, ineffective engagement strategies, and the inability to act in real-time—resulting in missed opportunities and declining customer lifetime value.
# MAGIC
# MAGIC <div style="background: #f7f7f7; border-left: 5px solid #ff5f46; padding: 20px; margin: 20px 0; font-size: 17px;">
# MAGIC "A recent review of the State of Customer Churn revealed that the <b>average churn rate</b> for the entire retail sector is <b>6.58%</b>, far higher than the <b>ideal rate of 3-5%</b>."[^3]
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #f7f7f7; border-left: 5px solid green; padding: 20px; margin: 20px 0; font-size: 17px;">
# MAGIC "With <b>prescriptive analytics</b> in place, decision makers can quickly extract <b>actionable knowledge</b> from the available data and define <b>effective actions</b> for the future."[^2]
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     <strong>Liza, a Generative AI Engineer,</strong> is leveraging Databricks AI to build a prescriptive system aimed at reducing customer churn.  
# MAGIC     By harnessing the full power of her company's data platform, she will create tailored advertising for at-risk customers, encouraging new purchases.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ### Liza's Goal  
# MAGIC Liza wants to develop a GenAI agent that minimizes the time Account Managers spend crafting customized outreach for customers at risk of churning. More specifically, Liza wants to make an agent that can do multiple things, including:
# MAGIC
# MAGIC 1. Determining if a customer is likely to churn.
# MAGIC 2. If that customer is likely to churn, looking up their recent order information.
# MAGIC 3. Based on that recent order information, generating custom, tailored marketing messaging in order to re-engage that customer and encourage their future purchases.
# MAGIC
# MAGIC Fortunately, on Databricks, building a multi-faceted AI System with tools that chain off of one another is simple. In the below sections, see how we make it happen!
# MAGIC
# MAGIC [^1]: https://www.qualtrics.com/experience-management/customer/customer-churn/
# MAGIC
# MAGIC [^2]: https://voziq.ai/featured/prescriptive-analytics-for-churn-reduction/
# MAGIC
# MAGIC [^3]: https://www.custify.com/blog/customer-churn-guide/
# MAGIC
# MAGIC [^4]: https://www.infobip.com/blog/how-generative-ai-can-help-reduce-churn
# MAGIC
# MAGIC [^5]: https://www.salesforce.com/sales/analytics/customer-churn/
# MAGIC
# MAGIC [^6]: https://graphite-note.com/customer-churn-prevention-predictive-analytics/
# MAGIC
# MAGIC [^7]: https://churnzero.com/blog/customer-success-statistics/
# MAGIC
# MAGIC [^8]: https://www.b2brocket.ai/blog-posts/ai-powered-churn-prediction
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05.1-Agent-Functions-Creation&demo_name=lakehouse-retail-c360&event=VIEW">
# MAGIC

# COMMAND ----------


# CONFIGURATION CELL, installing necessary packages for this demo.
%pip install mlflow==2.20.1 databricks-vectorsearch==0.40 databricks-feature-engineering==0.8.0 databricks-sdk==0.40.0
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🔍 Function 1: Customer Churn Prediction  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     To build a Generative AI system that reduces customer churn, Liza first needs a function to predict whether a given customer is at risk of churning.  
# MAGIC     Currently, her company's Account Managers manually look up customer IDs to review recent order history.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Solution  
# MAGIC Liza will develop a function that:
# MAGIC - Accepts a **customer ID** as input.  
# MAGIC - Returns a **predicted churn status**.  
# MAGIC
# MAGIC By leveraging Databricks' `ai_query` feature, she will call the **Churn Prediction model** defined in **Section 4** of this demo.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This function utilizes the churn prediction model we defined in [Section 4]($../04-Data-Science-ML/04.1-automl-churn-prediction) of this demo.

# COMMAND ----------

spark.sql("DROP FUNCTION IF EXISTS churn_predictor")

spark.sql(f"""
          CREATE OR REPLACE FUNCTION churn_predictor(id STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'This function determines whether a customer is at risk of churning. The possible value are AT RISK or NOT AT RISK of churning. Use this function when a User ID is given.'
RETURN
(
    SELECT CASE 
             WHEN ai_query(
                    'dbdemos_customer_churn_endpoint', 
                    named_struct(
                        'user_id', user_id,
                        'canal', MAX(canal),
                        'country', MAX(country),
                        'gender', MAX(gender),
                        'age_group', MAX(age_group),
                        'order_count', MAX(order_count),
                        'total_amount', MAX(total_amount),
                        'total_item', MAX(total_item),
                        'last_transaction', MAX(last_transaction),
                        'platform', MAX(platform),
                        'event_count', MAX(event_count),
                        'session_count', MAX(session_count),
                        'days_since_creation', MAX(days_since_creation),
                        'days_since_last_activity', MAX(days_since_last_activity),
                        'days_last_event', MAX(days_last_event)
                    ),
                    'STRING'
                 ) = '0' THEN 'NOT AT RISK'
             ELSE 'AT RISK'
           END
    FROM {catalog}.{db}.churn_user_features
    WHERE user_id = id GROUP BY user_id
)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC
# MAGIC Let's ensure that our new tool works. Here we look up a customer we know is at risk of churn:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT churn_predictor(
# MAGIC     '2d17d7cd-38ae-440d-8485-34ce4f8f3b46'
# MAGIC ) AS prediction
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📊 Function 2: Customer Order Information Lookup  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     Now that Liza's first function can identify customers at risk of churning, she needs an additional tool to retrieve their recent purchase behavior.  
# MAGIC     This function will also collect customer demographic data, ensuring that the AI-generated messaging is personalized and more effective at reducing churn.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Next Step  
# MAGIC In the following section, we define a function that retrieves a customer's recent orders and demographic information.

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION customer_order_lookup (
    input_user_id STRING
    COMMENT 'User ID of the customer to be searched'
  ) 
  RETURNS TABLE(
    firstname STRING,
    channel STRING,
    country STRING,
    gender INT,
    order_amount INT,
    order_item_count INT,
    last_order_date STRING,
    current_date STRING,
    days_elapsed_since_last_order INT
  )
  COMMENT "This function returns the customer details for a given customer User ID, along with their last order details. The return fields include First Name, Channel (e.g. Mobile App, Phone, or Web App/Browser ), Country of Residence, Gender, Order Amount, Item Count, and the Last Order Date (in format of yyyy-MM-dd). Use this function when a User ID is given." 
  RETURN (
    SELECT
      firstname,
      canal as channel,
      country,
      gender,
      o.order_amount,
      o.order_item_count,
      o.last_order_date,
      TO_CHAR(CURRENT_DATE(), 'yyyy-MM-dd') AS current_date,
      DATEDIFF(CURRENT_DATE(), o.last_order_date) AS days_elapsed_since_last_order
    FROM 
      {catalog}.{db}.churn_users u
    LEFT JOIN (
      SELECT
        user_id,
        amount AS order_amount,
        item_count AS order_item_count,
        TO_CHAR(creation_date, 'yyyy-MM-dd') AS last_order_date
      FROM {catalog}.{db}.churn_orders
      WHERE user_id = input_user_id
      ORDER BY creation_date DESC
      LIMIT 1
    ) o ON u.user_id = o.user_id
    WHERE
      u.user_id = input_user_id
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example:
# MAGIC To verify the function retrieves orders accurately, we perform the following check:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_order_lookup('2d17d7cd-38ae-440d-8485-34ce4f8f3b46')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ✉️ Function 3: Personalized Customer Outreach with Automated Marketing Copy Generation  
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" 
# MAGIC        style="width: 100px; border-radius: 10px;" />
# MAGIC   <div>
# MAGIC     Liza now has the ability to identify at-risk customers and retrieve their order history and demographic details.  
# MAGIC     The next step is to generate personalized marketing copy to proactively re-engage these customers.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### 🛠️ Solution  
# MAGIC This function will utilize **LLMs** to create targeted outreach messages, enabling Account Managers to scale their efforts efficiently. Previously, Account Managers spent significant time on personalized customer outreach to reduce churn. Now, with GenAI, their efforts will be far more efficient.
# MAGIC
# MAGIC In the next section, we define the function that automates **customer-specific messaging** to reduce churn. This function is the most complex of the three, so we are going to break it down into three distinct steps below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Define the model prompt
# MAGIC
# MAGIC This function will utilize Llama by-default for creating customized marketing copy on the fly. 
# MAGIC
# MAGIC Feel free to manipulate and change the below prompt in order to generate marketing copy that you believe would be the most impactful.

# COMMAND ----------

# Feel free to change this prompt! Just know that by changing it, your generated copy may be different. Databricks
#. does not guarantee quality or content of the generated copy in this demo.
prompt = """
    Your only role is to write copy for a contemporary apparel retailer. All your responses are a piece of copy, nothing further.\n
    You have access to the following information from the customer and their last order:\n
    - firstname: the first name of the customer
    - channel: the channel of which the customer is using. This could be WEBAPP for Web app or MOBILE for Mobile app
    - country: the customer's country of residence. This field uses string country code, for example FR for France
    - gender: the customer's gender. This field uses 0 for Male and 1 for Female
    - order_amount: the total amount in US dollars of the last order
    - order_item_count: the total item of the last order
    - last_order_date: the date of the last order in format of yyyy-MM-dd
    - current_date: the current date in format of yyyy-MM-dd
    - days_elapsed_since_last_order: the number of days elapsed since the last order
    - churn: either the customer is AT RISK or NOT AT RISK at churning

    These are the rules you must respect when writing copy:
    - Start every message by greeting the customer using their first_name. Do not include a final greeting.\n
    - When the customer's country of residence is not an English-spoken country, include a second message in their native language based on their country of residence. For example, if the country is FR, write two messages: one in English, and another in French. Never explicitly mention their country of residence.\n
    - Write short copy (SMS or push notification style) when channel is PHONE and MOBILE, and long copy (email style) for canal WEBAPP. Never explicitly mention the channel.
    - Never mention a customer's gender.\n
    - Consider the total amount of their last order when pushing for discount. For example, if the total amount is less than $100, push for discount. If the total amount is greater than $100, push for a free shipping offer.\n
    - Consider the total number of items in their last order when pushing for package deal. For example, if the total number of items is less than 5, push for package deal. If the total number of items is greater than 5, push for a free shipping offer.\n
    - Consider the days elapsed since the last purchased date when writing copy. For example, if the time elapsed is less than 30 days, push for discount. If the time elapsed is greater than 30 days, push for a free shipping offer.\n
    - Consider the current date when writing copy to take into account the seasonality of the country. For example, if the current season is Summer in the customer's country, promote for some products in summer and vice versa.\n
    - If the customer is at risk at churning, add urgency to the copy. Never explicitly mention the customer's churn status.\n
    - Make sure the input parameters match the order of the function.
    """

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Function definition
# MAGIC
# MAGIC Next, like before, we will utilize `ai_query` to define our function, leveraging the prompt from our previous cell as well as the previous two functions. 

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION generate_marketing_copy(input_user_id STRING, firstname STRING, channel STRING, country STRING, gender INT, order_amount INT, order_item_count INT, last_order_date STRING, current_date STRING, days_elapsed_since_last_order INT, churn_status STRING)
  RETURNS STRING
  LANGUAGE SQL
  COMMENT "This function generates marketing copy for a given customer User ID. This function expects the user order information which can be found with churn_predictor(), as well as their churn status which is found with customer_order_lookup(). This function returns the marketing copy as a string. "
  RETURN (
    SELECT ai_query(
      'databricks-meta-llama-3-1-405b-instruct', 
      CONCAT(
        "{prompt}" 
        'Customer details: ', 
        TO_JSON(NAMED_STRUCT('firstname', firstname, 'channel', channel, 'country', country, 'gender', gender, 'order_amount', order_amount, 'order_item_count', order_item_count, 'last_order_date', last_order_date, 'current_date', current_date, 'days_elapsed_since_last_order', days_elapsed_since_last_order, 'churn_status', churn_status))
      )
    )
  )
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Step 3: Example:
# MAGIC Now, let's make sure that this works. Let's input manually a customer's information that we know is likely to churn. Of course, in practice, we expect that this function will automatically utilize the output of the previous two functions, creating a _compound system_.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT generate_marketing_copy(
# MAGIC   '2d17d7cd-38ae-440d-8485-34ce4f8f3b46',
# MAGIC   'Christopher',
# MAGIC   'WEBAPP',
# MAGIC   'USA',
# MAGIC   0,
# MAGIC   105,
# MAGIC   3,
# MAGIC   '2023-06-07',
# MAGIC   '2025-03-05',
# MAGIC   637,
# MAGIC   churn_predictor('2d17d7cd-38ae-440d-8485-34ce4f8f3b46')
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to notebook [05.2-Agent-Creation-Guide]($./05.2-Agent-Creation-Guide) in order to package the above functions into an implementable AI Agent with Databricks!
