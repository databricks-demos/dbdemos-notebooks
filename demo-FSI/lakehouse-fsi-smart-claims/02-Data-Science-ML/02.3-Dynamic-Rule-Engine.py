# Databricks notebook source
# MAGIC %md
# MAGIC # Speedup decision process with a Rules Engine 
# MAGIC
# MAGIC To accelerate our claims decision process, we can setup pre-defined static checks that can be applied without requiring a human in the loop.
# MAGIC
# MAGIC These rules will leverage our previous Deep Learning image analysis. 
# MAGIC
# MAGIC Typically, minor damage can be automatically validated, but if the reported damage doesn't match what our AI model found, we'll flag the claim to involve additional human investigation (Checks on policy coverage, assessed severity, accident location and speed limit violations)
# MAGIC
# MAGIC ## Implementing Dynamic rules
# MAGIC
# MAGIC Many system exists to build such rules. These can be natively implemented in Spark or using Spark Declarative Pipelines (check `dbdemos.install('declarative-pipeline-unit-test')` for an example of dynamic SDP rules)
# MAGIC
# MAGIC In this simple example, we'll add our rules as SQL statement in a table table and then apply them over our dataset.
# MAGIC
# MAGIC This will allow our business to easily add / edit rules by simply adding a SQL entry to our table.
# MAGIC
# MAGIC * Rule definition inludes:
# MAGIC   * Unique Rule name/id
# MAGIC   * Definition of acceptable and not aceptable data - written as code that can be directly applied
# MAGIC   * Severity (HIGH, MEDIUM, LOW)
# MAGIC   * Is Active (True/False)
# MAGIC
# MAGIC * Some common checks include
# MAGIC   * Claim date should be within coverage period
# MAGIC   * Reported Severity should match ML predicted severity
# MAGIC   * Accident Location as reported by telematics data should match the location as reported in claim
# MAGIC   * Speed limit as reported by telematics should be within speed limits of that region if there is a dispute on who was on the offense 
# MAGIC
# MAGIC
# MAGIC   <img width="800px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/smart-claims/rule_engine.png" />
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=02.3-Dynamic-Rule-Engine&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE claims_rules (
# MAGIC   rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   rule STRING, 
# MAGIC   check_name STRING,
# MAGIC   check_code STRING,
# MAGIC   check_severity STRING,
# MAGIC   is_active Boolean
# MAGIC );
# MAGIC ALTER TABLE claims_rules SET OWNER TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating our Rules

# COMMAND ----------

# MAGIC %md
# MAGIC ### Invalid Policy Date

# COMMAND ----------

def insert_rule(rule, name, code, severity, is_active):
  spark.sql(f"INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('{rule}', '{name}', '{code}', '{severity}', {is_active})")

invalid_policy_date = '''
  CASE WHEN to_date(pol_eff_date, "yyyy-MM-dd") < to_date(claim_date) and to_date(pol_expiry_date, "yyyy-MM-dd") < to_date(claim_date) THEN "VALID" 
  ELSE "NOT VALID"  
  END
'''

insert_rule('invalid policy date', 'valid_date', invalid_policy_date, 'HIGH', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exceeds Policy Amount

# COMMAND ----------

exceeds_policy_amount = '''
CASE WHEN  sum_insured >= claim_amount_total 
    THEN "calim value in the range of premium"
    ELSE "claim value more than premium"
END 
'''
insert_rule('exceeds policy amount', 'valid_amount', exceeds_policy_amount, 'HIGH', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Severity Mismatch

# COMMAND ----------

severity_mismatch = '''
CASE WHEN    damage_prediction.label="major" AND damage_prediction.score > 0.8 THEN  "Severity matches the report"
       WHEN  damage_prediction.label="minor" AND damage_prediction.score > 0.6 THEN  "Severity matches the report"
       WHEN  damage_prediction.label="ok" AND damage_prediction.score > 0.4 THEN  "Severity matches the report"
       ELSE "Severity does not match"
END 
'''

insert_rule('severity mismatch', 'reported_severity_check', severity_mismatch, 'HIGH', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exceeds Speed

# COMMAND ----------

exceeds_speed = '''
CASE WHEN  telematics_speed <= 45 and telematics_speed > 0 THEN  "Normal Speed"
       WHEN telematics_speed > 45 THEN  "High Speed"
       ELSE "Invalid speed"
END
'''
insert_rule('exceeds speed', 'speed_check', exceeds_speed, 'HIGH', True)

# COMMAND ----------

release_funds = '''
CASE WHEN  reported_severity_check="Severity matches the report" and valid_amount="calim value in the range of premium" and valid_date="VALID" then "release funds"
       ELSE "claim needs more investigation" 
END
'''
insert_rule('release funds', 'speed_check', release_funds, 'HIGH', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Application of Rules 

# COMMAND ----------

df = spark.sql("SELECT * FROM claim_policy_accident")
display(df.limit(10))

# COMMAND ----------

rules = spark.sql('SELECT * FROM claims_rules where is_active=true order by rule_id').collect()
for rule in rules:
  print(rule.rule, rule.check_code)
  df=df.withColumn(rule.check_name, F.expr(rule.check_code))

#overwrite table with new insights
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("claim_insights")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT valid_date, valid_amount, reported_severity_check FROM claim_insights

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Some data points can be checked out easily using <b>simple rules</b>, others may require <b>ML models </b>to score the data to produce the desired insights. In this notebook and the previous one, we have demonstrated how the insights generated from both can be consolidated into <b> structured </b> tabular data for easy consumption from a <b> dashboard </b> by various stakeholders including executives and investigation officers who use the same data but for completely different purposes. <br>
# MAGIC Moreover, all these insights are well-secured by <b> Unity Catalog </b>. So if the data, model, or insight is sensitive and is meant for select audiences then using a few simple 'GRANT' statements can ensure that it is adequately protected and is never accidentally exposed to the wrong party. Dynamic masking can be used to hide PII data. In the next notebook, we'll see how we can put all of this on auto-drive using Databricks workflows.
