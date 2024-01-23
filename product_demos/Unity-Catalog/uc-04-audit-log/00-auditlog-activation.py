# Databricks notebook source
# MAGIC %md 
# MAGIC # Enabling audit log
# MAGIC 
# MAGIC Audit log isn't enabled by default and requires a few API call to be initialized. This notebook show you how to setup Audit Logs.
# MAGIC 
# MAGIC If you have already configured audit logs, you can skip this part and directly start with the [ingestion pipeline]($./01-AWS-Audit-log-ingestion) or [Audit log analysis queries]($./02-log-analysis-query) 
# MAGIC 
# MAGIC Please read the [Official documentation](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html) for more details.
# MAGIC 
# MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Faudit_log%2Fsetup&dt=FEATURE_UC_AUDIT">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a .netrc file
# MAGIC 
# MAGIC 
# MAGIC You can use a .netrc file to save your temporary credential. Please use a private user cluster to make sure only you can access the password.

# COMMAND ----------

# MAGIC %sh 
# MAGIC /bin/cat <<EOF > /home/ubuntu/.netrc
# MAGIC machine accounts.cloud.databricks.com
# MAGIC login xxxxx
# MAGIC password xxxxx
# MAGIC EOF

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install jq

# COMMAND ----------

# MAGIC %md ## 1/ Configure storage
# MAGIC 
# MAGIC See the [documentation](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html#step-1-configure-storage) for more detail

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --netrc-file /home/ubuntu/.netrc -X POST -n \
# MAGIC     'https://accounts.cloud.databricks.com/api/2.0/accounts/<databricks-account-id>/storage-configurations' \
# MAGIC   -d '{
# MAGIC     "storage_configuration_name": "databricks-workspace-storageconf-v1",
# MAGIC     "root_bucket_info": {
# MAGIC       "bucket_name": "<my-company-example-bucket>"
# MAGIC     }
# MAGIC   }'

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2/ Configure credential
# MAGIC 
# MAGIC See the [documentation](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html#step-2-configure-credentials) for more detail

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --netrc-file /home/ubuntu/.netrc -X POST -n \
# MAGIC    'https://accounts.cloud.databricks.com/api/2.0/accounts/<databricks-account-id>/credentials' \
# MAGIC    -d '{
# MAGIC    "credentials_name": "databricks-credentials-v1",
# MAGIC    "aws_credentials": {
# MAGIC      "sts_role": {
# MAGIC        "role_arn": "arn:aws:iam::<aws-account-id>:role/<my-company-example-role>"
# MAGIC      }
# MAGIC    }
# MAGIC  }'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Call the log delivery API
# MAGIC 
# MAGIC Note: Unity Catalog activity is logged at the level of the account. Do not enter a value into workspace_ids_filter.

# COMMAND ----------

# MAGIC 
# MAGIC %sh
# MAGIC curl --netrc-file /home/ubuntu/.netrc -X POST -n \
# MAGIC   'https://accounts.cloud.databricks.com/api/2.0/accounts/<databricks-account-id>/log-delivery' \
# MAGIC   -d '{
# MAGIC   "log_delivery_configuration": {
# MAGIC       "config_name": "<config_name>",
# MAGIC       "log_type": "AUDIT_LOGS",
# MAGIC       "output_format": "JSON",
# MAGIC       "credentials_id": "<credentials_id>",
# MAGIC       "storage_configuration_id": "<storage_configuration_id>",
# MAGIC       "delivery_path_prefix": "<delivery_path_prefix>",
# MAGIC       "workspace_ids_filter": []
# MAGIC     }
# MAGIC }'

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make sure it's enabled as expected.
# MAGIC 
# MAGIC Note that the first log delivery can take a couple of minute.

# COMMAND ----------

# MAGIC %sh
# MAGIC curl --netrc-file /home/ubuntu/.netrc -X GET -n \
# MAGIC   'https://accounts.cloud.databricks.com/api/2.0/accounts/<databricks-account-id>/log-delivery' | jq

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's cleanup our .netrc file

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm /home/ubuntu/.netrc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's it! Your audit logs are know ready for analysis.
# MAGIC 
# MAGIC Open the [ingestion pipeline]($./01-AWS-Audit-log-ingestion) to see how your logs can be leveraged.
