# Databricks notebook source
# MAGIC %md
# MAGIC ## Optional/ Retrieve your Azure OpenAI details
# MAGIC
# MAGIC To be able to call our AI function, we'll need a key to query the API. In this demo, we'll be using Azure OpenAI. Here are the steps to retrieve your key:
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-setup-1.png" width="1250px">
# MAGIC
# MAGIC - Navigate to [Azure Portal > All Services > Cognitive Services > Azure OpenAI](https://portal.azure.com/#view/Microsoft_Azure_ProjectOxford/CognitiveServicesHub/~/OpenAI)  
# MAGIC - Click into the relevant resource
# MAGIC - Click on `Keys and Endpoint`
# MAGIC   - Copy down your:
# MAGIC     - **Key** (do **NOT** store this in plaintext, include it in your code, and/or commit it into your git repo)
# MAGIC     - **Resource name** which is represented in the endpoint URL (of the form `https://<resource-name>.openai.azure.com/`)
# MAGIC - Click on `Model deployments`
# MAGIC   - Note down your **model deployment name**
# MAGIC
# MAGIC When you use `AI_GENERATE_TEXT()` these values will map to the `deploymentName` and `resourceName` parameters. Here is the function signature:
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-setup-2.png" width="1000px">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fsql_ai_functions%2Fsetup_key&dt=DBSQL">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store your key using Databricks secrets
# MAGIC
# MAGIC We'll use [Databricks secrets](https://docs.databricks.com/security/secrets/index.html) to hold our API tokens. Use the [Databricks Secrets CLI](https://docs.databricks.com/dev-tools/cli/secrets-cli.html) or [Secrets API 2.0](https://docs.databricks.com/dev-tools/api/latest/secrets.html) to manage your secrets. The below examples use the Secrets CLI
# MAGIC
# MAGIC - If you don't already have a secret scope to keep your OpenAI keys in, create one now: 
# MAGIC   `databricks secrets create-scope --scope dbdemos`
# MAGIC - You will need to give `READ` or higher access for principals (e.g. users, groups) who are allowed to connect to OpenAI. 
# MAGIC   - We recommend creating a group `openai-users` and adding permitted users to that group
# MAGIC   - Then give that group `READ` (or higher) permission to the scope: 
# MAGIC     `databricks secrets put-acl --scope dbdemos --principal openai-users --permission READ`
# MAGIC - Create a secret for your API access token. We recommend format `<resource-name>-key`: 
# MAGIC   `databricks secrets put --scope dbdemos --key azure-openai`
# MAGIC
# MAGIC **Caution**: Do **NOT** include your token in plain text in your Notebook, code, or git repo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites
# MAGIC
# MAGIC In order to run this demo in your own environment you will need to satisfy these prerequisites:
# MAGIC
# MAGIC - Enrolled in the Public Preview. Request enrolment [here](https://docs.google.com/forms/d/e/1FAIpQLSdHOk5Wmk38zGqGhi27Q3ZiTpV7aIHipSa1Al9C0vfX0wYHfQ/viewform)
# MAGIC - Access to a Databricks SQL Pro or Serverless [warehouse](https://docs.databricks.com/sql/admin/create-sql-warehouse.html#what-is-a-sql-warehouse)
# MAGIC - An [Azure OpenAI key](https://learn.microsoft.com/en-us/azure/cognitive-services/openai/overview#how-do-i-get-access-to-azure-openai)
# MAGIC - Store the API key in Databricks Secrets (documentation: [AWS](https://docs.databricks.com/security/secrets/index.html), [Azure](https://learn.microsoft.com/en-gb/azure/databricks/security/secrets/), [GCP](https://docs.gcp.databricks.com/security/secrets/index.html)).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an external model endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC To run the following code, connect this notebook to a <b> Standard interactive cluster </b>.

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.18.0
# MAGIC %pip install mlflow==2.9.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from mlflow.deployments import get_deploy_client
deploy_client = get_deploy_client("databricks")

try:
    endpoint_name  = "dbdemos-azure-openai"
    deploy_client.create_endpoint(
        name=endpoint_name,
        config={
            "served_entities": [
                {
                    "name": endpoint_name,
                    "external_model": {
                        "name": "gpt-35-turbo",
                        "provider": "openai",
                        "task": "llm/v1/chat",
                        "openai_config": {
                            "openai_api_type": "azure",
                            "openai_api_key": "{{secrets/dbdemos/azure-openai}}", #Replace with your own azure open ai key
                            "openai_deployment_name": "dbdemo-gpt35",
                            "openai_api_base": "https://dbdemos-open-ai.openai.azure.com/",
                            "openai_api_version": "2023-05-15"
                        }
                    }
                }
            ]
        }
    )
    print(f"Endpoint {endpoint_name} created successfully")
except Exception as e:
    if 'RESOURCE_ALREADY_EXISTS' in str(e):
        print('Endpoint already exists')
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step: generate our demo dataset leveraging AI functions
# MAGIC
# MAGIC To create the demo dataset using this external model we just created, follow these steps: 
# MAGIC
# MAGIC 1. Head to the notebook [02-Generate-fake-data-with-AI-functions-Foundation-Model]($./02-Generate-fake-data-with-AI-functions-Foundation-Model). 
# MAGIC
# MAGIC 2. Change the model name from 'databricks-mpt-30b-instruct' to 'dbdemos-azure-openai' in all relevant cells in this notebook.
# MAGIC
# MAGIC 3. Similarly, head to the notebook [03-automated-product-review-and-answer]($./03-automated-product-review-and-answer). 
# MAGIC
# MAGIC 4. Change the model name from 'databricks-mpt-30b-instruct' to 'dbdemos-azure-openai' in all relevant cells in this notebook.
# MAGIC
# MAGIC Go back to [the introduction]($./01-SQL-AI-Functions-Introduction)

# COMMAND ----------


