# Databricks notebook source
# MAGIC %md
# MAGIC # Optional: Setup an external Model Serving Endpoint
# MAGIC
# MAGIC The Model Serving Endpoints used by your AI function can be of any type.
# MAGIC
# MAGIC You can setup an endpoint to hit an external Foundation Model, such as Azure OpenAI.
# MAGIC
# MAGIC This let you define your secret and connection setup once, and allow other users to access your external model, ensuring security, cost control and traceability.
# MAGIC
# MAGIC These can be of different flavor: Azure OpenAI, Anthropic, OpenAI...
# MAGIC
# MAGIC ## Setting up Azure OpenAI
# MAGIC
# MAGIC To be able to call our AI function, we'll need a key to query the Azure OpenAI API. Here are the steps to retrieve your key:
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
# MAGIC ### Create an external model endpoint
# MAGIC To run the following code, connect this notebook to a <b> Standard interactive cluster </b>.

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.18.0 mlflow==2.9.0
# MAGIC dbutils.library.restartPython()

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
                        "provider": "openai", #can support openAI or other external models. see https://docs.databricks.com/en/generative-ai/external-models/index.html
                        "task": "llm/v1/chat",
                        "openai_config": {
                            "openai_api_type": "azure",
                            "openai_api_key": "{{secrets/dbdemos/azure-openai}}", #Replace with your own azure open ai key
                            "openai_deployment_name": "dbdemo-gpt35", #your deployment name
                            "openai_api_base": "https://dbdemos-open-ai.openai.azure.com/", #your api base
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
# MAGIC ## Your external model is now ready!
# MAGIC
# MAGIC You can use it in your SQL AI function:
# MAGIC
# MAGIC `SELECT AI_QUERY('dbdemos-azure-openai', 'This will be sent to azure openai!')``
# MAGIC
# MAGIC Congrats! You're now ready to use any model within your SQL queries!
# MAGIC
# MAGIC Go back to [the introduction]($./00-SQL-AI-Functions-Introduction)
