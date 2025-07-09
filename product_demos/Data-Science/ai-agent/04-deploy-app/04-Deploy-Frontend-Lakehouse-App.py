# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 3/ Deploying our frontend App with Lakehouse Applications
# MAGIC
# MAGIC
# MAGIC Mosaic AI Agent Evaluation review app is used for collecting stakeholder feedback during your development process.
# MAGIC
# MAGIC You still need to deploy your own front end application!
# MAGIC
# MAGIC Let's leverage Databricks Lakehouse Applications to build and deploy our first, simple chatbot frontend app. 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-frontend-app.png?raw=true" width="1200px">
# MAGIC
# MAGIC
# MAGIC <div style="background-color: #d4e7ff; padding: 10px; border-radius: 15px;">
# MAGIC <strong>Note:</strong> Lakehouse apps are in preview, reach-out your Databricks Account team for more details and to enable it.
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=03-Deploy-Frontend-Lakehouse-App&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install --quiet -U mlflow>=3.1 databricks-sdk==0.49.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add your application configuration
# MAGIC
# MAGIC Lakehouse apps let you work with any python framework. For our small demo, we will create a small configuration file containing the model serving endpoint name used for our demo and save it in the `chatbot_app/app.yaml` file.

# COMMAND ----------

import yaml

# Our frontend application will hit the model endpoint we deployed.
# Because dbdemos let you change your catalog and database, let's make sure we deploy the app with the proper endpoint name
yaml_app_config = {"command": ["uvicorn", "main:app", "--workers", "1"],
                    "env": [{"name": "MODEL_SERVING_ENDPOINT", "value": ENDPOINT_NAME}]
                  }
try:
    with open('chatbot_app/app.yaml', 'w') as f:
        yaml.dump(yaml_app_config, f)
except Exception as e:
    print(f'pass to work on build job - {e}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capturing feedback straight to MLFlow
# MAGIC
# MAGIC With MLFLow 3, it's now easy to directly capture feedback (thumb up/down) from your application!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's now create our chatbot application using Gradio

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying our application
# MAGIC
# MAGIC Our application is made of 2 files under the `chatbot_app` folder:
# MAGIC - `main.py` containing our python code
# MAGIC - `app.yaml` containing our configuration
# MAGIC
# MAGIC All we now have to do is call the API to create a new app and deploy using the `chatbot_app` path:

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppResource, AppResourceServingEndpoint, AppResourceServingEndpointServingEndpointPermission, AppDeployment

w = WorkspaceClient()
app_name = "dbdemos-ai-agent-app"

# COMMAND ----------

# MAGIC %md
# MAGIC Lakehouse apps come with an auto-provisioned Service Principal. Let's grant this Service Principal access to our model endpoint before deploying...

# COMMAND ----------

serving_endpoint = AppResourceServingEndpoint(name=ENDPOINT_NAME,
                                              permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY
                                              )

rag_endpoint = AppResource(name="rag-endpoint", serving_endpoint=serving_endpoint) 

rag_app = App(name=app_name, 
              description="Your Databricks assistant", 
              default_source_code_path=os.path.join(os.getcwd(), 'chatbot_app'),
              resources=[rag_endpoint])
try:
  app_details = w.apps.create_and_wait(app=rag_app)
  print(app_details)
except Exception as e:
  if "already exists" in str(e):
    print("App already exists, you can deploy it")
  else:
    raise e

# COMMAND ----------

# MAGIC %md 
# MAGIC Once the app is created, we can (re)deploy the code as following:

# COMMAND ----------

deployment = AppDeployment(source_code_path=os.path.join(os.getcwd(), 'chatbot_app'))

app_details = w.apps.deploy_and_wait(app_name=app_name, app_deployment=deployment)

# COMMAND ----------

#Let's access the application
w.apps.get(name=app_name).url

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Your Lakehouse app is ready and deployed!
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-gradio-app.png?raw=true" width="750px" style="float: right; margin-left:10px">
# MAGIC
# MAGIC Open the UI to start requesting your chatbot.
# MAGIC
# MAGIC As improvement, we could improve our chatbot UI to provide feedback and send it to Mosaic AI Quality Labs, so that bad answers can be reviewed and improved.
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC We saw how Databricks provides an end to end platform: 
# MAGIC - Building and deploying an endpoint
# MAGIC - Buit-in solution to review, analyze and improve our chatbot
# MAGIC - Deploy front-end genAI application with lakehouse apps!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: ready to take it to a next level? Let's monitor our agent performance in production
# MAGIC
# MAGIC Open the [05-production-monitoring/05.production-monitoring]($../05-production-monitoring/05.production-monitoring) notebook to learn how to monitor your endpoint and evaluate it while in production!
