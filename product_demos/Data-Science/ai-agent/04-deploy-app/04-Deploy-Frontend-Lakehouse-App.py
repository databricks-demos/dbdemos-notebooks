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
# MAGIC <strong>Note:</strong> In this example, we'll deploy the app using the endpoint. However, if the only use-case is the app itself, you can also directly package your MLFlow Chat Agent within your application, and remove the endpoint entirely!
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=04-Deploy-Frontend-Lakehouse-App&demo_name=ai-agent&event=VIEW">

# COMMAND ----------

# MAGIC %pip install --quiet -U mlflow[databricks]>=3.1.4 databricks-sdk>=0.59.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add your application configuration
# MAGIC
# MAGIC Lakehouse apps allow you to work with any Python framework. For our demo, we'll create a simple configuration file containing the model serving endpoint name and save it as `chatbot_app/app.yaml`.

# COMMAND ----------

print(f"The Databricks APP will be using the following model serving endpoint: {ENDPOINT_NAME}")

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
# MAGIC ## Capturing feedback through MLFlow Tracing and Feedback API
# MAGIC
# MAGIC With MLFLow 3, it's now easy to directly capture feedback (thumb up/down) from your application!
# MAGIC
# MAGIC ```
# MAGIC client = mlflow.deployments.get_deploy_client("databricks")
# MAGIC
# MAGIC input_message = [{"content": "test", "role": "user", "type": "message"}]
# MAGIC
# MAGIC response = client.predict(
# MAGIC   endpoint=ENDPOINT_NAME,
# MAGIC   inputs={'input': input_message, "databricks_options": {
# MAGIC       # Return the trace so we can get the trace_id for logging feedback. (return only the id for faster results)
# MAGIC       "return_trace": True
# MAGIC     }}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Then, simply use the `mlflow-tracing` in your chatbot backend to send emit the trace with the user feedback:
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC mlflow.log_feedback(
# MAGIC                 trace_id=trace_id, #the trace id present in the response, typically tr-xxxxx
# MAGIC                 name='user_feedback',
# MAGIC                 value=True if like_data.liked else False,
# MAGIC                 rationale=None,
# MAGIC                 source=mlflow.entities.AssessmentSource(source_type='HUMAN', source_id='user')
# MAGIC             )
# MAGIC ```
# MAGIC
# MAGIC *Note: you can also manage your own IDs - see the [feedback documentation](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/collect-user-feedback/) for more details*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's now create our chatbot application using Gradio using Databricks Applications

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

import mlflow

xp_name = os.getcwd().rsplit("/", 1)[0]+"/03-knowledge-base-rag/03.1-pdf-rag-tool"
mlflow.set_experiment(xp_name)

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
