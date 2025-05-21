# Databricks notebook source
# MAGIC %md-sandbox # Deploying the Agent System for Prescriptive Maintenance using the Mosaic AI Agent Framework
# MAGIC
# MAGIC Now that have created the Mosaic AI Tools in Unity Catalog, we will leverage the Mosaic AI Agent Framework to build, deploy and evaluate an AI agent for Prescriptive Maintenance. The Agent Framework comprises a set of tools on Databricks designed to help developers build, deploy, and evaluate production-quality AI agents like Retrieval Augmented Generation (RAG) applications. Moreover, Mosaic AI Agent Evaluation provides a platform to capture and implement human feedback, ground truth, response and request logs, LLM judge feedback, chain traces, and more.
# MAGIC
# MAGIC <div style="float: right; margin-left: 20px; margin-bottom: 40px;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/agent2.png?raw=true" width="900px">
# MAGIC </div>
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to deploy the prescriptive maintenance agent defined in [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-build-agent-iot-turbine-prescriptive-maintenance) notebook. This notebook does the following:
# MAGIC 1. Logs the agent to MLflow
# MAGIC 2. Evaluate the agent with Agent Evaluation
# MAGIC 3. Registers the agent to Unity Catalog
# MAGIC 4. Deploys the agent to a Model Serving endpoint
# MAGIC
# MAGIC
# MAGIC This is an high-level overview of the agent system that we will deploy in this demo:
# MAGIC
# MAGIC
# MAGIC The resulting prescriptive maintenance agent is able to perform a variety of prescriptive actions to augment maintenance technicians, including:
# MAGIC - Predicting turbine failure
# MAGIC - Retrieving specification information about turbines
# MAGIC - Generating maintenance work orders using past maintenance reports
# MAGIC - Answering follow-up questions about work orders
# MAGIC
# MAGIC #### Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See docs ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)) for details

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow langchain langgraph==0.3.4 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-build-agent-iot-turbine-prescriptive-maintenance) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Log the model to MLflow
import sys
sys.path.append(".")
import os
import mlflow

input_example = {
    "messages": [{"role": "user", "content": "Fetch me information and readings for turbine 004a641f-e9e5-9fff-d421-1bf88319420b. Give me maintenance recommendation based on existing reports"}]
}

assert os.path.exists('config.yml') and os.path.exists('agent.py'), "Make sure you run the notebook 05.2 first to create the yaml file required in the demo deployment"

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import tools, LLM_ENDPOINT_NAME
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]
for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))


with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="agent.py",
        model_config="config.yml",
        input_example=input_example,
        resources=resources,
        extra_pip_requirements=[
            "databricks-connect"
        ]
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.

# COMMAND ----------

import pandas as pd

eval_examples = [
    {
        "request": {
            "messages": [
                {
                    "role": "system",
                    "content": """Act as an assistant for wind turbine maintenance technicians.\n
                                These are the tools you can use to answer questions:
                                \n- turbine_maintenance_predictor: takes as input sensor_readings and predicts whether or not a turbine is at risk of failure.
                                \n- turbine_maintenance_reports_predictor: takes sensor_readings as input and retrieves historical maintenance reports with similar sensor_readings. Critical for prescriptive maintenance.
                                \n- turbine_specifications_retriever: takes turbine_id as input and retrieves turbine specifications.
                                

                                \nIf a user gives you a turbine ID, first look up that turbine's information with turbine_specifications_retriever. 
                                \nIf a user asks for recommendations on how to do maintenance on a turbine, use the turbine reading and search for similar reports matching the turbine readings using the  turbine_maintenance_reports_predictor. Use the report retrived from other turbines to understand what could be happening and suggest maintenance recommendation.
                                """
                },
                {
                    "role": "user",
                    "content": "Fetch me information and readings for turbine 004a641f-e9e5-9fff-d421-1bf88319420b. Give me maintenance recommendation based on existing reports"
                }
            ]
        },
        "expected_response": """Based on the turbine specifications and maintenance reports, it appears that the turbine 004a641f-e9e5-9fff-d421-1bf88319420b may be experiencing issues with its pitch system, gearbox, or blades. The reports suggest that the turbine may be experiencing pitch system misalignment, high gearbox temperature, or blade delamination.
To address these issues, it is recommended to conduct comprehensive diagnostic tests of the pitch system, inspect the gearbox internals, and perform non-destructive testing on the blades. Additionally, regular maintenance procedures such as lubrication of moving parts, inspection of hydraulic lines, and vibration analysis can help prevent future malfunctions.
Specifically, the recommended solutions include:
Conducting comprehensive diagnostic tests of the pitch system and repairing or replacing faulty components
Inspecting the gearbox internals and replacing worn-out bearings, realigning gear meshes, and replenishing gearbox oil
Conducting non-destructive testing on the blades and repairing delaminated sections using composite patching materials or adhesive bonding techniques
Implementing regular maintenance procedures such as lubrication, inspection, and vibration analysis to prevent future issues.
It is also important to monitor the turbine's condition regularly and perform preventative maintenance to address any emerging issues promptly."""
    }
]

eval_dataset = pd.DataFrame(eval_examples)
display(eval_dataset)

# COMMAND ----------

import mlflow
import pandas as pd

with mlflow.start_run(run_id=logged_agent_info.run_id):
    eval_results = mlflow.evaluate(
        f"runs:/{logged_agent_info.run_id}/agent",  # replace `chain` with artifact_path that you used when calling log_model.
        data=eval_dataset,  # Your evaluation dataset
        model_type="databricks-agent",  # Enable Mosaic AI Agent Evaluation
    )

# Review the evaluation results in the MLFLow UI (see console output), or access them in place:
display(eval_results.tables['eval_results'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = f"{catalog}.{db}.{agent_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent
# MAGIC For PyFunc flavored agents, you must manually specify any resource dependencies during logging of the deployed agent in the resources parameter (see above under: "Log the `agent` as an MLflow model"). During deployment, databricks.agents.deploy creates an M2M OAuth token with access to the resources specified in the resources parameter, and deploys it to the deployed agent. For the DBSQL [vector_search function](https://docs.databricks.com/en/sql/language-manual/functions/vector_search.html) automatic authentication passthrough is supported. However, `vector_search function` doesn't support `DIRECT ACCESS` indexes (which we created before in [05.1-ai-tools-iot-turbine-prescriptive-maintenance]($./05.1-ai-tools-iot-turbine-prescriptive-maintenance) notebook). Therefore, we have to reference the secret in the model serving by specifying the secrets-based `environment vars`. This allows credentials to be fetched at serving time from model serving endpoints (see [Documentation](https://docs.databricks.com/en/machine-learning/model-serving/store-env-variable-model-serving.html#add-secrets-based-environment-variables)).

# COMMAND ----------

from databricks import agents

# Deploy the model to the review app and a model serving endpoint
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "playground"})

# COMMAND ----------

# MAGIC %md 
# MAGIC During deployment a system service principal was automatically created. To be able to query the agent from the Model Serving endpoint or the AI playground, we have to give this service principal read access to the secret scope. The system service principal ID can be found in the model serving endpoint events, which looks like this:
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-10-22%20at%2018.17.35.png?raw=true" width="900px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples in Databricks AI Playground
# MAGIC
# MAGIC Now you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See docs ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)) for detail. Please find below some examples questions the prescriptive maintenance agent can answer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### Example 1: Retrieve turbine specifications  
# MAGIC *Question:* "What's the model type of turbine with ID X?"
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/model.gif?raw=true" width="600px">
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### Example 2: Generate prescriptive work orders
# MAGIC *Question:* "Generate a work order if the turbine is predicted to be at risk of failure."
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/work%20order.gif?raw=true" width="600px">
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### Example 3: Answer follow-up Questions  
# MAGIC *Question:* "How can I perform task Y from the work order?"
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/follow-up%20question.gif?raw=true" width="600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! You have deployed your first Agent System for Prescriptive Maintenance! 
# MAGIC
# MAGIC We have seen how Databricks Mosaic AI provides all the capabilities needed to move these components to production quickly and cost-effectively, while maintaining complete control and governance:
# MAGIC - Simplifying model deployment by creating an API endpoint.
# MAGIC - Scaling similarity search against unstructured data to support billions of embeddings.
# MAGIC - Leveraging structured data by serving features on an endpoint.
# MAGIC - Deploying open models, while keeping the flexibility to swap them as needed.
# MAGIC - Integrating everything using popular orchestration frameworks like Langchain or LlamaIndex.
# MAGIC - Managing AI tools using Unity Catalog for security and re-use.
# MAGIC - Composing the AI system with the Mosaic AI Agent Framework.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Potential next steps:
# MAGIC Enhance the Agent System by incorporating:
# MAGIC - **Automated Technician Assignment for Work Orders:** Automatically asign maintenance work orders to technicians based on availability, distance to turbines and skill set.
# MAGIC - **Automated Field Service Route Optimization:** optimizes field service routes for technicians to execute the maintenance work orders based on priority levels of work orders, travel time and real-time traffic conditions.

# COMMAND ----------

# MAGIC %md
# MAGIC <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Open the Prescriptive Maintenance AI/BI dashboard</a> | [Go back to the introduction]($../00-IOT-wind-turbine-introduction-DI-platform)
