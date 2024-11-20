# Databricks notebook source
# MAGIC %md-sandbox # Deploying the Agent System for Prescriptive Maintenance using the Mosaic AI Agent Framework
# MAGIC
# MAGIC Now that have created the Mosaic AI Tools in Unity Catalog, we will leverage the Mosaic AI Agent Framework to build, deploy and evaluate an AI agent for Prescriptive Maintenance. The Agent Framework comprises a set of tools on Databricks designed to help developers build, deploy, and evaluate production-quality AI agents like Retrieval Augmented Generation (RAG) applications. Moreover, Mosaic AI Agent Evaluation provides a platform to capture and implement human feedback, ground truth, response and request logs, LLM judge feedback, chain traces, and more.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to deploy the prescriptive maintenance agent defined in [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-build-agent-iot-turbine-prescriptive-maintenance) notebook. This notebook does the following:
# MAGIC 1. Logs the agent to MLflow
# MAGIC 2. Evaluate the agent with Agent Evaluation
# MAGIC 3. Registers the agent to Unity Catalog
# MAGIC 4. Deploys the agent to a Model Serving endpoint
# MAGIC
# MAGIC This is an high-level overview of the agent system that we will deploy in this demo:
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/agent2.png?raw=true" width="900px">
# MAGIC </div>
# MAGIC
# MAGIC The resulting prescriptive maintenance agent is able to perform a variety of prescriptive actions to augment maintenance technicians, including:
# MAGIC - Predicting turbine failure
# MAGIC - Retrieving specification information about turbines
# MAGIC - Generating maintenance work orders using past maintenance reports
# MAGIC - Answering follow-up questions about work orders

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow==2.17.2 databricks-agents==0.7.0 databricks-sdk==0.34.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-build-agent-iot-turbine-prescriptive-maintenance) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Log the model to MLflow
import os
import mlflow

from mlflow.models import ModelConfig
from mlflow.models.signature import ModelSignature
from mlflow.models.rag_signatures import (
    ChatCompletionRequest,
    ChatCompletionResponse,
)
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
config = ModelConfig(development_config="config.yml")
resources = [DatabricksServingEndpoint(endpoint_name=config.get("llm_endpoint")),
             DatabricksServingEndpoint(endpoint_name=MODEL_SERVING_ENDPOINT_NAME)]
uc_functions_to_expand = config.get("tools").get("uc_functions")
for func in uc_functions_to_expand:
    # if the function name is a regex, get all functions in the schema
    if func.endswith("*"):
        catalog, schema, _ = func.split(".")
        expanded_functions = list(
            w.functions.list(catalog_name=catalog, schema_name=schema)
        )
        for expanded_function in expanded_functions:
            resources.append(
                DatabricksFunction(function_name=expanded_function.full_name)
            )
    # otherwise just add the function
    else:
        resources.append(DatabricksFunction(function_name=func))

signature = ModelSignature(ChatCompletionRequest(), ChatCompletionResponse())

input_example = {
    "messages": [{"role": "user", "content": "Turbine ID = 5ef39b37-7f89-b8c2-aff1-5e4c0453377d, Sensor Readings = (0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638)"}]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        "05.2-build-agent-iot-turbine-prescriptive-maintenance",
        python_model=os.path.join(
            os.getcwd(),
            "05.2-build-agent-iot-turbine-prescriptive-maintenance",
        ),
        signature=signature,
        input_example=input_example,
        model_config="config.yml",
        resources=resources,
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
                    "content": "Act as an assistant for wind turbine maintenance technicians to generate work orders and answer follow-up questions.\nThese are the tools you can use to answer questions:\n- Turbine_predictor: takes as input sensor_readings and predicts whether or not a turbine is at risk of failure. If turbine is predicted to be ‘ok’, end the chain and return ’N/A’.\n- Turbine_maintenance_reports_retriever: takes sensor_readings as input and retrieves historical maintenance reports with similar sensor_readings.\n- Turbine_specifications_retriever: takes turbine_id as input and retrieves turbine specifications.\n\nIf both turbine_id and sensor_readings are provided as input, generate work order with the following template:\n\nTurbine Details:\n - Turbine ID: [Turbine ID]\n - Model: [Model]\n - Location: [Location]\n - State: [State]\n - Country: [Country]\n - Lat: [Lat]\n - Long: [Long]\n\nIdentified Issue: [Identified issue based on sensor readings]\nRoot Causes: [Historical root causes for similar issues]\n\nTasks:\n1. [Task 1] (Parts: [Parts], Tools: [Tools], Time: [Time])\n<add more if needed>\n\nPriority: [Priority level]\nDeadline: [Deadline]"
                },
                {
                    "role": "user",
                    "content": "Turbine ID = 5ef39b37-7f89-b8c2-aff1-5e4c0453377d, Sensor Readings = (0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638)"
                }
            ]
        },
        "expected_response": None
    }
]

eval_dataset = pd.DataFrame(eval_examples)
display(eval_dataset)

# COMMAND ----------

import mlflow
import pandas as pd

with mlflow.start_run(run_id=logged_agent_info.run_id):
    eval_results = mlflow.evaluate(
        f"runs:/{logged_agent_info.run_id}/05.2-build-agent-iot-turbine-prescriptive-maintenance",  # replace `chain` with artifact_path that you used when calling log_model.
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

#Define environment variables
env_vars = {"DATABRICKS_TOKEN": f"{{{{secrets/{secret_scope_name}/{secret_key_name}}}}}"}

# Deploy the model to the review app and a model serving endpoint
deployment = agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, environment_vars = env_vars)

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

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

principal_id = "" # TO DO: fill with system service principal ID from the agent model serving endpoint

# Add read permission on the secret to the service principal
WorkspaceClient().secrets.put_acl(
    scope=secret_scope_name, 
    principal=principal_id, 
    permission=workspace.AclPermission.READ
)

# COMMAND ----------

# MAGIC %md
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
# MAGIC ## Generating the work orders in batch mode
# MAGIC To automate the work order generation process, we will invoke the prescriptive maintenance agent in batch mode, while leveraging the parallelization benefits of Spark. Next, we will write out the resulting work orders in a UC-managed Delta table, which can be leveraged in an AI/BI dashboard to discover an overview of the generated work orders. Additionally, the work orders can be synchronized to work order management systems to assign them directly to field service engineers (for simplicity out of scope for this demo). Let's first load the 'new' input data we want to feed into the AI system and format properly in a way the agent expects. For simplicity of this demo, we use the training set to generate work orders.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
from pyspark.sql.functions import udf, struct, col

df = spark.table(f"{catalog}.{db}.turbine_training_dataset").dropDuplicates(["turbine_id"])[['turbine_id', 'sensor_vector']].limit(200)

# Define the UDF to return a struct
format_inputs_udf = udf(lambda turbine_id, sensor_vector: {'messages':[{
    "role": "user",
    "content": turbine_id + ', ' + str(sensor_vector)
}]}, "map<string, array<struct<role:string, content:string>>>")

# Apply the UDF to each row
df = df.withColumn("request", format_inputs_udf(df["turbine_id"], df["sensor_vector"]))

# Display the input dataset
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invoke the agent in batch
# MAGIC
# MAGIC Next, we can invoke our agent in parallel, to accelerate the generation of work orders.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# Load the mlflow model from UC
model = mlflow.pyfunc.load_model(f"models:/{UC_MODEL_NAME}@prod")

# Function to apply model.predict to every row
def predict_row(row):
    turbine_id, request = row['turbine_id'], row['request']
    response = model.predict(request)
    return turbine_id, response['choices'][0]['message']['content']

# Use ThreadPoolExecutor to apply the predict_row function to each row
with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
    predictions = list(executor.map(predict_row, df.select("turbine_id", "request").rdd.map(lambda row: row.asDict()).collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write out the work orders as Delta table in Unity Catalog
# MAGIC The final step is to write out the create work orders to Unity catalog. We will only write out the outputs that are predicted to be faulty, because only for them the agent generates work orders. Note that the final number of generated work orders is indeed lower than the total number of turbines.

# COMMAND ----------

from pyspark.sql.functions import expr, col

# Create new dataframe for work orders with turbine_id's and agent_outputs
work_orders = spark.createDataFrame(predictions, schema=["turbine_id", "work_order"]).withColumn("work_order", expr("substring(work_order, instr(work_order, 'Turbine Details'), length(work_order))"))

# Keep only records that are predicted to be faulty and filter out the ones that are predicted to be 'ok'
work_orders = work_orders.filter(col("work_order").startswith('Turbine Details'))

# Write out the new work orders as a Delta table to Unity Catalog
work_orders.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{db}.work_orders")

display(work_orders)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Discover the work orders in the AI/BI Dashboard
# MAGIC Finally, the Delta table with the work orders in Unity Catalog can be leveraged in the AI/BI dashboard to browse through the generated work orders.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Dashboard.png?raw=true" style="float: right; width: 50px; margin-left: 10px">

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
