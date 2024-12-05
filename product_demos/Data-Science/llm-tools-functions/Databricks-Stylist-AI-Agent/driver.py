# Databricks notebook source
# MAGIC %md
# MAGIC # Driver notebook
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI Playground export. We generated three notebooks in the same folder:
# MAGIC - [agent]($./agent): contains the code to build the agent.
# MAGIC - [config.yml]($./config.yml): contains the configurations.
# MAGIC - [**driver**]($./driver): logs, evaluate, registers, and deploys the agent.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to deploy the agent defined in the [agent]($./agent) notebook. The notebook does the following:
# MAGIC 1. Logs the agent to MLflow
# MAGIC 2. Registers the agent to Unity Catalog
# MAGIC 3. Deploys the agent to a Model Serving endpoint
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.
# MAGIC - Review the contents of [config.yml]($./config.yml) as it defines the tools available to your agent and the LLM endpoint.
# MAGIC - Review and run the [agent]($./agent) notebook in this folder to view the agent's code, iterate on the code, and test outputs.
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See docs ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)) for details

# COMMAND ----------

# DBTITLE 1,Updating and Restarting Libraries in Python
# MAGIC %pip install -U -qqqq mlflow databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Log the agent as code from the [agent]($./agent) notebook. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# DBTITLE 1,Logging and Deploying an ML Model with MLflow
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
resources = [DatabricksServingEndpoint(endpoint_name=config.get("llm_endpoint"))]
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
    "messages": [{"role": "user", "content": "I need a dress for an interview I have today. What style would you recommend for today in Delhi India?"}]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        "agent",
        python_model=os.path.join(
            os.getcwd(),
            "agent",
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

# DBTITLE 1,Evaluate Agent with Agent evaluation
import pandas as pd

eval_examples = [
    {
        "request":  "I need a dress for an interview in Delhi. What style would you recommend for today?",

        "expected_response": "Lovely day today! Given the pleasant temperature of 25.0°C and no rain, I'd be happy to help you with an outfit recommendation for an interview. \
For a professional and stylish look, I suggest the following: \
 \
**For Women:** \
* A tailored white or light-colored blouse (e.g., pale blue or pastel pink) with a modest neckline. Consider a classic style with a \ relaxed fit, such as a bell sleeve or a soft ruffle detail. \
* A pair of well-fitted, dark-washed trousers or a pencil skirt in a neutral color like navy, black, or gray. If you have a preferred size, please let me know, and I can provide more specific guidance.\
* A tailored blazer in a neutral color to add a professional touch. Look for one with a fitted silhouette and a classic lapel style. \
* Closed-toe heels in a neutral color like black, navy, or beige. A low to moderate heel (around 2-3 inches) is suitable for an interview. \
* Simple, elegant jewelry like a classic watch, a simple necklace, or a pair of stud earrings. \
\
**For Men:** \
\
* A crisp, white dress shirt with a conservative collar style. Consider a slim-fit or tailored fit to look polished. \
* A pair of well-fitted, dark-washed dress pants in a neutral color like navy, black, or gray. If you have a preferred size, please let me know, and I can provide more specific guidance. \
* A tailored blazer in a neutral color to add a professional touch. Look for one with a fitted silhouette and a classic lapel style. \
* Dress shoes in a neutral color like black, brown, or loafers. Make sure they are polished and in good condition. \
* A simple leather belt and a classic watch complete the outfit. \
\
**Common Colors:** \
\
* Neutral colors like navy, black, gray, beige, and white are always safe choices for an interview. \
* Earthy tones like olive green, terracotta, or rust can add a touch of personality to your outfit. \
 \
**Additional Tips:** \
 \
* Pay attention to the dress code specified by the company or the industry you're applying to. \
* Make sure your outfit is clean, ironed, and well-maintained. \
* Keep jewelry and accessories simple and understated. \
* Consider the company culture and dress accordingly. For example, if you're interviewing at a creative agency, you may be able to \ add a bit more personality to your outfit. \
 \
Feel free to share your size, preferred colors, or any other details you'd like me to consider, and I'll be happy to provide more specific guidance!"
    }

     
]

eval_dataset = pd.DataFrame(eval_examples)
display(eval_dataset)

# COMMAND ----------

# DBTITLE 1,Using MLflow for Evaluating a Databricks AI Agent
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

# DBTITLE 1,Register and Organize ML Model with Databricks UC
mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = "main_dbdemo_agent"
schema = "dbdemos_agent_tools"
model_name = "tools_agent_demo"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

# DBTITLE 1,Deploy Model to Review App and Serving Endpoint
from databricks import agents

# Deploy the model to the review app and a model serving endpoint
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Set up permision for the Review app for the specified model

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Databricks Agent Permissions for User Queries
from databricks import agents


# Note that <user_list> can specify individual users or groups.
agents.set_permissions(model_name='main_dbdemo_agent.dbdemos_agent_tools.tools_agent_demo', 
                       users=['xxx@xxx.com'], permission_level=agents.PermissionLevel.CAN_QUERY)

# COMMAND ----------


