# Databricks notebook source
# MAGIC %md # Turn the Review App logs into an Evaluation Set
# MAGIC
# MAGIC The Review application captures your user feedbacks.
# MAGIC
# MAGIC This feedback is saved under 2 tables within your schema.
# MAGIC
# MAGIC In this notebook, we will show you how to extract the logs from the Review App into an Evaluation Set.  It is important to review each row and ensure the data quality is high e.g., the question is logical and the response makes sense.
# MAGIC
# MAGIC 1. Requests with a 👍 :
# MAGIC     - `request`: As entered by the user
# MAGIC     - `expected_response`: If the user edited the response, that is used, otherwise, the model's generated response.
# MAGIC 2. Requests with a 👎 :
# MAGIC     - `request`: As entered by the user
# MAGIC     - `expected_response`: If the user edited the response, that is used, otherwise, null.
# MAGIC 3. Requests without any feedback
# MAGIC     - `request`: As entered by the user
# MAGIC
# MAGIC Across all types of requests, if the user 👍 a chunk from the `retrieved_context`, the `doc_uri` of that chunk is included in `expected_retrieved_context` for the question.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=03-Offline-Evaluation&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install --quiet -U databricks-agents mlflow==2.20.0 mlflow-skinny==2.20.0 databricks-sdk==0.23.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1.1/ Extracting the logs 
# MAGIC
# MAGIC
# MAGIC *Note: for now, this part requires a few SQL queries that we provide in this notebook to properly format the review app into training dataset.*
# MAGIC
# MAGIC *We'll update this notebook soon with an simpler version - stay tuned!*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=advanced/02-Evaluate-RAG-Chatbot-Model&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %run ../_resources/00-init-advanced $reset_all_data=false

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
init_experiment_for_batch("chatbot-rag-llm-advanced", "advanced")

# COMMAND ----------

from databricks import agents
MODEL_NAME = "rag_demo_advanced"
MODEL_NAME_FQN = f"{catalog}.{db}.{MODEL_NAME}"
browser_url = mlflow.utils.databricks_utils.get_browser_hostname()

# # Get the name of the Inference Tables where logs are stored
active_deployments = agents.list_deployments()
active_deployment = next((item for item in active_deployments if item.model_name == MODEL_NAME_FQN), None)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(active_deployment)
endpoint = w.serving_endpoints.get(active_deployment.endpoint_name)

try:
    endpoint_config = endpoint.config.auto_capture_config
except AttributeError as e:
    endpoint_config = endpoint.pending_config.auto_capture_config

inference_table_name = endpoint_config.state.payload_table.name
inference_table_catalog = endpoint_config.catalog_name
inference_table_schema = endpoint_config.schema_name

# Cleanly formatted tables
assessment_table = f"{inference_table_catalog}.{inference_table_schema}.`{inference_table_name}_assessment_logs`"
request_table = f"{inference_table_catalog}.{inference_table_schema}.`{inference_table_name}_request_logs`"

# Note: you might have to wait a bit for the tables to be ready
print(f"Request logs: {request_table}")
requests_df = spark.table(request_table)
print(f"Assessment logs: {assessment_table}")
#Temporary helper to extract the table - see _resources/00-init-advanced 
assessment_df = deduplicate_assessments_table(assessment_table)

# COMMAND ----------

requests_with_feedback_df = requests_df.join(assessment_df, requests_df.databricks_request_id == assessment_df.request_id, "left")
display(requests_with_feedback_df.select("request_raw", "trace", "source", "text_assessment", "retrieval_assessments"))

# COMMAND ----------


requests_with_feedback_df.createOrReplaceTempView('latest_assessments')
eval_dataset = spark.sql(f"""
-- Thumbs up.  Use the model's generated response as the expected_response
select
  a.request_id,
  r.request,
  r.response as expected_response,
  'thumbs_up' as type,
  a.source.id as user_id
from
  latest_assessments as a
  join {request_table} as r on a.request_id = r.databricks_request_id
where
  a.text_assessment.ratings ["answer_correct"].value == "positive"
union all
  --Thumbs down.  If edited, use that as the expected_response.
select
  a.request_id,
  r.request,
  IF(
    a.text_assessment.suggested_output != "",
    a.text_assessment.suggested_output,
    NULL
  ) as expected_response,
  'thumbs_down' as type,
  a.source.id as user_id
from
  latest_assessments as a
  join {request_table} as r on a.request_id = r.databricks_request_id
where
  a.text_assessment.ratings ["answer_correct"].value = "negative"
union all
  -- No feedback.  Include the request, but no expected_response
select
  a.request_id,
  r.request,
  IF(
    a.text_assessment.suggested_output != "",
    a.text_assessment.suggested_output,
    NULL
  ) as expected_response,
  'no_feedback_provided' as type,
  a.source.id as user_id
from
  latest_assessments as a
  join {request_table} as r on a.request_id = r.databricks_request_id
where
  a.text_assessment.ratings ["answer_correct"].value != "negative"
  and a.text_assessment.ratings ["answer_correct"].value != "positive"
  """)
display(eval_dataset)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 1.2/ Our eval dataset is now ready! 
# MAGIC
# MAGIC The review app makes it easy to build & create your evaluation dataset. 
# MAGIC
# MAGIC *Note: the eval app logs may take some time to be available to you. If the dataset is empty, wait a bit.*
# MAGIC
# MAGIC To simplify the demo and make sure you don't have to craft your own eval dataset, we saved a ready-to-use eval dataset already pre-generated for you. We'll use this one for the demo instead.

# COMMAND ----------

eval_dataset = spark.table("eval_set_databricks_documentation").limit(10)
display(eval_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the correct Python environment for the model
# MAGIC

# COMMAND ----------

#Retrieve the model we want to eval
model = get_latest_model(MODEL_NAME_FQN)
pip_requirements = mlflow.pyfunc.get_model_dependencies(f"runs:/{model.run_id}/chain")

# COMMAND ----------

# MAGIC %pip install -r $pip_requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run our evaluation from the dataset!

# COMMAND ----------

with mlflow.start_run(run_name="eval_dataset_advanced"):
    # Evaluate the logged model
    eval_results = mlflow.evaluate(
        data=eval_dataset,
        model=f'runs:/{model.run_id}/chain',
        model_type="databricks-agent",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC You can open MLFlow and review the eval metrics, and also compare it to previous eval runs!
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/mlflow-eval.gif?raw=true" width="1200px"> 

# COMMAND ----------

# MAGIC %md
# MAGIC ### This is looking good, let's tag our model as production ready
# MAGIC
# MAGIC After reviewing the model correctness and potentially comparing its behavior to your other previous version, we can flag our model as ready to be deployed.
# MAGIC
# MAGIC *Note: Evaluation can be automated and part of a MLOps step: once you deploy a new Chatbot version with a new prompt, run the evaluation job and benchmark your model behavior vs the previous version.*

# COMMAND ----------

client = MlflowClient()
client.set_registered_model_alias(name=MODEL_NAME_FQN, alias="prod", version=model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC In a production setup, we would deploy another PROD model endpoint serving using this mode. To keep this demo simple, we will keep our previous endpoint for our next online evaluation step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step: track production online inferences with Databricks AI Gateway
# MAGIC
# MAGIC Mosaic AI Agent Evaluation makes it easy to evaluate your LLM Models, leveraging custom metrics.
# MAGIC
# MAGIC Evaluating your chatbot is key to measure your future version impact, and your Data Intelligence Platform makes it easy leveraging automated Workflow for your MLOps pipelines.
# MAGIC
# MAGIC Let's now review how to track our production model endpoint, tracking our users question through Databricks AI Gateway: [open 04-Online-Evaluation]($./04-Online-Evaluation)
# MAGIC
