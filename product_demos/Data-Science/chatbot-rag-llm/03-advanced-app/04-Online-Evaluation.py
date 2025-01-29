# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Monitoring your chatbot behavior over time!
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/online-monitoring-dashboard.gif" style="float:right" width="750px" />
# MAGIC
# MAGIC In the previous notebook, we have investigated the evaluation input to create an evaluation dataset and benchmark newer version against it.
# MAGIC
# MAGIC Because Databricks also tracks all your customer discussions, we can also deploy online monitoring, directly reviewing where your model doesn't perform well and trigger alerts and ask for your expert to improve your dataset!
# MAGIC
# MAGIC This is also a good opportunity to either improve your documentation or adjust your prompt, and ultimately add the correct answer to your evaluation dataset!
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=04-Online-Evaluation&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install -qqq databricks-agents==0.15.0 mlflow[databricks]==2.20.0 databricks-sdk==0.38.0 mlflow==2.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init-advanced $reset_all_data=false

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(name="endpoint_name", defaultValue=f"agents_{catalog}-{db}-rag_demo_advanced", label="1. Agent's Model serving endpoint name")

dbutils.widgets.text(name="topics", defaultValue='''"spark", "AI", "DBSQL","other"''', label="""3. List of topics in which to categorize the input requests. Format must be comma-separated strings in double quotes. We recommend having an "other" category as a catch-all.""")

dbutils.widgets.text(name="sample_rate", defaultValue="0.3", label="2. Sampling rate between 0.0 and 1.0: on what % of requests should the LLM judge quality analysis run?")

local_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
local_path = '/'.join(local_path.split('/')[:-1])
dbutils.widgets.text(name="workspace_folder", defaultValue=local_path, label="4. Folder to create dashboard.")

endpoint_name = dbutils.widgets.get("endpoint_name")
from databricks.sdk import WorkspaceClient
wc = WorkspaceClient()
ep = wc.serving_endpoints.get(endpoint_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start by protecting your endpoint: setting up AI Guardrails
# MAGIC
# MAGIC ### Set invalid keywords
# MAGIC
# MAGIC You can investigate the inference table to see whether the endpoint is being used for inappropriate topics. From the inference table, it looks like a user is talking about SuperSecretProject! For this example, you can assume that topic is not in the scope of use for this chat endpoint.
# MAGIC
# MAGIC ### Set up PII detection
# MAGIC
# MAGIC Now, the endpoint blocks messages referencing SuperSecretProject. You can also make sure the endpoint doesn't accept requests with or respond with messages containing any PII.
# MAGIC
# MAGIC The following updates the guardrails configuration for pii:

# COMMAND ----------

from databricks.sdk.service.serving import AiGatewayGuardrails, AiGatewayRateLimit,AiGatewayRateLimitRenewalPeriod, AiGatewayUsageTrackingConfig

guardrails = AiGatewayGuardrails.from_dict({
    "input": {
        "pii": {"behavior": "BLOCK"},
        "invalid_keywords": ["SuperSecretProject"]
    },
    "output": {
        "pii": {"behavior": "BLOCK"}
    }
})

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add rate limits
# MAGIC
# MAGIC Say you are investigating the inference tables further and you see some steep spikes in usage suggesting a higher-than-expected volume of queries. Extremely high usage could be costly if not monitored and limited.

# COMMAND ----------

rate_limits = AiGatewayRateLimit(calls=100,
                                renewal_period=AiGatewayRateLimitRenewalPeriod.MINUTE)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC Let's enable the AI gateway:
# MAGIC
# MAGIC <div style="background-color: #d4f8d4; border-radius: 15px; padding: 20px; text-align: center;">
# MAGIC         Note: AI gateway might not be enable in all workspaces, please reach out your account team if needed!
# MAGIC     </div>

# COMMAND ----------

wc.serving_endpoints.put_ai_gateway(endpoint_name, 
                                    guardrails=guardrails, 
                                    rate_limits=[rate_limits],
                                    usage_tracking_config=AiGatewayUsageTrackingConfig(enabled=True))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Let's try our endpoint
# MAGIC
# MAGIC Say you are investigating the inference tables further and you see some steep spikes in usage suggesting a higher-than-expected volume of queries. Extremely high usage could be costly if not monitored and limited.

# COMMAND ----------

from mlflow.deployments import get_deploy_client
deploy_client = get_deploy_client("databricks")

for r in spark.table("eval_set_databricks_documentation").limit(10).collect():
    print(f"Simulating traffic: {r['request']}")
    df = pd.DataFrame({'messages': [[{'content': r['request'], 'role': 'user'}]]})
    result = deploy_client.predict(endpoint="agents_main__build-dbdemos_rag_chatbot-rag_demo_advanced", inputs={"dataframe_split": df.to_dict(orient='split')})
    print(f"Model answer : {result['predictions'][0]['choices'][0]['message']}")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building custom Dashboard to track your online model
# MAGIC
# MAGIC Now that our model is in production through our AI gateway, we can add custom dashboard to track its behavior.
# MAGIC
# MAGIC Here is a script example that you can run to build and refresh an evaluation dashboard.
# MAGIC
# MAGIC These next cells are inspired from Databricks Documentation: https://docs.databricks.com/en/generative-ai/agent-evaluation/evaluating-production-traffic.html
# MAGIC
# MAGIC **We strongly recommend checking the full documentation for more details and up to date dashboard!**

# COMMAND ----------

# DBTITLE 1,helper function to prepare our dashboard
import pandas as pd
from typing import List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from IPython.display import display_markdown
from mlflow.utils import databricks_utils as du

wc = WorkspaceClient()

def get_payload_table_name(endpoint_info) -> str:
  catalog_name = endpoint_info.config.auto_capture_config.catalog_name
  schema_name = endpoint_info.config.auto_capture_config.schema_name
  payload_table_name = endpoint_info.config.auto_capture_config.state.payload_table.name
  return f"{catalog_name}.{schema_name}.{payload_table_name}"

def get_model_name(endpoint_info) -> str:
  served_models = [
    model for model in endpoint_info.config.served_models if not model.model_name.endswith(".feedback")
  ]
  return served_models[0].model_name

# Helper function for display Delta Table URLs
def get_table_url(table_fqdn: str) -> str:
    table_fqdn = table_fqdn.replace("`", "")
    split = table_fqdn.split(".")
    browser_url = du.get_browser_hostname()
    url = f"https://{browser_url}/explore/data/{split[0]}/{split[1]}/{split[2]}"
    return url

# Helper function to split the evaluation dataframe by hour
def split_df_by_hour(df: pd.DataFrame, max_samples_per_hours: int) -> List[pd.DataFrame]:
    df['hour'] = pd.to_datetime(df["timestamp"]).dt.floor('H')
    dfs_by_hour = [
        group.sample(min(len(group), max_samples_per_hours), replace=False)
        for _, group in df.groupby('hour')
    ]
    return dfs_by_hour

# COMMAND ----------

# DBTITLE 1,Read parameters
sample_rate = float(dbutils.widgets.get("sample_rate"))
# Verify that sample_rate is a number in [0,1]
assert(0.0 <= sample_rate and sample_rate <= 1.0), 'Please specify a sample rate between 0.0 and 1.0'

sample_rate_display = f"{sample_rate:0.0%}"

workspace_folder = dbutils.widgets.get("workspace_folder").replace("/Workspace", "")
if workspace_folder is None or workspace_folder == "":
  username = spark.sql("select current_user() as username").collect()[0]["username"]
  workspace_folder=f"/Users/{username}"

folder_info = wc.workspace.get_status(workspace_folder)
assert (folder_info is not None and folder_info.object_type == workspace.ObjectType.DIRECTORY), f"Please specify a valid workspace folder. The specified folder {workspace_folder} is invalid."

topics = dbutils.widgets.get("topics")

# Print debugging information
display_markdown("## Monitoring notebook configuration", raw=True)
display_markdown(f"- **Agent Model Serving endpoint name:** `{endpoint_name}`", raw=True)
display_markdown(f"- **% of requests that will be run through LLM judge quality analysis:** `{sample_rate_display}`", raw=True)
display_markdown(f"- **Storing output artifacts in:** `{workspace_folder}`", raw=True)
display_markdown(f"- **Topics to detect:** `{topics}`", raw=True)

# COMMAND ----------

# DBTITLE 1,Set up table-name variables
def escape_table_name(table_name: str) -> str:
  return ".".join(list(map(lambda x: f"`{x}`", table_name.split("."))))

# Deployed agents create multiple inference tables that can be used for further processing such as evaluation. See the documentation:
# AWS documentation: https://docs.databricks.com/en/generative-ai/deploy-agent.html#agent-enhanced-inference-tables
# Azure documentation: https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent#agent-enhanced-inference-tables
endpoint_info = wc.serving_endpoints.get(endpoint_name)
inference_table_name = get_payload_table_name(endpoint_info)
fully_qualified_uc_model_name = get_model_name(endpoint_info)

requests_log_table_name = f"{inference_table_name}_request_logs"
eval_requests_log_table_name = escape_table_name(f"{requests_log_table_name}_eval")
assessment_log_table_name = escape_table_name(f"{inference_table_name}_assessment_logs")

eval_requests_log_checkpoint = f"{requests_log_table_name}_eval_checkpoint"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {escape_table_name(eval_requests_log_checkpoint)}")
eval_requests_log_checkpoint_path = f"/Volumes/{eval_requests_log_checkpoint.replace('.', '/')}"

# Print debugging information
display_markdown("## Input tables", raw=True)
display_markdown(
  f"- **Inference table:** [{inference_table_name}]({get_table_url(inference_table_name)})",
  raw=True
)
display_markdown(
  f"- **Request logs table:** [{requests_log_table_name}]({get_table_url(requests_log_table_name)})",
  raw=True
)
display_markdown(
  f'- **Human feedback logs table:** [{assessment_log_table_name.replace("`", "")}]({get_table_url(assessment_log_table_name.replace("`", ""))})',
  raw=True
)
display_markdown("## Output tables/volumes", raw=True)
display_markdown(
  f'- **LLM judge results table:** [{eval_requests_log_table_name.replace("`", "")}]({get_table_url(eval_requests_log_table_name.replace("`", ""))})',
  raw=True
)
display_markdown(f"- **Streaming checkpoints volume:** `{eval_requests_log_checkpoint_path}`", raw=True)

# COMMAND ----------

# DBTITLE 1,Initialize mlflow experiment
import mlflow
mlflow_client = mlflow.MlflowClient()

# Create a single experiment to store all monitoring runs for this endpoint
experiment_name = f"{workspace_folder}/{inference_table_name}_experiment"
experiment = mlflow_client.get_experiment_by_name(experiment_name)
experiment_id = mlflow_client.create_experiment(experiment_name) if experiment is None else experiment.experiment_id

mlflow.set_experiment(experiment_name=experiment_name)

# COMMAND ----------

# DBTITLE 1,Update the table with unprocessed requests
import pyspark.sql.functions as F

# Streams any unprocessed rows from the requests log table into the evaluation requests log table.
# Unprocessed requests have an empty run_id.
# Processed requests have one of the following values: "skipped", "to_process", or a valid run_id.
(
  spark.readStream.format("delta")
  .table(escape_table_name(requests_log_table_name))
  .withColumn("run_id", F.lit(None).cast("string"))
  .withColumn(
    "retrieval/llm_judged/chunk_relevance/precision", F.lit(None).cast("double")
  )
  .writeStream.option("checkpointLocation", eval_requests_log_checkpoint_path)
  .option("mergeSchema", "true")
  .format("delta")
  .outputMode("append")
  .trigger(availableNow=True)
  .toTable(eval_requests_log_table_name)
  .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,Mark rows for processing, mark the rest as "skipped"
spark.sql(f"""
          WITH sampled_requests AS (
            -- Sample unprocessed requests
            SELECT *
            FROM (
              SELECT databricks_request_id
              FROM {eval_requests_log_table_name}
              WHERE 
                run_id IS NULL
                AND `timestamp` >= CURRENT_TIMESTAMP() - INTERVAL 30 DAY
            ) TABLESAMPLE ({sample_rate*100} PERCENT)
          ), requests_with_user_feedback AS (
            -- Get unprocessed requests with user feedback
            SELECT assessments.request_id
            FROM {assessment_log_table_name} assessments
            INNER JOIN {eval_requests_log_table_name} requests
            ON assessments.request_id = requests.databricks_request_id
            WHERE 
              requests.run_id is NULL
              AND assessments.`timestamp` >= CURRENT_TIMESTAMP() - INTERVAL 30 DAY
          )

          -- Mark the selected rows as `to_process`
          UPDATE {eval_requests_log_table_name} 
          SET run_id="to_process"
          WHERE databricks_request_id IN (
            SELECT * FROM sampled_requests
            UNION 
            SELECT * FROM requests_with_user_feedback
          )
          """)

###############
# CONFIG: Add custom logic here to select more rows. Update the run_id of selected rows to the value "to_process".
###############

spark.sql(f"""
          UPDATE {eval_requests_log_table_name}
          SET run_id="skipped"
          WHERE run_id IS NULL
          """)

# COMMAND ----------

# DBTITLE 1,Run evaluate on unprocessed rows
from databricks.rag_eval import env_vars

eval_df = spark.sql(f"""
                    SELECT 
                      `timestamp`,
                      databricks_request_id as request_id, 
                      from_json(request_raw, 'STRUCT<messages ARRAY<STRUCT<role STRING, content STRING>>>') AS request,
                      trace
                    FROM {eval_requests_log_table_name} 
                    WHERE run_id="to_process"                  
                    """)

eval_pdf = eval_df.toPandas().drop_duplicates(subset=["request_id"])

if eval_pdf.empty:
  print("[Warning] No new rows to process.")
else:
  with mlflow.start_run() as run:
    ###############
    # CONFIG: Adjust mlflow.evaluate(...) to change which Databricks LLM judges are run. By default, judges that do not require ground truths
    # are run, including groundedness, safety, chunk relevance, and relevance to query. For more details, see the documentation:
    # AWS documentation: https://docs.databricks.com/en/generative-ai/agent-evaluation/advanced-agent-eval.html#evaluate-agents-using-a-subset-of-llm-judges
    # Azure documentation: https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-evaluation/advanced-agent-eval#evaluate-agents-using-a-subset-of-llm-judges
    ###############
    for eval_pdf_batch in split_df_by_hour(eval_pdf, max_samples_per_hours=env_vars.RAG_EVAL_MAX_INPUT_ROWS.get()):
      eval_results = mlflow.evaluate(data=eval_pdf_batch, model_type="databricks-agent")

      results_df = (
        spark
        .createDataFrame(eval_results.tables['eval_results'])
        .withColumn("databricks_request_id", F.col("request_id"))
        .withColumn("run_id", F.lit(run.info.run_id).cast("string"))
        .withColumn("experiment_id", F.lit(experiment_id).cast("string"))
        .withColumn("topic", F.lit(None).cast("string"))
        .drop("request_id")
        .drop("request")
        .drop("response")
        .drop("trace")
      )

      results_df.createOrReplaceTempView("updates")

      spark.sql(f"""
                merge with schema evolution into {eval_requests_log_table_name} evals 
                using updates ON evals.databricks_request_id=updates.databricks_request_id 
                WHEN MATCHED THEN UPDATE SET *
                """)

# COMMAND ----------

# DBTITLE 1,Perform topic detection
# Perform topic detection using the `ai_classify` function. For more details, see the documentation:
# AWS documentation: https://docs.databricks.com/en/sql/language-manual/functions/ai_classify.html
# Azure documentation: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_classify
if not eval_pdf.empty:   
  if not len(topics.strip()) or topics == "\"other\"":
    spark.sql(f"""
              merge with schema evolution into {eval_requests_log_table_name} evals 
              using updates ON evals.databricks_request_id=updates.databricks_request_id 
              WHEN MATCHED THEN UPDATE SET topic="other"
              """)
  else:          
    spark.sql(f"""
              merge with schema evolution into {eval_requests_log_table_name} evals 
              using updates ON evals.databricks_request_id=updates.databricks_request_id 
              WHEN MATCHED THEN UPDATE SET topic=ai_classify(request, ARRAY({topics}))
              """)

# COMMAND ----------

# DBTITLE 1,Load the dashboard template
import requests

dashboard_template_url = 'https://raw.githubusercontent.com/databricks/genai-cookbook/main/rag_app_sample_code/resources/agent_quality_online_monitoring_dashboard_template.json'
response = requests.get(dashboard_template_url)

if response.status_code == 200:
  dashboard_template = str(response.text)
else:
  raise Exception("Failed to get the dashboard template. Please try again or download the template directly from the URL.")    

# COMMAND ----------

# DBTITLE 1,Create or get the dashboard
from databricks.sdk import WorkspaceClient
from databricks.sdk import errors

wc = WorkspaceClient()
object_info = None

num_evaluated_requests = spark.sql(f"select count(*) as num_rows from {eval_requests_log_table_name}").collect()[0].num_rows
if not num_evaluated_requests:
  print("There are no evaluated requests! Skipping dashboard creation.")
else:
  dashboard_name = f"Monitoring dashboard for {fully_qualified_uc_model_name}"
  try:
    dashboard_content = (
      dashboard_template
        .replace("{{inference_table_name}}", inference_table_name)
        .replace("{{eval_requests_log_table_name}}", eval_requests_log_table_name)
        .replace("{{assessment_log_table_name}}", assessment_log_table_name)
        .replace("{{fully_qualified_uc_model_name}}", fully_qualified_uc_model_name)
        .replace("{{sample_rate}}", sample_rate_display)
    )
    object_info = wc.workspace.get_status(path=f"{workspace_folder}/{dashboard_name}.lvdash.json")
    dashboard_id = object_info.resource_id
  except errors.platform.ResourceDoesNotExist as e:
    dashboard_info = wc.lakeview.create(display_name=dashboard_name, serialized_dashboard=dashboard_content, parent_path=workspace_folder)
    dashboard_id = dashboard_info.dashboard_id
  
  displayHTML(f"""<a href="sql/dashboardsv3/{dashboard_id}">Dashboard</a>""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it!
# MAGIC
# MAGIC Our model is now deployed in production, and you can periodically run these last cells to refresh your monitoring dashboard and track its behavior in production.
