# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Production Monitoring: Automated Quality at Scale
# MAGIC
# MAGIC MLflow's production monitoring automatically runs quality assessments on a sample of your production traffic, ensuring your GenAI app maintains high quality standards without manual intervention. MLflow lets you use the same metrics you defined for offline evaluation in production, enabling you to have consistent quality evaluation across your entire application lifecycle - dev to prod.
# MAGIC
# MAGIC **Key benefits:** 
# MAGIC
# MAGIC - Automated evaluation - Run LLM judges on production traces with configurable sampling rates
# MAGIC - Continuous quality assessment - Monitor quality metrics in real-time without disrupting user experience
# MAGIC - Cost-effective monitoring - Smart sampling strategies to balance coverage with computational cost
# MAGIC
# MAGIC Production monitoring enables you to deploy confidently, knowing that you will proactively detect issues so you can address them before they cause a major impact to your users.
# MAGIC
# MAGIC For more details on generative AI monitoring refer to the [Monitor served models using AI Gateway-enabled inference tables](https://docs.databricks.com/gcp/en/ai-gateway/inference-tables) and [Production quality monitoring](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/run-scorer-in-prod) documentation.
# MAGIC
# MAGIC <img src="https://i.imgur.com/wv4p562.gif">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=05.production-monitoring&demo_name=ai-agent&event=VIEW">

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow[databricks]>=3.1.1 databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Let's create our production grade monitor
# MAGIC
# MAGIC You can easily create your monitor using the UI, or directly the SDK:
# MAGIC

# COMMAND ----------

from databricks.agents.monitoring import (
  AssessmentsSuiteConfig,
  GuidelinesJudge,
  create_external_monitor,
  get_external_monitor,
  update_external_monitor,
  BuiltinJudge
)
import mlflow

# Let's re-use an existing experiment
xp_name = os.getcwd().rsplit("/", 1)[0]+"/03-knowledge-base-rag/03.1-pdf-rag-tool"
mlflow.set_experiment(xp_name)

accuracy_guidelines = [
  """
  The response correctly references all factual information from the provided_info based on these rules:
    - All factual information must be directly sourced from the provided data with NO fabrication
    - Names, dates, numbers, and company details must be 100% accurate with no errors
    - Meeting discussions must be summarized with the exact same sentiment and priority as presented in the data
    - Support ticket information must include correct ticket IDs, status, and resolution details when available
    - All product usage statistics must be presented with the same metrics provided in the data
    - No references to CloudFlow features, services, or offerings unless specifically mentioned in the customer data
    - AUTOMATIC FAIL if any information is mentioned that is not explicitly provided in the data
  """,
]

steps_and_reasoning_guildelines = [
  """
  Reponse must be done without showing reasoning.
    - don't mention that you need to look up things
    - do not mention tools or function used
    - do not tell your intermediate steps or reasoning
  """,
]

assessments = [
  # Builtin judges
  BuiltinJudge(name="safety"),
  BuiltinJudge(name="groundedness", sample_rate=0.4),
  BuiltinJudge(name="relevance_to_query"),
  # Guidelines can refer to the request and response.
  GuidelinesJudge(guidelines={
    'accuracy': accuracy_guidelines,
    'steps_and_reasoning': steps_and_reasoning_guildelines
  })
]

# COMMAND ----------

def get_or_create_monitor():
  try:
    external_monitor = get_external_monitor(experiment_name=xp_name)
    print(f"Monitor already exists: {external_monitor}, updating it")

    external_monitor = update_external_monitor(
      experiment_name=xp_name,
      assessments_config=AssessmentsSuiteConfig(
          sample=1.0,  # sampling rate
          assessments=assessments
        ),
    )
    print(f"Monitor updated: {external_monitor}")

  except Exception as e:
    if "does not exist" in str(e):
      # Create external monitor for automated production monitoring
      external_monitor = create_external_monitor(
        # Change to a Unity Catalog schema where you have CREATE TABLE permissions.
        catalog_name=catalog,
        schema_name=dbName,
        assessments_config=AssessmentsSuiteConfig(
          sample=1.0,  # sampling rate
          assessments=assessments
        )
      )
  print(f"Monitor created: {external_monitor}")

# COMMAND ----------

# monitor will create a run that will be refreshed periodically (small cost incures). 
# uncomment to create the monitor in your experiment!
get_or_create_monitor()

# COMMAND ----------

# MAGIC %md
# MAGIC The monitoring job will take ~15 - 30 minutes to run for the first time. After the initial run, it runs every 15 minutes. Note that if you have a large volume of production traffic, the job can take additional time to complete.
# MAGIC
# MAGIC Each time the job runs, it:
# MAGIC
# MAGIC 1. Runs each configured scorer on the sample of traces
# MAGIC   If you have different sampling rates per scorer, the monitoring job attempts to score as many of the same traces as possible. For example, if scorer A has a 20% sampling rate and scorer B has a 40% sampling rate, the same 20% of traces will be used for A and B.
# MAGIC 2. Attaches the feedback from the scorer to each trace in the specified MLflow Experiment
# MAGIC 3. Writes a copy of ALL traces (not just the ones sampled) to the Delta Table named `trace_logs_<MLflow_experiment_id>`    
# MAGIC   You can view the monitoring results using the Trace tab in the MLflow Experiment. Alternatively, you can query the traces using SQL or Spark in the generated Delta Table.
