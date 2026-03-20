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
# MAGIC For more details on generative AI monitoring refer to the [Monitor served models using AI Gateway-enabled inference tables](https://docs.databricks.com/aws/en/ai-gateway/inference-tables) and [Production quality monitoring](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/production-monitoring) documentation.
# MAGIC
# MAGIC <img src="https://i.imgur.com/wv4p562.gif">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=05-production-monitoring/05.production-monitoring&demo_name=ai-agent&event=VIEW&path=/_dbdemos/data-science/ai-agent/05-production-monitoring/05.production-monitoring&version=1&user_hash=e0e2ee33e235fc2614124e893f8db907189efe896e2a7f02a1934da9e2ba72ab">

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow[databricks]>=3.10.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#mlflow[databricks]>=3.1.4 
#mlflow[databricks]==3.7.0
#databricks-agents

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Let's create our production grade monitor
# MAGIC
# MAGIC You can easily create your monitor using the UI, or directly using the SDK:
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Choose the right type of scorer depending on how much customization and control you need. Each approach builds on the previous one, adding more complexity and control. Start with [built-in judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/scorers) and [Guidelines judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/judges/guidelines) for quick evaluation. As your needs evolve, build [custom LLM judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-judge/create-custom-judge) for domain-specific criteria and create [custom code-based scorers](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-scorers) for deterministic business logic.
# MAGIC
# MAGIC | Approach                | Level of customization | Use Case                                                                                 |
# MAGIC |-------------------------|-----------------------|------------------------------------------------------------------------------------------|
# MAGIC | Built-in judges         | Minimal                  | Quickly try LLM evaluation with built-in scorers such as `Safety` and `RetrievalGroundedness`.                            |
# MAGIC | Guidelines judges       | Moderate                | A built-in judge that checks whether responses pass or fail custom natural-language rules, such as style or factuality guidelines.                             |
# MAGIC | Custom LLM judges       | Full                  | Create fully customized LLM judges with detailed evaluation criteria.<br><br>Capable of returning numerical scores, categories, or boolean values.                              |
# MAGIC | Custom code-based scorers | Full            | Programmatic and deterministic scorers that evaluate things like exact matching, format validation, and performance metrics. |
# MAGIC
# MAGIC This demo shows the usage of Bult-in judges, Guidelines judges and custom LLM judges. 

# COMMAND ----------

from mlflow.genai.scorers import RetrievalGroundedness, RelevanceToQuery, Safety
import mlflow

# Let's re-use an existing experiment
xp_name = os.getcwd().rsplit("/", 1)[0]+"/02-agent-eval/02.1_agent_evaluation"
mlflow.set_experiment(xp_name)

safety_judge = Safety().register(name="safety")
groundedness_judge = RetrievalGroundedness().register(name="groundedness")
relevance_judge = RelevanceToQuery().register(name="relevance_to_query")

# COMMAND ----------

# MAGIC %md
# MAGIC [Guidelines LLM judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/judges/guidelines#1-built-in-guidelines-judge-global-guidelines) use pass/fail natural language criteria to evaluate GenAI outputs.
# MAGIC A `Guidelines()`judge applies global guidelines uniformly to all rows, evaluating app inputs/outputs only. Works in both offline evaluation and production monitoring.
# MAGIC <br><br>Guidelines judges use a specially-tuned LLM to evaluate whether text meets your specified criteria. The judge:
# MAGIC * **Receives context:** Any JSON dictionary containing the data to evaluate (e.g., request, response, retrieved_documents, user_preferences). You can reference these keys by name directly in your guidelines
# MAGIC * **Applies guidelines:** Your natural language rules defining pass/fail conditions
# MAGIC * **Makes judgment:** Returns a binary pass/fail score with detailed rationale

# COMMAND ----------

from mlflow.genai.scorers import Guidelines

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

accuracy_judge = Guidelines(name="accuracy_guidelines", guidelines=accuracy_guidelines).register(name="accuracy")


# COMMAND ----------

# MAGIC %md
# MAGIC [Custom LLM judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-judge/) let you define complex and nuanced scoring guidelines for GenAI applications using natural language.
# MAGIC
# MAGIC While MLflow [built-in LLM judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/scorers) offer excellent starting points for common quality dimensions, custom judges created using `make_judge()` give you full control over evaluation criteria.
# MAGIC
# MAGIC To create a judge, you provide a prompt with natural language instructions on how to assess the quality of your agent. `make_judge()` accepts template variables to access the agent's inputs, outputs, expected outputs or behaviors, and even complete traces.
# MAGIC
# MAGIC Your instructions must include at least one template variable, but you don't need to use all of them.
# MAGIC
# MAGIC * `{{ inputs }}` - Input data provided to the agent
# MAGIC * `{{ outputs }}` - Output data generated by your agent
# MAGIC * `{{ expectations }}` - Ground truths or expected outcomes
# MAGIC * `{{ trace }}` - The complete execution trace of your agent
# MAGIC
# MAGIC These are the only variables allowed. Custom variables like `{{ question }}` will throw validation errors in order to ensure consistent behavior and prevent template injection issues.

# COMMAND ----------

from mlflow.genai import make_judge

steps_and_reasoning_judge = make_judge(
  name="steps_and_reasoning_validator",
  instructions=(
    "Analyze the agent's response: {{ outputs }} to verify whether it contains steps from reasiong.\n"
    "Reponse must be done without showing reasoning.\n"
    "  - don't mention that you need to look up things\n"
    "  - do not mention tools or function used\n"
    "  - do not tell your intermediate steps or reasoning\n"
    "Rate as true: the response does not contain any steps or reasoning\n"
    "Rate as false: the response contains steps or reasoning\n"
    "Your response must be a boolean: true or false"
  ),
  feedback_value_type=bool,
)

steps_and_reasoning_judge = steps_and_reasoning_judge.register(name="steps_and_reasoning")

# COMMAND ----------

# MAGIC %md
# MAGIC Trace-based judges analyze execution traces to understand what happened during agent execution. They autonomously explore traces using Model Context Protocol (MCP) tools and can:
# MAGIC
# MAGIC * Validate tool usage patterns
# MAGIC * Identify performance bottlenecks
# MAGIC * Investigate execution failures
# MAGIC * Verify multi-step workflows
# MAGIC
# MAGIC The following example defines a judge that assesses tool calling correctness by analyzing traces:

# COMMAND ----------

# Create a trace-based judge that validates tool calls from the trace
tool_call_judge = make_judge(
    name="tool_call_correctness",
    instructions=(
        "Analyze the execution {{ trace }} to determine if the agent called appropriate tools for the user's request.\n"
        "Examine the trace to:\n"
        "1. Identify what tools were available and their purposes\n"
        "2. Determine which tools were actually called\n"
        "3. Assess whether the tool calls were reasonable for addressing the user's question\n"
        "Evaluate the tool usage and respond with a boolean value:\n"
        "- true: The agent called the right tools to address the user's request\n"
        "- false: The agent called wrong tools, missed necessary tools, or called unnecessary tools\n"
        "Your response must be a boolean: true or false."
    ),
    feedback_value_type=bool,
    # To analyze a full trace with a trace-based judge, a model must be specified
    model="databricks:/databricks-gpt-5-1"
    )

tool_call_judge = tool_call_judge.register(name="tool_call_correctness")

# COMMAND ----------

# starting production monitoring will create a job that will be refreshed periodically (small cost incures)
# uncomment to start production monitoring on your experiment!
from mlflow.genai.scorers import list_scorers, ScorerSamplingConfig
scorers = list_scorers()
# for scorer in scorers:
#   scorer.start(sampling_config=ScorerSamplingConfig(sample_rate=1.0)) # check all traces

# COMMAND ----------

# uncomment to stop and delete all scorers from the experiment!
from mlflow.genai.scorers import list_scorers, delete_scorer
import mlflow
import os

xp_name = os.getcwd().rsplit("/", 1)[0]+"/02-agent-eval/02.1_agent_evaluation"
mlflow.set_experiment(xp_name)
scorers = list_scorers()
# for scorer in scorers:
#   scorer.stop()
#   delete_scorer(name=scorer.name)

# COMMAND ----------

#Manually delete the production monitoring job under Jobs & Pipelines 

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
