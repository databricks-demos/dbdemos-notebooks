# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluating your Fine Tune LLM 
# MAGIC
# MAGIC In the previous notebook, we saw how Databricks simplifies LLM Fine-tuning.
# MAGIC
# MAGIC While fine-tuning is a simple API call, you need to evaluate how your new LLM behave. This is critical to assess if the fine tuning helped in the right way, and also key to understanding how you can improve your fine-tuning dataset and detect potential gaps.
# MAGIC
# MAGIC
# MAGIC Databricks leverages MLFlow and its new LLM capabilities. We will be comparing the plain/base Foundation Model versus the fine tuned one across different metrics.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-eval-judge.png?raw=true" width="1200px">
# MAGIC
# MAGIC To do so, we need to create an evaluation dataset and have our RAG application call the 2 models we want to benchmark: a baseline vs the fine-tuned one. This will let us appreciate how Fine Tuning improved our model performance!

# COMMAND ----------

# Order is important here. Install MLflow last.
%pip install textstat==0.7.3 databricks-genai==1.0.2 openai==1.30.1 langchain==0.2.0 langchain-community==0.2.0 langchain_text_splitters==0.2.0 markdown==3.6
%pip install databricks-sdk==0.27.1
%pip install "transformers==4.37.1" "mlflow==2.12.2"
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Evaluation with MLFlow
# MAGIC
# MAGIC This example shows how you can use LangChain in conjunction with MLflow evaluate with custom System Prompt.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Establish Baseline
# MAGIC We will first establish baseline performance using the standard LLM. If the LLM you fine-tune is available as Foundation Model, you can use the API provided by Databricks directly.
# MAGIC
# MAGIC Because we fine-tuned on mistral or llama2-7B, we will deploy a Serving endpoint using this model, making sure it scales to zero to avoid costs.

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput, AutoCaptureConfigInput

serving_endpoint_baseline_name = "dbdemos_llm_not_fine_tuned"
w = WorkspaceClient()
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_baseline_name,
    served_entities=[
        ServedEntityInput(
            entity_name="system.ai.mistral_7b_instruct_v0_2", #Make sure you're using the same base model as the one you're fine-tuning on for relevant evaluation!
            entity_version=1,
            min_provisioned_throughput=0, # The minimum tokens per second that the endpoint can scale down to.
            max_provisioned_throughput=100,# The maximum tokens per second that the endpoint can scale up to.
            scale_to_zero_enabled=True
        )
    ]
)

existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_baseline_name), None
)
if existing_endpoint == None:
    print(f"Creating the endpoint {serving_endpoint_baseline_name}, this will take a few minutes to package and deploy the LLM...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_baseline_name, config=endpoint_config)
else:
  print(f"endpoint {serving_endpoint_baseline_name} already exist")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Trying a simple, manual query to see the difference between the 2 endpoints

# COMMAND ----------

import mlflow
from mlflow import deployments

question = "How can I find my account ID?"
inputs = {"messages": [{"role": "user", "content": question}], "max_tokens": 400}

client = mlflow.deployments.get_deploy_client("databricks")
not_fine_tuned_answer = client.predict(endpoint=serving_endpoint_baseline_name, inputs=inputs)
display_answer(not_fine_tuned_answer)

# COMMAND ----------

serving_endpoint_ft_name = "dbdemos_llm_fine_tuned"
fine_tuned_answer = client.predict(endpoint=serving_endpoint_ft_name, inputs={"messages": [{"role": "user", "content": question}]})
display_answer(fine_tuned_answer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Offline Model Evaluation
# MAGIC
# MAGIC We'll be using [mlflow.evaluate](https://mlflow.org/docs/latest/llms/llm-evaluate/notebooks/huggingface-evaluation.html?highlight=mlflow%20evaluate%20llm) to see how our model is performing, using what an external LLM as a judge.
# MAGIC
# MAGIC The idea is to send our questions + context to the LLM, and then compare its answer with the expected one leveraging an external model (usually stronger). In our case, we'll ask the built-in, serverless DBRX model endpoint to judge the Mistral LLMs answer compared to the ground truth.

# COMMAND ----------

# DBTITLE 1,Build an eval dataset
eval_dataset = spark.table("chat_completion_evaluation_dataset").withColumnRenamed("content", "context").toPandas()
display(eval_dataset)

# COMMAND ----------

from langchain_community.chat_models.databricks import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts.chat import ChatPromptTemplate
import pandas as pd


base_model_name = "mistralai/Mistral-7B-Instruct-v0.2"

# ----------------------------------------------------------------------------------------------------------------------------------------- #
# -- Basic chain for the demo. This should instead be your full, real RAG chain (you want to evaluate your LLM on your final chain) ------- #
# ----------------------------------------------------------------------------------------------------------------------------------------- #
system_prompt = """You are a highly knowledgeable and professional Databricks Support Agent. Your goal is to assist users with their questions and issues related to Databricks. Answer questions as precisely and accurately as possible, providing clear and concise information. If you do not know the answer, respond with "I don't know." Be polite and professional in your responses. Provide accurate and detailed information related to Databricks. If the question is unclear, ask for clarification.\n"""

user_input = "Here is a documentation page that could be relevant: {context}. Based on this, answer the following question: {question}"

def build_chain(llm):
    #mistral doesn't support the system role
    if "mistral" in base_model_name:
        messages = [("user", f"{system_prompt} \n{user_input}")]
    else:
        messages = [("system", system_prompt),
                    ("user", user_input)]
    return ChatPromptTemplate.from_messages(messages) | llm | StrOutputParser()
# --------------------------------------------------------------------------------------------------- #

def eval_llm(llm_endoint_name, eval_dataset, llm_judge = "databricks-dbrx-instruct", run_name="dbdemos_fine_tuning_rag"):
    #Build the chain. This should be your actual RAG chain, querying your index
    llm = ChatDatabricks(endpoint=llm_endoint_name, temperature=0.1)
    chain = build_chain(llm)
    #For each entry, call the endpoint
    eval_dataset["prediction"] = chain.with_retry(stop_after_attempt=2) \
                                      .batch(eval_dataset[["context", "question"]].to_dict(orient="records"), config={"max_concurrency": 4})

    #starts an mlflow run to evaluate the model
    with mlflow.start_run(run_name="eval_"+llm_endoint_name) as run:
        eval_df = eval_dataset.reset_index(drop=True).rename(columns={"question": "inputs"})
        results = mlflow.evaluate(
            data=eval_df,
            targets="answer",
            predictions="prediction",
            extra_metrics=[
                mlflow.metrics.genai.answer_similarity(model=f"endpoints:/{llm_judge}"),
                mlflow.metrics.genai.answer_correctness(model=f"endpoints:/{llm_judge}")
            ],
            evaluators="default",
        )
        return results
    
#Evaluate the base foundation model
baseline_results = eval_llm(serving_endpoint_baseline_name, eval_dataset, llm_judge = "databricks-dbrx-instruct", run_name="dbdemos_fine_tuning_rag")
#Evaluate the fine tuned model
fine_tuned_results = eval_llm(serving_endpoint_ft_name, eval_dataset, llm_judge = "databricks-dbrx-instruct", run_name="dbdemos_fine_tuning_rag")

# COMMAND ----------

# DBTITLE 1,Cancel Run Llama Python
# MAGIC %md
# MAGIC ## 3/ Using MLFlow experiment UI to analyse our runs
# MAGIC
# MAGIC MLFlow will automatically collect the stats for us.
# MAGIC
# MAGIC You can now open the experiment and compare the 2 runs:
# MAGIC
# MAGIC * `eval_dbdemos_llm_not_fine_tuned` against the baseline model
# MAGIC * `eval_dbdemos_llm_fine_tuned` on your baseline model
# MAGIC
# MAGIC Here is an example. As you can see, our metrics got improved on the fine tuning model!
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-eval.png?raw=true">

# COMMAND ----------

# MAGIC %md 
# MAGIC You can also analyse individual queries and filter on the question with very incorrect answer, and improve your training dataset accordingly.
# MAGIC
# MAGIC If you have a bigger dataset, you can also programatically filter on the rows having bad answer, and ask an external model such as DBRX to summarize what is not working to give you insights on your training dataset at scale!

# COMMAND ----------

fine_tuned_results.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Fine tuning your LLM on Databricks is simple, no need to be an expert Data Scientist.
# MAGIC
# MAGIC With Mosaic ML Fine Tuning, you'll be able to unlock new use-cases, fine tuning OSS LLMs to learn your own language and behave the way you want.
# MAGIC It makes it easy to build any text-related tasks on top of your data: entity extraction, conversation style and more!
# MAGIC
# MAGIC
# MAGIC ## Next: Want to go deeper and explore another fine-tuning use-case? 
# MAGIC
# MAGIC See how to do Instruction Fine tuning to specialize your model on NER (Named Entity Recognition) with the notebook [instruction-fine-tuning/01-llm-instruction-drug-extraction-fine-tuning]($./instruction-fine-tuning/01-llm-instruction-drug-extraction-fine-tuning)
