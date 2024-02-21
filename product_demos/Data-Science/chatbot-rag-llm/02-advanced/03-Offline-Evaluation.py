# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 3/ Evaluating the RAG Chat Bot with LLMs-as-a-Judge for automated evaluation
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-llm-as-a-judge.png?raw=true" style="float: right" width="900px">
# MAGIC
# MAGIC Now that our RAG model is deployed, we aim to evaluate its predictions correctness.
# MAGIC
# MAGIC Evaluating LLMs can be challenging as existing benchmarks and metrics can not measure them comprehensively. Humans are often involved in these tasks (see [RLHF](https://en.wikipedia.org/wiki/Reinforcement_learning_from_human_feedback), but it doesn't scale well: humans are slow and expensive!
# MAGIC
# MAGIC ## Introducing LLM-as-a-Judge
# MAGIC
# MAGIC In this notebook, we'll automate the evaluation process with a trending approach in the LLM community: **LLMs-as-a-judge**.
# MAGIC
# MAGIC Faster and cheaper than human evaluation, LLM-as-a-Judge leverages an external agent who judges the generative model predictions given what is expected from it.
# MAGIC
# MAGIC Superior models are typically used for such evaluation (e.g. `llama2-70B` judges `llama2-7B`, or `GPT4` judges `llama2-70B`)
# MAGIC
# MAGIC We'll explore the new LLMs-as-a-judges evaluation methods introduced in MLflow 2.9, with its powerful `mlflow.evaluate()`API.+
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=advanced/02-Evaluate-RAG-Chatbot-Model&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.12.0 mlflow==2.10.1 textstat==0.7.3 tiktoken==0.5.1 evaluate==0.4.1 langchain==0.1.5 databricks-vectorsearch==0.22 transformers==4.30.2 torch==2.0.1 cloudpickle==2.2.1 pydantic==2.5.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init-advanced $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Creating an external model endpoint with Azure Open AI as a judge
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/create-external-endpoint.png?raw=true" style="float:right" width="500px" />
# MAGIC
# MAGIC Databricks Serving Endpoint can be of 3 types:
# MAGIC
# MAGIC - Your own models, deployed as an endpoint (a chatbot model, your custom fine tuned LLM)
# MAGIC - Fully managed, serverless Foundation Models (e.g. llama2, MPT...)
# MAGIC - An external Foundation Model (e.g. Azure OpenAI)
# MAGIC
# MAGIC Let's create a external model endpoint using Azure Open AI.
# MAGIC
# MAGIC Note that you'll need to change the values with your own Azure Open AI configuration. Alternatively, you can setup a connection to another provider like OpenAI.
# MAGIC
# MAGIC *Note: If you don't have an Azure OpenAI deployment, this demo will fallback to a Databricks managed llama 2 model. Evaluation won't be as good.* 

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
init_experiment_for_batch("chatbot-rag-llm-advanced", "simple")

# COMMAND ----------

from mlflow.deployments import get_deploy_client
deploy_client = get_deploy_client("databricks")

try:
    endpoint_name  = "dbdemos-azure-openai"
    deploy_client.create_endpoint(
        name=endpoint_name,
        config={
            "served_entities": [
                {
                    "name": endpoint_name,
                    "external_model": {
                        "name": "gpt-35-turbo",
                        "provider": "openai",
                        "task": "llm/v1/chat",
                        "openai_config": {
                            "openai_api_type": "azure",
                            "openai_api_key": "{{secrets/dbdemos/azure-openai}}", #Replace with your own azure open ai key
                            "openai_deployment_name": "dbdemo-gpt35",
                            "openai_api_base": "https://dbdemos-open-ai.openai.azure.com/",
                            "openai_api_version": "2023-05-15"
                        }
                    }
                }
            ]
        }
    )
except Exception as e:
    if 'RESOURCE_ALREADY_EXISTS' in str(e):
        print('Endpoint already exists')
    else:
        print(f"Couldn't create the external endpoint with Azure OpenAI: {e}. Will fallback to llama2-70-B as judge. Consider using a stronger model as a judge.")
        endpoint_name = "databricks-llama-2-70b-chat"

#Let's query our external model endpoint
answer_test = deploy_client.predict(endpoint=endpoint_name, inputs={"messages": [{"role": "user", "content": "What is Apache Spark?"}]})
answer_test['choices'][0]['message']['content']

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Offline LLM evaluation
# MAGIC
# MAGIC We will start with offline evaluation, scoring our model before its deployment. This requires a set of questions we want to ask to our model.
# MAGIC
# MAGIC In our case, we are fortunate enough to have a labeled training set (questions+answers)  with state-of-the-art technical answers from our Databricks support team. Let's leverage it so we can compare our RAG predictions and ground-truth answers in MLflow.
# MAGIC
# MAGIC **Note**: This is optional! We can benefit from the LLMs-as-a-Judge approach without ground-truth labels. This is typically the case if you want to evaluate "live" models answering any customer questions

# COMMAND ----------

volume_folder =  f"/Volumes/{catalog}/{db}/volume_databricks_documentation/evaluation_dataset"
#Load the eval dataset from the repository to our volume
upload_dataset_to_volume(volume_folder)

# COMMAND ----------

# DBTITLE 1,Preparing our evaluation dataset
spark.sql(f'''
CREATE OR REPLACE TABLE evaluation_dataset AS
  SELECT q.id, q.question, a.answer FROM parquet.`{volume_folder}/training_dataset_question.parquet` AS q
    LEFT JOIN parquet.`{volume_folder}/training_dataset_answer.parquet` AS a
      ON q.id = a.question_id ;''')

display(spark.table('evaluation_dataset'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Automated Evaluation of our chatbot model registered in Unity Catalog
# MAGIC
# MAGIC Let's retrieve the chatbot model we registered in Unity Catalog and predict answers for each questions in the evaluation set.

# COMMAND ----------

import mlflow
os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get("dbdemos", "rag_sp_token")
model_name = f"{catalog}.{db}.dbdemos_advanced_chatbot_model"
model_version_to_evaluate = get_latest_model_version(model_name)
mlflow.set_registry_uri("databricks-uc")
rag_model = mlflow.langchain.load_model(f"models:/{model_name}/{model_version_to_evaluate}")

@pandas_udf("string")
def predict_answer(questions):
    def answer_question(question):
        dialog = {"messages": [{"role": "user", "content": question}]}
        return rag_model.invoke(dialog)['result']
    return questions.apply(answer_question)

# COMMAND ----------

df_qa = (spark.read.table('evaluation_dataset')
                  .selectExpr('question as inputs', 'answer as targets')
                  .where("targets is not null")
                  .sample(fraction=0.005, seed=40)) #small sample for interactive demo

df_qa_with_preds = df_qa.withColumn('preds', predict_answer(col('inputs'))).cache()

display(df_qa_with_preds)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##LLMs-as-a-judge: automated LLM evaluation with out of the box and custom GenAI metrics
# MAGIC
# MAGIC MLflow 2.8 provides out of the box GenAI metrics and enables us to make our own GenAI metrics:
# MAGIC - Mlflow will automatically compute relevant task-related metrics. In our case, `model_type='question-answering'` will add the `toxicity` and `token_count` metrics.
# MAGIC - Then, we can import out of the box metrics provided by MLflow 2.8. Let's benefit from our ground-truth labels by computing the `answer_correctness` metric. 
# MAGIC - Finally, we can define customer metrics. Here, creativity is the only limit. In our demo, we will evaluate the `professionalism` of our Q&A chatbot.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Custom correctness answer
from mlflow.metrics.genai.metric_definitions import answer_correctness
from mlflow.metrics.genai import make_genai_metric, EvaluationExample

# Because we have our labels (answers) within the evaluation dataset, we can evaluate the answer correctness as part of our metric. Again, this is optional.
answer_correctness_metrics = answer_correctness(model=f"endpoints:/{endpoint_name}")
print(answer_correctness_metrics)

# COMMAND ----------

# DBTITLE 1,Adding custom professionalism metric
professionalism_example = EvaluationExample(
    input="What is MLflow?",
    output=(
        "MLflow is like your friendly neighborhood toolkit for managing your machine learning projects. It helps "
        "you track experiments, package your code and models, and collaborate with your team, making the whole ML "
        "workflow smoother. It's like your Swiss Army knife for machine learning!"
    ),
    score=2,
    justification=(
        "The response is written in a casual tone. It uses contractions, filler words such as 'like', and "
        "exclamation points, which make it sound less professional. "
    )
)

professionalism = make_genai_metric(
    name="professionalism",
    definition=(
        "Professionalism refers to the use of a formal, respectful, and appropriate style of communication that is "
        "tailored to the context and audience. It often involves avoiding overly casual language, slang, or "
        "colloquialisms, and instead using clear, concise, and respectful language."
    ),
    grading_prompt=(
        "Professionalism: If the answer is written using a professional tone, below are the details for different scores: "
        "- Score 1: Language is extremely casual, informal, and may include slang or colloquialisms. Not suitable for "
        "professional contexts."
        "- Score 2: Language is casual but generally respectful and avoids strong informality or slang. Acceptable in "
        "some informal professional settings."
        "- Score 3: Language is overall formal but still have casual words/phrases. Borderline for professional contexts."
        "- Score 4: Language is balanced and avoids extreme informality or formality. Suitable for most professional contexts. "
        "- Score 5: Language is noticeably formal, respectful, and avoids casual elements. Appropriate for formal "
        "business or academic settings. "
    ),
    model=f"endpoints:/{endpoint_name}",
    parameters={"temperature": 0.0},
    aggregations=["mean", "variance"],
    examples=[professionalism_example],
    greater_is_better=True
)

print(professionalism)

# COMMAND ----------

# DBTITLE 1,Start the evaluation run
from mlflow.deployments import set_deployments_target

set_deployments_target("databricks")

#This will automatically log all
with mlflow.start_run(run_name="chatbot_rag") as run:
    eval_results = mlflow.evaluate(data = df_qa_with_preds.toPandas(), # evaluation data,
                                   model_type="question-answering", # toxicity and token_count will be evaluated   
                                   predictions="preds", # prediction column_name from eval_df
                                   targets = "targets",
                                   extra_metrics=[answer_correctness_metrics, professionalism])
    
eval_results.metrics

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Visualization of our GenAI metrics produced by our GPT4 judge
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-llm-as-a-judge-mlflow.png?raw=true" style="float: right; margin-left:10px" width="800px">
# MAGIC
# MAGIC You can open your MLFlow experiment runs from the Experiments menu on the right. 
# MAGIC
# MAGIC From here, you can compare multiple model versions, and filter by correctness to spot where your model doesn't answer well. 
# MAGIC
# MAGIC Based on that and depending on the issue, you can either fine tune your prompt, your model fine tuning instruction with RLHF, or improve your documentation.
# MAGIC <br style="clear: both"/>
# MAGIC
# MAGIC ### Custom visualizations
# MAGIC You can equaly plot the evaluation metrics directly from the run, or pulling the data from MLFlow:

# COMMAND ----------

df_genai_metrics = eval_results.tables["eval_results_table"]
display(df_genai_metrics)

# COMMAND ----------

import plotly.express as px
px.histogram(df_genai_metrics, x="token_count", labels={"token_count": "Token Count"}, title="Distribution of Token Counts in Model Responses")

# COMMAND ----------

# Counting the occurrences of each answer correctness score
px.bar(df_genai_metrics['answer_correctness/v1/score'].value_counts(), title='Answer Correctness Score Distribution')

# COMMAND ----------

df_genai_metrics['toxicity'] = df_genai_metrics['toxicity/v1/score'] * 100
fig = px.scatter(df_genai_metrics, x='toxicity', y='answer_correctness/v1/score', title='Toxicity vs Correctness', size=[10]*len(df_genai_metrics))
fig.update_xaxes(tickformat=".2f")

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is looking good, let's tag our model as production ready
# MAGIC
# MAGIC After reviewing the model correctness and potentially comparing its behavior to your other previous version, we can flag our model as ready to be deployed.
# MAGIC
# MAGIC *Note: Evaluation can be automated and part of a MLOps step: once you deploy a new Chatbot version with a new prompt, run the evaluation job and benchmark your model behavior vs the previous version.*

# COMMAND ----------

client = MlflowClient()
client.set_registered_model_alias(name=model_name, alias="prod", version=model_version_to_evaluate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Databricks AI makes it easy to evaluate your LLM Models, leveraging custom metrics.
# MAGIC
# MAGIC Evaluating your chatbot is key to measure your future version impact, and your Data Intelligence Platform makes it easy leveraging automated Workflow for your MLOps pipelines.
# MAGIC
# MAGIC For a production-grade GenAI application, this step should be automated and part as a job, executed everytime the model is changed and benchmarked against previous run to make sure you don't have performance regression.
# MAGIC
# MAGIC ### Next: Deploy our model as Model Serving Endpoint with Inference Tables and deploy LLM metric monitoring (live monitoring)
# MAGIC
# MAGIC Open the [04-Deploy-Model-as-Endpoint]($./04-Deploy-Model-as-Endpoint) to deploy your model and track your endpoint payload as a Delta Table.
