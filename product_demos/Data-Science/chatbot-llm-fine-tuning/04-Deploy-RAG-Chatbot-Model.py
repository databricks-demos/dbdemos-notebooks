# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 3/ Creating the Chat bot with Retrieval Augmented Generation (RAG)
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-inference.png?raw=true" style="float: right; margin-left: 10px"  width="900px;">
# MAGIC
# MAGIC
# MAGIC Our Vector Search Index is now ready!
# MAGIC
# MAGIC Let's now create and deploy a new Model Serving Endpoint to perform RAG.
# MAGIC
# MAGIC The flow will be the following:
# MAGIC
# MAGIC - A user asks a question
# MAGIC - The question is sent to our serverless Chatbot RAG endpoint
# MAGIC - The endpoint searches for docs similar to the question, leveraging Vector Search on our Documentation table
# MAGIC - The endpoint creates a prompt enriched with the doc
# MAGIC - The prompt is sent to the AI Gateway, ensuring security, stability and governance
# MAGIC - The gateway sends the prompt to a MosaicML LLM Endpoint (currently LLama 2 70B)
# MAGIC - Mosaic returns the result
# MAGIC - We display the output to our customer!

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch-preview mlflow[gateway] databricks-sdk==0.8.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=dbdemos $db=chatbot $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Let's now create an endpoint for the RAG chatbot, using the gateway we deployed
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-inference-1.png?raw=true" style="float: right; margin-left: 10px"  width="600px;">
# MAGIC
# MAGIC Our gateway is ready, and our different model deployements can now securely use the MosaicML route to query our LLM.
# MAGIC
# MAGIC We are now going to build our Chatbot RAG model and deploy it as an endpoint for realtime Q&A!
# MAGIC
# MAGIC #### A note on prompt engineering
# MAGIC
# MAGIC The usual prompt engineering method applies for this chatbot. Make sure you're prompting your model with proper parameters and matching the model prompt format if any.
# MAGIC
# MAGIC For a production-grade example, you'd typically use `langchain` and potentially send the entire chat history to your endpoint to support "follow-up" style questions.
# MAGIC
# MAGIC More advanced chatbot behavior can be added here, including Chain of Thought, history summarization etc.
# MAGIC
# MAGIC Here is an example with `langchain`:
# MAGIC
# MAGIC ```
# MAGIC from langchain.llms import MlflowAIGateway
# MAGIC
# MAGIC gateway = MlflowAIGateway(
# MAGIC     gateway_uri="databricks",
# MAGIC     route="mosaicml-llama2-70b-completions",
# MAGIC     params={"temperature": 0.7, "top_p": 0.95,}
# MAGIC   )
# MAGIC prompt = PromptTemplate(input_variables=['context', 'question'], template=<your template as string>)
# MAGIC ```
# MAGIC
# MAGIC To keep our demo super simple and not getting confused with `langchain`, we will create a plain text template. 

# COMMAND ----------

import mlflow
from mlflow.pyfunc import PythonModel


import os
#Service principal Databricks PAT token we'll use to access our AI Gateway
os.environ['AI_GATEWAY_SP'] = dbutils.secrets.get("dbdemos", "ai_gateway_service_principal")

route = gateway.get_route(mosaic_route_name).route_url
workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

class ChatbotRAG(mlflow.pyfunc.PythonModel):
    #Send a request to our Vector Search Index to retrieve similar content.
    def find_relevant_doc(self, question, num_results = 1, relevant_threshold = 0.66):
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient(token=os.environ['AI_GATEWAY_SP'], workspace_url=workspace_url)
        results = vsc.similarity_search(
          index_name= f"vs_catalog.{db}.databricks_documentation_index",
          query_text=question,
          columns=["url", "content"],
          num_results=1)
        docs = results.get('result', {}).get('data_array', [])
        #Filter on the relevancy score. Below 0.6 means we don't have good relevant content
        if len(docs) > 0 and docs[0][-1] > relevant_threshold :
          return {"url": docs[0][0], "content": docs[0][1]}
        return None

    def predict(self, context, model_input):
        import os
        import requests
        import numpy as np
        import pandas as pd
        # If the input is a DataFrame or numpy array,
        # convert the first column to a list of strings.
        if isinstance(model_input, pd.DataFrame):
            model_input = model_input.iloc[:, 0].tolist()
        elif isinstance(model_input, np.ndarray):
            model_input = model_input[:, 0].tolist()
        elif isinstance(model_input, str):
            model_input = [model_input]
        answers = []
        for question in model_input:
          #Build the prompt
          prompt = "[INST] <<SYS>>You are an assistant for Databricks users. You are answering python, coding, SQL, data engineering, spark, data science, DW and platform, API or infrastructure administration question related to Databricks. If the question is not related to one of these topics, kindly decline to answer."
          doc = self.find_relevant_doc(question)
          #Add docs from our knowledge base to the prompt
          if doc is not None:
            prompt += f"\n\n Here is a documentation page which might help you answer: \n\n{doc['content']}"
          #Final instructions
          prompt += f"\n\n <</SYS>>Answer the following user question. If you don't know or the question isn't relevant or professional, say so. Only give a detailed answer. Don't have note or comment.\n\n  Question: {question}[/INST]"
          #Note the AI_GATEWAY_SP environement variable. It contains a service principal key
          response = requests.post(route, json = {"prompt": prompt, "max_tokens": 500}, headers={"Authorization": "Bearer "+os.environ['AI_GATEWAY_SP']})
          response.raise_for_status()
          response = response.json()
          
          if 'candidates' not in response:
            raise Exception(f"Can't parse response: {response}")
          answer = response['candidates'][0]['text']
          if doc is not None:
            answer += f"""\nFor more details, <a href="{doc['url']}">open the documentation</a>  """
          answers.append({"answer": answer.replace('\n', '<br/>'), "prompt": prompt})
        return answers

# COMMAND ----------

# DBTITLE 1,Let's try our chatbot in the notebook directly:
proxy_model = ChatbotRAG()
results = proxy_model.predict(None, ["How can I track billing usage on my workspaces?"])
print(results[0]["answer"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving our chatbot model in Unity Catalog

# COMMAND ----------

from mlflow.models import infer_signature
with mlflow.start_run(run_name="chatbot_rag") as run:
    chatbot = ChatbotRAG()
    #Let's try our model calling our Gateway API: 
    signature = infer_signature(["some", "data"], results)
    #Temp fix, do not use mlflow 2.6
    mlflow.pyfunc.log_model("model", python_model=chatbot, signature=signature, pip_requirements=["mlflow==2.4.0", "cloudpickle==2.0.0", "databricks-vectorsearch-preview"]) #
    #mlflow.set_tags({"route": proxy_model.route})
print(run.info.run_id)

# COMMAND ----------

#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')

client = MlflowClient()
try:
  #Get the model if it is already registered to avoid re-deploying the endpoint
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.dbdemos_chatbot_model", "prod")
  print(f"Our model is already deployed on UC: {catalog}.{db}.dbdemos_chatbot_model")
except:  
  #Add model within our catalog
  latest_model = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.dbdemos_chatbot_model")
  client.set_registered_model_alias(name=f"{catalog}.{db}.dbdemos_chatbot_model", alias="prod", version=latest_model.version)

  #Make sure all other users can access the model for our demo(see _resource/00-init for details)
  set_model_permission(f"{catalog}.{db}.dbdemos_chatbot_model", "ALL_PRIVILEGES", "account users")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Let's now deploy our realtime model endpoint
# MAGIC
# MAGIC Let's leverage Databricks Secrets to load the Service Principal key when the endpoint starts. Our SP can be see has a technical user which will be allowed to query the gateway. See the [documentation](https://docs.databricks.com/en/machine-learning/model-serving/store-env-variable-model-serving.html) for more details.
# MAGIC <br/>
# MAGIC To run this demo, you'll have to register a SP token under dbdemos/ai_gateway_service_principal: `databricks secrets put --scope dbdemos --key ai_gateway_service_principal`

# COMMAND ----------

#Helper for the endpoint rest api, see details in _resources/00-init
serving_client = EndpointApiClient()
#Start the enpoint using the REST API (you can do it using the UI directly)
serving_client.create_enpoint_if_not_exists("dbdemos_chatbot_rag", 
                                            model_name=f"{catalog}.{db}.dbdemos_chatbot_model", 
                                            model_version = latest_model.version, 
                                            workload_size="Small",
                                            scale_to_zero_enabled=True, 
                                            wait_start = True, 
                                            environment_vars={"AI_GATEWAY_SP": "{{secrets/dbdemos/ai_gateway_service_principal}}"})

#Make sure all users can access our endpoint for this demo
set_model_endpoint_permission("dbdemos_chatbot_rag", "CAN_MANAGE", "users")

# COMMAND ----------

# MAGIC %md
# MAGIC Our endpoint is now deployed! You can directly [open it from the UI](/endpoints/dbdemos_chatbot_rag) and visualize its performance!
# MAGIC
# MAGIC Let's run a REST query to try it in Python. As you can see, we send the `test sentence` doc and it returns an embedding representing our document.

# COMMAND ----------

# DBTITLE 1,Let's try to send a query to our chatbot
import timeit
question = "How can I track billing usage on my workspaces?"

answer = requests.post(f"{serving_client.base_url}/realtime-inference/dbdemos_chatbot_rag/invocations", 
                       json={"dataframe_split": {'data': [question]}}, 
                       headers=serving_client.headers).json()
#Note: If your workspace has ip access list, you need to allow your model serving endpoint to hit your AI gateways. Please reach out your Databrics Account team for IP ranges.
display_answer(question, answer['predictions'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! You have deployed your first Gen AI RAG model!
# MAGIC
# MAGIC You're now ready to deploy the same logic for your internal knowledge base leveraging Lakehouse AI.
# MAGIC
# MAGIC We've seen how the Lakehouse AI is uniquely positioned to help you solve your Gen AI challenge:
# MAGIC
# MAGIC - Simplify Data Ingestion and preparation with Databricks Engineering Capabilities
# MAGIC - Accelerate Vector Search  deployment with fully managed indexes
# MAGIC - Simplify, secure and control your LLM access with AI gateway
# MAGIC - Access MosaicML's LLama 2 endpoint
# MAGIC - Deploy realtime model endpoint to perform RAG 
# MAGIC
# MAGIC Lakehouse AI is uniquely positioned to accelerate your Gen AI deployment.
# MAGIC
# MAGIC Interested in deploying your own models? Reach out to your account team!
