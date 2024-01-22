# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Chat Bot with langchain and Dolly
# MAGIC
# MAGIC ## Chat Bot Prompt engineering
# MAGIC
# MAGIC In this example, we will improve our previous Q&A to create a chat bot.
# MAGIC
# MAGIC The main thing we'll be adding is a memory between the different question so that our bot can answer having the context of the previous Q&A.
# MAGIC
# MAGIC
# MAGIC <img style="float:right" width="800px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly-chatbot.png">
# MAGIC
# MAGIC ### Keeping memory between multiple questions
# MAGIC
# MAGIC The main challenge for our chat bot is that we won't be able to use the entire discussion history as context to send to Dolly. 
# MAGIC
# MAGIC First of all this is expensive, but more importantly this won't support long discussion as we'll end up with a text longer than our max window size for our mdoel.
# MAGIC
# MAGIC The trick is to use a summarize model and add an intermediate step which will take the summary of our discussion and inject it in our prompt.
# MAGIC
# MAGIC We will use an intermediate summarization task to do that, using `ConversationSummaryMemory` from `langchain`.
# MAGIC
# MAGIC
# MAGIC **Note: This is a more advanced content, we recommend you start with the Previous notebook: 03-Q&A-prompt-engineering-for-dolly**
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdolly-chatbot%2Fadvanced&dt=ML">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Setup
# MAGIC
# MAGIC - Run this on a cluster with Databricks Runtime 13.0 ML GPU. It should work on 12.2 ML GPU as well.
# MAGIC - To run this notebook's examples _without_ distributed Spark inference at the end, all that is needed is a single-node 'cluster' with a GPU
# MAGIC   - A10 and V100 instances should work, and this example is designed to fit the model in their working memory at some cost to quality
# MAGIC   - A100 instances work best, and perform better with minor modifications commented below
# MAGIC - To run the examples using distributed Spark inference at the end, provision a cluster of GPUs (and change the repartitioning at the end to match GPU count)
# MAGIC
# MAGIC *Note that `bitsandbytes` is not needed if running on A100s and the code is modified per comments below to not load in 8-bit.*

# COMMAND ----------

# MAGIC %pip install -U chromadb==0.3.22 langchain==0.0.164 transformers==4.29.0 accelerate==0.19.0 bitsandbytes

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=hive_metastore $db=dbdemos_llm

# COMMAND ----------

# DBTITLE 1,Create our vector database connection for context
if len(get_available_gpus()) == 0:
  Exception("Running dolly without GPU will be slow. We recommend you switch to a Single Node cluster with at least 1 GPU to properly run this demo.")

from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma

gardening_vector_db_path = "/dbfs"+demo_path+"/vector_db"
hf_embed = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
chroma_db = Chroma(collection_name="gardening_docs", embedding_function=hf_embed, persist_directory=gardening_vector_db_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2/ Prompt engineering with `langchain` and memory
# MAGIC
# MAGIC Now we can compose with a language model and prompting strategy to make a `langchain` chain that answers questions with a memory.

# COMMAND ----------

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline, AutoModelForSeq2SeqLM
from langchain import PromptTemplate
from langchain.llms import HuggingFacePipeline
from langchain.chains.question_answering import load_qa_chain
from langchain.memory import ConversationSummaryBufferMemory

def build_qa_chain():
  torch.cuda.empty_cache()
  # Defining our prompt content.
  # langchain will load our similar documents as {context}
  template = """You are a chatbot having a conversation with a human. Your are asked to answer gardening questions and help cultivating plants.
  Given the following extracted parts of a long document and a question, answer the user question. If you don't know, say that you do not know. 
  
  {context}

  {chat_history}

  {human_input}

  Response:
  """
  prompt = PromptTemplate(input_variables=['context', 'human_input', 'chat_history'], template=template)

  # Increase max_new_tokens for a longer response
  # Other settings might give better results! Play around
  model_name = "databricks/dolly-v2-7b" # can use dolly-v2-3b, dolly-v2-7b or dolly-v2-12b for smaller model and faster inferences.
  instruct_pipeline = pipeline(model=model_name, torch_dtype=torch.bfloat16, trust_remote_code=True, device_map="auto", 
                               return_full_text=True, max_new_tokens=256, top_p=0.95, top_k=50)
  hf_pipe = HuggingFacePipeline(pipeline=instruct_pipeline)

  # Add a summarizer to our memory conversation
  # Let's make sure we don't summarize the discussion too much to avoid losing to much of the content

  # Models we'll use to summarize our chat history
  # We could use one of these models: https://huggingface.co/models?filter=summarization. facebook/bart-large-cnn gives great results, we'll use t5-small for memory
  summarize_model = AutoModelForSeq2SeqLM.from_pretrained("t5-small", device_map="auto", torch_dtype=torch.bfloat16, trust_remote_code=True)
  summarize_tokenizer = AutoTokenizer.from_pretrained("t5-small", padding_side="left", model_max_length = 512)
  pipe_summary = pipeline("summarization", model=summarize_model, tokenizer=summarize_tokenizer) #, max_new_tokens=500, min_new_tokens=300
  # langchain pipeline doesn't support summarization yet, we added it as temp fix in the companion notebook _resources/00-init 
  hf_summary = HuggingFacePipeline_WithSummarization(pipeline=pipe_summary)
  #will keep 500 token and then ask for a summary. Removes prefix as our model isn't trained on specific chat prefix and can get confused.
  memory = ConversationSummaryBufferMemory(llm=hf_summary, memory_key="chat_history", input_key="human_input", max_token_limit=500, human_prefix = "", ai_prefix = "")

  # Set verbose=True to see the full prompt:
  print("loading chain, this can take some time...")
  return load_qa_chain(llm=hf_pipe, chain_type="stuff", prompt=prompt, verbose=True, memory=memory)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the Chain for Simple Question Answering
# MAGIC
# MAGIC That's it! it's ready to go. Define a function to answer a question and pretty-print the answer, with sources:

# COMMAND ----------

class ChatBot():
  def __init__(self, db):
    self.reset_context()
    self.db = db

  def reset_context(self):
    self.sources = []
    self.discussion = []
    # Building the chain will load Dolly and can take some time depending on the model size and your GPU
    self.qa_chain = build_qa_chain()
    displayHTML("<h1>Hi! I'm a chat bot specialized in gardening. How Can I help you today?</h1>")

  def get_similar_docs(self, question, similar_doc_count):
    return self.db.similarity_search(question, k=similar_doc_count)

  def chat(self, question):
    # Keep the last 3 discussion to search similar content
    self.discussion.append(question)
    similar_docs = self.get_similar_docs(" \n".join(self.discussion[-3:]), similar_doc_count=2)
    # Remove similar doc if they're already in the last questions (as it's already in the history)
    similar_docs = [doc for doc in similar_docs if doc.metadata['source'] not in self.sources[-3:]]

    result = self.qa_chain({"input_documents": similar_docs, "human_input": question})
    # Cleanup the answer for better display:
    answer = result['output_text'].capitalize()
    result_html = f"<p><blockquote style=\"font-size:24\">{question}</blockquote></p>"
    result_html += f"<p><blockquote style=\"font-size:18px\">{answer}</blockquote></p>"
    result_html += "<p><hr/></p>"
    for d in result["input_documents"]:
      source_id = d.metadata["source"]
      self.sources.append(source_id)
      result_html += f"<p><blockquote>{d.page_content}<br/>(Source: <a href=\"https://gardening.stackexchange.com/a/{source_id}\">{source_id}</a>)</blockquote></p>"
    displayHTML(result_html)

chat_bot = ChatBot(chroma_db)

# COMMAND ----------

# MAGIC %md 
# MAGIC Try asking a gardening question!

# COMMAND ----------

chat_bot.chat("What is the best kind of soil to grow blueberries in?")

# COMMAND ----------

chat_bot.chat("How much water should I give?")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Extra: Deploying a langchain pipeline in production with MLFlow (requires DBRML 13+)
# MAGIC
# MAGIC Once our bot is ready, we can package our pipeline using MLflow and the langchain flavor: 

# COMMAND ----------

# DBTITLE 1,Deploying our chat bot to MLFlow
def publish_model_to_mlflow():
  # Build our langchain pipeline
  langchain_model = build_qa_chain()

  with mlflow.start_run() as run:
      # Save model to MLFlow
      # Note that this only saves the langchain pipeline (we could also add the ChatBot with a custom Model Wrapper class)
      # See https://mlflow.org/docs/latest/models.html#custom-python-models for an example
      # The vector database lives outside of your model
      
      #Note: for now only LLMChain model are supported, qaChain will be added soon
      mlflow.langchain.log_model(langchain_model, artifact_path="model")
      model_registered = mlflow.register_model(f"runs:/{run.info.run_id}/model", "gardening-bot")

  # Move the model in production
  client = mlflow.tracking.MlflowClient()
  print("registering model version "+model_registered.version+" as production model")
  client.transition_model_version_stage("gardening-bot", model_registered.version, stage = "Production", archive_existing_versions=True)

def load_model_and_answer(similar_docs, question): 
  # Note: this will load the model once more in memory
  # Load the langchain pipeline & run inferences
  chain = mlflow.pyfunc.load_model(model_uri)
  chain.predict({"input_documents": similar_docs, "human_input": question})

# COMMAND ----------

# Make sure you restart the python kernel to free our gpu memory if you're using multiple notebooks0
# (load the model only once in 1 single notebook to avoid OOM)
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our chat bot is ready!
# MAGIC
# MAGIC That's it, you're ready to deploy your chat bot!
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC In this demo, we've seen a basic prompt engineering solution using history memory. More advanced solution can be build to provide better context.
# MAGIC
# MAGIC Having a good training dataset is key to improve our model performance and load better context. Collecting and preparing high quality data is likely the most important part to a successful bot!
# MAGIC
# MAGIC A good way to improve your dataset is to capture your user questions and chat, and incrementally improve your Q&A dataset.  <br/>
# MAGIC As example, `langchain` is especially build to work well with a chat bot trained with a dataset similar to OpenAI's, which isn't exactly matching Dolly's. The closest your prompt is engineered to match your training dataset content, the better your bot will behave.
# MAGIC
# MAGIC *A note on inference speed: As we load big models, inference time can be greatly optimized compiling our transformer models. Here is a quick example using onnx:*
# MAGIC
# MAGIC `%pip install -U transformers langchain chromadb accelerate bitsandbytes protobuf==3.19.0 optimum onnx onnxruntime-gpu`
# MAGIC
# MAGIC `%sh optimum-cli export onnx --model databricks/dolly-v2-7b  --device cuda --optimize O4 dolly_v2_7b_onnx`
# MAGIC
# MAGIC ```
# MAGIC from optimum.onnxruntime import ORTModelForCausalLM
# MAGIC
# MAGIC # Use Dolly as main model
# MAGIC model_name = "databricks/dolly-v2-3b" # can use dolly-v2-3b, dolly-v2-7b or dolly-v2-12b for smaller model and faster inferences.
# MAGIC tokenizer = AutoTokenizer.from_pretrained(model_name, padding_side="left")
# MAGIC model = ORTModelForCausalLM.from_pretrained("databricks/dolly-v2-3b", export=True, provider="CUDAExecutionProvider")
# MAGIC ```
# MAGIC *You could also leverage FasterTransformer. Contact your Databricks team for more details*
