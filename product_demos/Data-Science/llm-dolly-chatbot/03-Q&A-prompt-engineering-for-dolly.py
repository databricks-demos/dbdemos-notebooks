# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Question Answering over Custom Datasets with langchain and Dolly
# MAGIC
# MAGIC ## Prompt engineering
# MAGIC
# MAGIC Prompt engineering is a technique used to wrap the given user question with more information to better guide the model in its anwser.<br/>
# MAGIC Prompt engineering would typically involve:
# MAGIC - Guidance on how to answer given the usage (*ex: You are a gardener. Answer the following question as best as you can to keep plants alive*)
# MAGIC - Extra context to help your model. For example similar text close to the user question (*ex: Knowing that [Content from your internal Q&A], please answer...*)
# MAGIC - Specific instruction in the answer (*ex: Answer in Italian*) 
# MAGIC - Information on the previous questions to keep a context if you're building a chat bot (compressed as embedding)
# MAGIC - ...
# MAGIC
# MAGIC <img style="float:right" width="700px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly-inference-small.png">
# MAGIC
# MAGIC In this example, we'll use `langchain` to help us craft better prompt
# MAGIC
# MAGIC ## Our Gardening prompt engineering
# MAGIC
# MAGIC
# MAGIC
# MAGIC This example shows how to apply `langchain`, Hugging Face `transformers`, and even Apache Spark to answer questions about a specific text corpus. 
# MAGIC
# MAGIC It uses the Dolly2 LLM from Databricks, though this example can make use of any text-generation LLM or even OpenAI with minor changes. 
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>
# MAGIC
# MAGIC We'll implement the following flow: <br><br>
# MAGIC
# MAGIC <div style="margin-left: 20px">
# MAGIC   <div class="badge_b"><div class="badge">1</div> Get the question and transform it as embedding using the same sentence 2 embedding model.</div>
# MAGIC   <div class="badge_b"><div class="badge">2</div> Do a similarity search within chroma to find related question & answers</div>
# MAGIC   <div class="badge_b"><div class="badge">3</div> Engineer a prompt containing the question & the similar Q&A as context</div>
# MAGIC   <div class="badge_b"><div class="badge">4</div> Send the prompt to dolly</div>
# MAGIC   <div class="badge_b"><div class="badge">5</div> Our customer get their gardening advise!</div>
# MAGIC </div>
# MAGIC <br/>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdolly-chatbot%2Fprompt-engineering&dt=ML">

# COMMAND ----------

# MAGIC %pip install -U chromadb==0.3.22 langchain==0.0.164 transformers==4.29.0 accelerate==0.19.0 bitsandbytes

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=hive_metastore $db=dbdemos_llm

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

# MAGIC %md 
# MAGIC ### 1/ Download our 2 embeddings model from hugging face (same as data preparation)

# COMMAND ----------

# Start here to load a previously-saved DB
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma

if len(get_available_gpus()) == 0:
  Exception("Running dolly without GPU will be slow. We recommend you switch to a Single Node cluster with at least 1 GPU to properly run this demo.")

gardening_vector_db_path = "/dbfs"+demo_path+"/vector_db"

hf_embed = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
db = Chroma(collection_name="gardening_docs", embedding_function=hf_embed, persist_directory=gardening_vector_db_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2/ Similarity search using chroma
# MAGIC
# MAGIC Let's test our similarity search with a simple question.
# MAGIC
# MAGIC Note that `k` (`similar_doc_count`): is the number of chunks of text retrieved to send to the prompt. Longer prompts add more context but takes longer to process.

# COMMAND ----------

def get_similar_docs(question, similar_doc_count):
  return db.similarity_search(question, k=similar_doc_count)

# Let's test it with blackberries:
for doc in get_similar_docs("how to grow blackberry?", 2):
  print(doc.page_content)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3/ Prompt engineering with `langchain` 
# MAGIC
# MAGIC Now we can compose with a language model and prompting strategy to make a `langchain` chain that answers questions.

# COMMAND ----------

from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch
from langchain import PromptTemplate
from langchain.llms import HuggingFacePipeline
from langchain.chains.question_answering import load_qa_chain

def build_qa_chain():
  torch.cuda.empty_cache()
  model_name = "databricks/dolly-v2-7b" # can use dolly-v2-3b or dolly-v2-7b for smaller model and faster inferences.

  # Increase max_new_tokens for a longer response
  # Other settings might give better results! Play around
  instruct_pipeline = pipeline(model=model_name, torch_dtype=torch.bfloat16, trust_remote_code=True, device_map="auto", 
                               return_full_text=True, max_new_tokens=256, top_p=0.95, top_k=50)
  # Note: if you use dolly 12B or smaller model but a GPU with less than 24GB RAM, use 8bit. This requires %pip install bitsandbytes
  # instruct_pipeline = pipeline(model=model_name, trust_remote_code=True, device_map="auto", model_kwargs={'load_in_8bit': True})
  # For GPUs without bfloat16 support, like the T4 or V100, use torch_dtype=torch.float16 below
  # model = AutoModelForCausalLM.from_pretrained(model_name, device_map="auto", torch_dtype=torch.float16, trust_remote_code=True)

  # Defining our prompt content.
  # langchain will load our similar documents as {context}
  template = """Below is an instruction that describes a task. Write a response that appropriately completes the request.

  Instruction: 
  You are a gardener and your job is to help providing the best gardening answer. 
  Use only information in the following paragraphs to answer the question at the end. Explain the answer with reference to these paragraphs. If you don't know, say that you do not know.

  {context}
 
  Question: {question}

  Response:
  """
  prompt = PromptTemplate(input_variables=['context', 'question'], template=template)

  hf_pipe = HuggingFacePipeline(pipeline=instruct_pipeline)
  # Set verbose=True to see the full prompt:
  return load_qa_chain(llm=hf_pipe, chain_type="stuff", prompt=prompt, verbose=True)

# COMMAND ----------

# Building the chain will load Dolly and can take several minutes depending on the model size
qa_chain = build_qa_chain()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that there are _many_ factors that affect how the language model answers a question. Most notable is the prompt template itself. This can be changed, and different prompts may work better or worse with certain models.
# MAGIC
# MAGIC The generation process itself also has many knobs to tune, and often it simply requires trial and error to find settings that work best for certain models and certain data sets. See this [excellent guide from Hugging Face](https://huggingface.co/blog/how-to-generate). 
# MAGIC
# MAGIC The settings that most affect performance are:
# MAGIC - `max_new_tokens`: longer responses take longer to generate. Reduce for shorter, faster responses
# MAGIC - `num_beams`: if using beam search, more beams increase run time more or less linearly

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Using the Chain for Simple Question Answering
# MAGIC
# MAGIC That's it! It's ready to go. Define a function to answer a question and pretty-print the answer, with sources:

# COMMAND ----------

def answer_question(question):
  similar_docs = get_similar_docs(question, similar_doc_count=2)
  result = qa_chain({"input_documents": similar_docs, "question": question})
  result_html = f"<p><blockquote style=\"font-size:24\">{question}</blockquote></p>"
  result_html += f"<p><blockquote style=\"font-size:18px\">{result['output_text']}</blockquote></p>"
  result_html += "<p><hr/></p>"
  for d in result["input_documents"]:
    source_id = d.metadata["source"]
    result_html += f"<p><blockquote>{d.page_content}<br/>(Source: <a href=\"https://gardening.stackexchange.com/a/{source_id}\">{source_id}</a>)</blockquote></p>"
  displayHTML(result_html)

# COMMAND ----------

# MAGIC %md 
# MAGIC Try asking a gardening question!

# COMMAND ----------

answer_question("What is the best kind of soil to grow blueberries in?")

# COMMAND ----------

# MAGIC %md
# MAGIC # Scaling our Question Answering with Spark
# MAGIC
# MAGIC Let's now see how we can scale this process to answer our question at scale using a Spark UDF.
# MAGIC
# MAGIC Questions will be answered in parallel with Spark. Note that this section requires a cluster with GPU workers.

# COMMAND ----------

#Free some memory to avoid loading model twice & OOM
del hf_embed, qa_chain, db 
cuda.get_current_device().reset()
gc.collect()


@pandas_udf('answer string, sources array<string>')
def answer_question_udf(question_sets: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  os.environ['TRANSFORMERS_CACHE'] = hugging_face_cache
  hf_embed_udf = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
  db_udf = Chroma(collection_name="gardening_docs", embedding_function=hf_embed_udf, persist_directory=gardening_vector_db_path)
  qa_chain_udf = build_qa_chain()
  for questions in question_sets:
    responses = []
    for question in questions:
      # k is the number of docs to retrieve to feed as context
      similar_docs = db_udf.similarity_search(question, k=1)
      result = qa_chain_udf({"input_documents": similar_docs, "question": question})
      responses.append({"answer": result["output_text"], "sources": [str(d.metadata["source"]) for d in result["input_documents"]]}) 
    yield pd.DataFrame(responses)

# COMMAND ----------

# MAGIC %md 
# MAGIC Add some questions to answer

# COMMAND ----------

new_questions_df = spark.table("gardening_dataset") \
                        .filter("parent_id IS NULL") \
                        .select("body") \
                        .toDF("question") \
                        .limit(5)

#Saving a subset of question to answer for faster processing for the demo.
new_questions_df.repartition(1).write.mode("overwrite").saveAsTable("question_to_answer")
new_questions_df = spark.table("question_to_answer").repartition(1)  # Repartition to number of GPUs (multi node or single node with N gpu)
display(new_questions_df)

# COMMAND ----------

#Note: see the next notebook to compile the models for faster inference time.
response_df = new_questions_df.select(col("question"), answer_question_udf("question").alias("response")).select("question", "response.*")
display(response_df)

# COMMAND ----------

# DBTITLE 1,Cleanup our GPU memory before the next notebook
# Make sure you restart the python kernel to free our gpu memory if you're using multiple notebooks0
# (load the model only once in 1 single notebook to avoid OOM)
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Next: improving our Q&A prompt to chain questions as a chat bot
# MAGIC
# MAGIC Open the next notebook [04-chat-bot-prompt-engineering-dolly]($./04-chat-bot-prompt-engineering-dolly) to improve our chain and add memory between our interaction.
