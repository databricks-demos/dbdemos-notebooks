# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 1/ Ingesting and preparing PDF for LLM and Self Managed Vector Search Embeddings
# MAGIC
# MAGIC ## In this example, we will focus on ingesting pdf documents as source for our retrieval process. 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-pdf-self-managed-0.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC
# MAGIC For this example, we will add Databricks ebook PDFs from [Databricks resources page](https://www.databricks.com/resources) to our knowledge database.
# MAGIC
# MAGIC **Note: This demo is advanced content, we strongly recommend going over the simple version first to learn the basics.**
# MAGIC
# MAGIC Here are all the detailed steps:
# MAGIC
# MAGIC - Use autoloader to load the binary PDFs into our first table. 
# MAGIC - Use the `unstructured` library  to parse the text content of the PDFs.
# MAGIC - Use `llama_index` or `Langchain` to split the texts into chuncks.
# MAGIC - Compute embeddings for the chunks.
# MAGIC - Save our text chunks + embeddings in a Delta Lake table, ready for Vector Search indexing.
# MAGIC
# MAGIC
# MAGIC Lakehouse AI not only provides state of the art solutions to accelerate your AI and LLM projects, but also to accelerate data ingestion and preparation at scale, including unstructured data like PDFs.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=advanced/01-PDF-Advanced-Data-Preparation&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Install required external libraries 
# MAGIC %pip install transformers==4.41.1 pypdf==4.1.0 langchain-text-splitters==0.2.0 databricks-vectorsearch==0.37 mlflow tiktoken==0.7.0 torch==2.3.0 llama-index==0.10.43
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init-advanced $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ingesting Databricks ebook PDFs and extracting their pages
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-pdf-self-managed-1.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC First, let's ingest our PDFs as a Delta Lake table with path urls and content in binary format. 
# MAGIC
# MAGIC We'll use [Databricks Autoloader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) to incrementally ingeset new files, making it easy to incrementally consume billions of files from the data lake in various data formats. Autoloader easily ingests our unstructured PDF data in binary format.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS volume_databricks_documentation;

# COMMAND ----------

# DBTITLE 1,Our pdf or docx files are available in our Volume (or DBFS)
# List our raw PDF docs
volume_folder =  f"/Volumes/{catalog}/{db}/volume_databricks_documentation"
# Let's upload some pdf files to our volume as example. Change this with your own PDFs / docs.
upload_pdfs_to_volume(volume_folder+"/databricks-pdf")

display(dbutils.fs.ls(volume_folder+"/databricks-pdf"))

# COMMAND ----------

# DBTITLE 1,Ingesting PDF files as binary format using Databricks cloudFiles (Autoloader)
df = (spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'BINARYFILE')
        .option("pathGlobFilter", "*.pdf")
        .load('dbfs:'+volume_folder+"/databricks-pdf"))

# Write the data as a Delta table
(df.writeStream
  .trigger(availableNow=True)
  .option("checkpointLocation", f'dbfs:{volume_folder}/checkpoints/raw_docs')
  .table('pdf_raw').awaitTermination())

# COMMAND ----------

# MAGIC %sql SELECT * FROM pdf_raw LIMIT 2

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-pdf-self-managed-2.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC ## Extracting our PDF content as text chunks
# MAGIC
# MAGIC We need to convert the PDF documents bytes to text, and extract chunks from their content.
# MAGIC
# MAGIC This part can be tricky as PDFs are hard to work with and can be saved as images, for which we'll need an OCR to extract the text.
# MAGIC
# MAGIC Using the `Unstructured` library within a Spark UDF makes it easy to extract text. 
# MAGIC
# MAGIC *Note: Your cluster will need a few extra libraries that you would typically install with a cluster init script.*
# MAGIC
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC ### Splitting our big documentation page in smaller chunks
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/chunk-window-size.png?raw=true" style="float: right" width="700px">
# MAGIC
# MAGIC In this demo, some PDFs are very large, with a lot of text.
# MAGIC
# MAGIC We'll extract the content and then use llama_index `SentenceSplitter`, and ensure that each chunk isn't bigger than 500 tokens. 
# MAGIC
# MAGIC
# MAGIC The chunk size and chunk overlap depend on the use case and the PDF files. 
# MAGIC
# MAGIC Remember that your prompt+answer should stay below your model max window size (4096 for llama2). 
# MAGIC
# MAGIC For more details, review the previous [../01-Data-Preparation](01-Data-Preparation) notebook. 
# MAGIC
# MAGIC <br/>
# MAGIC <br style="clear: both">
# MAGIC <div style="background-color: #def2ff; padding: 15px;  border-radius: 30px; ">
# MAGIC   <strong>Information</strong><br/>
# MAGIC   Remember that the following steps are specific to your dataset. This is a critical part to building a successful RAG assistant.
# MAGIC   <br/> Always take time to review the chunks created and ensure they make sense and contain relevant information.
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,To extract our PDF,  we'll need to setup libraries in our nodes
import warnings
from pypdf import PdfReader

def parse_bytes_pypdf(raw_doc_contents_bytes: bytes):
    try:
        pdf = io.BytesIO(raw_doc_contents_bytes)
        reader = PdfReader(pdf)
        parsed_content = [page_content.extract_text() for page_content in reader.pages]
        return "\n".join(parsed_content)
    except Exception as e:
        warnings.warn(f"Exception {e} has been thrown during parsing")
        return None

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's start by extracting text from our PDF.

# COMMAND ----------

# DBTITLE 1,Trying our text extraction function with a single pdf file
import io
import re
with requests.get('https://github.com/databricks-demos/dbdemos-dataset/blob/main/llm/databricks-pdf-documentation/Databricks-Customer-360-ebook-Final.pdf?raw=true') as pdf:
  doc = parse_bytes_pypdf(pdf.content)  
  print(doc)

# COMMAND ----------

# MAGIC %md
# MAGIC This looks great. We'll now wrap it with a text_splitter to avoid having too big pages, and create a Pandas UDF function to easily scale that across multiple nodes.
# MAGIC
# MAGIC *Note that our pdf text isn't clean. To make it nicer, we could use a few extra LLM-based pre-processing steps, asking to remove unrelevant content like the list of chapters and to only keep the core text.*

# COMMAND ----------

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core import Document, set_global_tokenizer
from transformers import AutoTokenizer
from typing import Iterator

# Reduce the arrow batch size as our PDF can be big in memory
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10)

@pandas_udf("array<string>")
def read_as_chunk(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    #set llama2 as tokenizer to match our model size (will stay below BGE 1024 limit)
    set_global_tokenizer(
      AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")
    )
    #Sentence splitter from llama_index to split on sentences
    splitter = SentenceSplitter(chunk_size=500, chunk_overlap=10)
    def extract_and_split(b):
      txt = parse_bytes_pypdf(b)
      if txt is None:
        return []
      nodes = splitter.get_nodes_from_documents([Document(text=txt)])
      return [n.text for n in nodes]

    for x in batch_iter:
        yield x.apply(extract_and_split)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What's required for our Vector Search Index
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/databricks-vector-search-type.png?raw=true" style="float: right" width="800px">
# MAGIC
# MAGIC Databricks provide multiple types of vector search indexes:
# MAGIC
# MAGIC - **Managed embeddings**: you provide a text column and endpoint name and Databricks synchronizes the index with your Delta table 
# MAGIC - **Self Managed embeddings**: you compute the embeddings and save them as a field of your Delta Table, Databricks will then synchronize the index
# MAGIC - **Direct index**: when you want to use and update the index without having a Delta Table.
# MAGIC
# MAGIC In this demo, we will show you how to setup a **Self-managed Embeddings** index. 
# MAGIC
# MAGIC To do so, we will have to first compute the embeddings of our chunks and save them as a Delta Lake table field as `array&ltfloat&gt`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Introducing Databricks BGE Embeddings Foundation Model endpoints
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-pdf-self-managed-4.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC Foundation Models are provided by Databricks, and can be used out-of-the-box.
# MAGIC
# MAGIC Databricks supports several endpoint types to compute embeddings or evaluate a model:
# MAGIC - DBRX Instruct, a **foundation model endpoint**, or another model served by databricks (ex: llama2-70B, MPT...)
# MAGIC - An **external endpoint**, acting as a gateway to an external model (ex: Azure OpenAI)
# MAGIC - A **custom**, fined-tuned model hosted on Databricks model service
# MAGIC
# MAGIC Open the [Model Serving Endpoint page](/ml/endpoints) to explore and try the foundation models.
# MAGIC
# MAGIC For this demo, we will use the foundation model `BGE` (embeddings) and `llama2-70B` (chat). <br/><br/>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/databricks-foundation-models.png?raw=true" width="600px" >

# COMMAND ----------

# DBTITLE 1,Using Databricks Foundation model BGE as embedding endpoint
from mlflow.deployments import get_deploy_client

# bge-large-en Foundation models are available using the /serving-endpoints/databricks-bge-large-en/invocations api. 
deploy_client = get_deploy_client("databricks")

## NOTE: if you change your embedding model here, make sure you change it in the query step too
embeddings = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": ["What is Apache Spark?"]})
pprint(embeddings)

# COMMAND ----------

# DBTITLE 1,Create the final databricks_pdf_documentation table containing chunks and embeddings
# MAGIC %sql
# MAGIC --Note that we need to enable Change Data Feed on the table to create the index
# MAGIC CREATE TABLE IF NOT EXISTS databricks_pdf_documentation (
# MAGIC   id BIGINT GENERATED BY DEFAULT AS IDENTITY,
# MAGIC   url STRING,
# MAGIC   content STRING,
# MAGIC   embedding ARRAY <FLOAT>
# MAGIC ) TBLPROPERTIES (delta.enableChangeDataFeed = true); 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Computing the chunk embeddings and saving them to our Delta Table
# MAGIC
# MAGIC The last step is to now compute an embedding for all our documentation chunks. Let's create an udf to compute the embeddings using the foundation model endpoint.
# MAGIC
# MAGIC *Note that this part would typically be setup as a production-grade job, running as soon as a new documentation page is updated. <br/> This could be setup as a Delta Live Table pipeline to incrementally consume updates.*

# COMMAND ----------

@pandas_udf("array<float>")
def get_embedding(contents: pd.Series) -> pd.Series:
    import mlflow.deployments
    deploy_client = mlflow.deployments.get_deploy_client("databricks")
    def get_embeddings(batch):
        #Note: this will fail if an exception is thrown during embedding creation (add try/except if needed) 
        response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": batch})
        return [e['embedding'] for e in response.data]

    # Splitting the contents into batches of 150 items each, since the embedding model takes at most 150 inputs per request.
    max_batch_size = 150
    batches = [contents.iloc[i:i + max_batch_size] for i in range(0, len(contents), max_batch_size)]

    # Process each batch and collect the results
    all_embeddings = []
    for batch in batches:
        all_embeddings += get_embeddings(batch.tolist())

    return pd.Series(all_embeddings)

# COMMAND ----------

(spark.readStream.table('pdf_raw')
      .withColumn("content", F.explode(read_as_chunk("content")))
      .withColumn("embedding", get_embedding("content"))
      .selectExpr('path as url', 'content', 'embedding')
  .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", f'dbfs:{volume_folder}/checkpoints/pdf_chunk')
    .table('databricks_pdf_documentation').awaitTermination())

#Let's also add our documentation web page from the simple demo (make sure you run the quickstart demo first)
if table_exists(f'{catalog}.{db}.databricks_documentation'):
  (spark.readStream.option("skipChangeCommits", "true").table('databricks_documentation') #skip changes for more stable demo
      .withColumn('embedding', get_embedding("content"))
      .select('url', 'content', 'embedding')
  .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", f'dbfs:{volume_folder}/checkpoints/docs_chunks')
    .table('databricks_pdf_documentation').awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_pdf_documentation WHERE url like '%.pdf' limit 10

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Our dataset is now ready! Let's create our Self-Managed Vector Search Index.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-pdf-self-managed-3.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC Our dataset is now ready. We chunked the documentation pages into small sections, computed the embeddings and saved it as a Delta Lake table.
# MAGIC
# MAGIC Next, we'll configure Databricks Vector Search to ingest data from this table.
# MAGIC
# MAGIC Vector search index uses a Vector search endpoint to serve the embeddings (you can think about it as your Vector Search API endpoint). <br/>
# MAGIC Multiple Indexes can use the same endpoint. Let's start by creating one.

# COMMAND ----------

# DBTITLE 1,Creating the Vector Search endpoint
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if not endpoint_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME):
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC You can view your endpoint on the [Vector Search Endpoints UI](#/setting/clusters/vector-search). Click on the endpoint name to see all indexes that are served by the endpoint.

# COMMAND ----------

# DBTITLE 1,Create the Self-managed vector search using our endpoint
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = f"{catalog}.{db}.databricks_pdf_documentation"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.databricks_pdf_documentation_self_managed_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED", #Sync needs to be manually triggered
    primary_key="id",
    embedding_dimension=1024, #Match your model embedding size (bge)
    embedding_vector_column="embedding"
  )
  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
  vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).sync()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Searching for similar content
# MAGIC
# MAGIC That's all we have to do. Databricks will automatically capture and synchronize new entries in your Delta Lake Table.
# MAGIC
# MAGIC Note that depending on your dataset size and model size, index creation can take a few seconds to start and index your embeddings.
# MAGIC
# MAGIC Let's give it a try and search for similar content.
# MAGIC
# MAGIC *Note: `similarity_search` also supports a filters parameter. This is useful to add a security layer to your RAG system: you can filter out some sensitive content based on who is doing the call (for example filter on a specific department based on the user preference).*

# COMMAND ----------

question = "How can I track billing usage on my workspaces?"

response = deploy_client.predict(endpoint="databricks-bge-large-en", inputs={"input": [question]})
embeddings = [e['embedding'] for e in response.data]

results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
  query_vector=embeddings[0],
  columns=["url", "content"],
  num_results=1)
docs = results.get('result', {}).get('data_array', [])
pprint(docs)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next step: Deploy our chatbot model with RAG
# MAGIC
# MAGIC We've seen how Databricks Lakehouse AI makes it easy to ingest and prepare your documents, and deploy a Self Managed Vector Search index on top of it with just a few lines of code and configuration.
# MAGIC
# MAGIC This simplifies and accelerates your data projects so that you can focus on the next step: creating your realtime chatbot endpoint with well-crafted prompt augmentation.
# MAGIC
# MAGIC Open the [02-Advanced-Chatbot-Chain]($./02-Advanced-Chatbot-Chain) notebook to create and deploy a chatbot endpoint.
