# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 2/ Creating a Vector Search Index on top of our Delta Lake table
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-data-prep-3.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC We now have our knowledge base ready, and saved as a Delta Lake table within Unity Catalog (including permission, lineage, audit logs and all UC features).
# MAGIC
# MAGIC Typically, deploying a production-grade Vector Search index on top of your knowledge base is a difficult task. You need to maintain a process to capture table changes, index the model, provide a security layer, and all sorts of advanced search capabilities.
# MAGIC
# MAGIC Databricks Vector Search removes those painpoints.
# MAGIC
# MAGIC ## Databricks Vector Search
# MAGIC Databricks Vector Search is a new production-grade service that allows you to store a vector representation of your data, including metadata. It will automatically sync with the source Delta table and keep your index up-to-date without you needing to worry about underlying pipelines or clusters. 
# MAGIC
# MAGIC It makes embeddings highly accessible. You can query the index with a simple API to return the most similar vectors, and can optionally include filters or keyword-based queries.
# MAGIC
# MAGIC Vector Search is currently in Private Preview; you can [*Request Access Here*](https://docs.google.com/forms/d/e/1FAIpQLSeeIPs41t1Ripkv2YnQkLgDCIzc_P6htZuUWviaUirY5P5vlw/viewform)
# MAGIC
# MAGIC If you still do not have access to Databricks Vector Search, you can leverage [Chroma](https://docs.trychroma.com/getting-started) (open-source embedding database for building LLM apps). For an example end-to-end implementation with Chroma, pleaase see [this demo](https://www.dbdemos.ai/minisite/llm-dolly-chatbot/). 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document Embeddings 
# MAGIC
# MAGIC The first step is to create embeddings from the documents saved in our Delta Lake table. To do so, we need an LLM model specialized in taking a text of arbitrary length, and turning it into an embedding (vector of fixed size representing our document). 
# MAGIC
# MAGIC Embedding creation is done through LLMs, and many options are available: from public APIs to private models fine-tuned on your datasets.
# MAGIC
# MAGIC *Note: It is critical to ensure that the model is always the same for both embedding index creation and real-time similarity search. Remember that if your embedding model changes, you'll have to re-index your entire set of vectors, otherwise similarity search won't return relevant results.*

# COMMAND ----------

# DBTITLE 1,Install vector search package
# MAGIC %pip install databricks-vectorsearch-preview git+https://github.com/mlflow/mlflow@master databricks-sdk==0.8.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=dbdemos $db=chatbot $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Creating the Vector Search Index using our endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-data-prep-4.png?raw=true" style="float: right; margin-left: 10px" width="600px">
# MAGIC
# MAGIC
# MAGIC Now that our embedding endpoint is up and running, we can use it in our Vector Search index definition.
# MAGIC
# MAGIC Every time a new row is added in our Delta Lake table, Databricks will automatically capture the change, call the embedding endpoint with the row content, and index the embedding.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating our Vector Search Catalog
# MAGIC As of now, Vector Search indexes live in a specific Catalog. Let's start by creating the catalog using the `VectorSearchClient`
# MAGIC
# MAGIC *Note: During the private preview, only 1 Vector Search catalog may be enabled and a few indexes may be defined.* 

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

# COMMAND ----------

#Vector search requires a specific catalog. Let's create it and name it "vs_catalog"
try:
    vsc.get_catalog("vs_catalog")
except Exception as e:
    if 'CATALOG_DOES_NOT_EXIST' not in str(e):
        print(f'Unexpected error {e}. Is The preview enabled? Try deleting the vs catalog: vsc.delete_catalog("vs_catalog")')
        raise e
    print("creating Vector Search catalog")
    vsc.create_catalog("vs_catalog")
    #Make sure all users can access our VS index
    spark.sql(f"ALTER CATALOG vs_catalog SET OWNER TO `account users`")

#Wait for the catalog initialization which can take a few sec (see _resource/01-init for more details)
wait_for_vs_catalog_to_be_ready("vs_catalog")
vsc.list_indexes("vs_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating our Index
# MAGIC
# MAGIC As reminder, we want to add the index in the `databricks_documentation` table, indexing the column `content`. Let's review our `databricks_documentation` table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_documentation

# COMMAND ----------

# MAGIC %md
# MAGIC Vector search will capture all changes in your table, including updates and deletions, to synchronize your embedding index.
# MAGIC
# MAGIC To do so, make sure the `delta.enableChangeDataFeed` option is enabled in your Delta Lake table. Databricks Vector Index will use it to automatically propagate the changes. See the [Change Data Feed docs](https://docs.databricks.com/en/delta/delta-change-data-feed.html#enable-change-data-feed) to learn more, including how to set this property at the time of table creation

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE databricks_documentation SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# DBTITLE 1,Creating the index
#Endpoint name Databricks will used to create the embeddings
embedding_model_endpoint = "dbdemos_embedding_endpoint"
#The table we'd like to index
source_table_fullname = f"{catalog}.{db}.databricks_documentation"
#Where we want to store our index
vs_index_fullname = f"vs_catalog.{db}.databricks_documentation_index"

#Use this to reset your catalog/index 
#vsc.delete_catalog("vs_catalog") #!!deleting the catalog will drop all the index!!
#vsc.delete_index(vs_index_fullname) #Uncomment to delete & re-create the index.

try:
    vsc.get_index(vs_index_fullname)
except Exception as e:
    if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
        print(f'Unexepected error listing the index. Try deleting it? vsc.delete_index(vs_index_fullname)')
        raise e
    print(f'Creating a vector store index named `{vs_index_fullname}` against the table `{source_table_fullname}`, '+ \
            f'using model {embedding_model_endpoint}')
    i = vsc.create_index(
        source_table_name=source_table_fullname,
        dest_index_name=vs_index_fullname,
        primary_key="id",
        index_column="content",
        embedding_model_endpoint_name=embedding_model_endpoint
    )
    sleep(3) #Set permission so that all users can access the demo index (shared)
    spark.sql(f'ALTER SCHEMA vs_catalog.{db} OWNER TO `account users`')
    set_index_permission(f"vs_catalog.{db}.databricks_documentation_index", "ALL_PRIVILEGES", "account users")
    print(i)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Waiting for the index to build
# MAGIC That's all we have to do. Under the hood, Databricks will maintain a [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html) (DLT) job to refresh our pipeline.
# MAGIC
# MAGIC Note that depending on your dataset size and model size, this can take several minutes.
# MAGIC
# MAGIC For more details, you can access the DLT pipeline from the link you get in the index definition.

# COMMAND ----------

#Let's wait for the index to be ready and all our embeddings to be created and indexed
wait_for_index_to_be_ready(vs_index_fullname)
vsc.list_indexes("vs_catalog")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Your index is now ready and will be automatically synchronized with your table.
# MAGIC
# MAGIC Databricks will capture all changes made to the `databricks_documentation` Delta Lake table, and update the index accordingly. You can run your ingestion pipeline and update your documentations table, the index will automatically reflect these changes and get in synch with the best latencies.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Searching for similar content
# MAGIC
# MAGIC Our index is ready, and our Delta Lake table is now fully synchronized!
# MAGIC
# MAGIC Let's give it a try and search for similar content.
# MAGIC
# MAGIC *Note: Make sure that what you search is similar to one of the documents you indexed! Check your document table if in doubt.*

# COMMAND ----------

question = "How can I track billing usage on my workspaces?"
results = vsc.similarity_search(
  index_name=vs_index_fullname,
  query_text=question,
  columns=["url", "content"],
  num_results=1)
docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next step: Deploy our chatbot model with RAG
# MAGIC
# MAGIC We've seen how Databricks Lakehouse AI makes it easy to ingest and prepare your documents, and deploy a Vector Search index on top of it with just a few lines of code and configuration.
# MAGIC
# MAGIC This simplifies and accelerates your data projects so that you can focus on the next step: creating your realtime chatbot endpoint with well-crafted prompt augmentation.
# MAGIC
# MAGIC Open the [03-Deploy-RAG-Chatbot-Model]($./03-Deploy-RAG-Chatbot-Model) notebook to create and deploy a chatbot endpoint.
