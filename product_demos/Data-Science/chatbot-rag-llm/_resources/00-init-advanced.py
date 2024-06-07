# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init-advanced&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %run ./00-init

# COMMAND ----------

import requests
import collections
import os
 
def download_file_from_git(dest, owner, repo, path):
    def download_file(url, destination):
      local_filename = url.split('/')[-1]
      # NOTE the stream=True parameter below
      with requests.get(url, stream=True) as r:
          r.raise_for_status()
          print('saving '+destination+'/'+local_filename)
          with open(destination+'/'+local_filename, 'wb') as f:
              for chunk in r.iter_content(chunk_size=8192): 
                  # If you have chunk encoded response uncomment if
                  # and set chunk_size parameter to None.
                  #if chunk: 
                  f.write(chunk)
      return local_filename

    if not os.path.exists(dest):
      os.makedirs(dest)
    from concurrent.futures import ThreadPoolExecutor
    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents{path}').json()
    files = [f['download_url'] for f in files if 'NOTICE' not in f['name']]
    def download_to_dest(url):
         download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
        collections.deque(executor.map(download_to_dest, files))

# COMMAND ----------

def upload_pdfs_to_volume(volume_path):
  download_file_from_git(volume_path, "databricks-demos", "dbdemos-dataset", "/llm/databricks-pdf-documentation")

def upload_dataset_to_volume(volume_path):
  download_file_from_git(volume_path, "databricks-demos", "dbdemos-dataset", "/llm/databricks-documentation")

# COMMAND ----------

def deduplicate_assessments_table(assessment_table):
    # De-dup response assessments
    assessments_request_deduplicated_df = spark.sql(f"""select * except(row_number)
                                        from ( select *, row_number() over (
                                                partition by request_id
                                                order by
                                                timestamp desc
                                            ) as row_number from {assessment_table} where text_assessment is not NULL
                                        ) where row_number = 1""")
    # De-dup the retrieval assessments
    assessments_retrieval_deduplicated_df = spark.sql(f"""select * except( retrieval_assessment, source, timestamp, text_assessment),
        any_value(timestamp) as timestamp,
        any_value(source) as source,
        collect_list(retrieval_assessment) as retrieval_assessments
      from {assessment_table} where retrieval_assessment is not NULL group by request_id, source.id, step_id"""    )

    # Merge together
    assessments_request_deduplicated_df = assessments_request_deduplicated_df.drop("retrieval_assessment", "step_id")
    assessments_retrieval_deduplicated_df = assessments_retrieval_deduplicated_df.withColumnRenamed("request_id", "request_id2").withColumnRenamed("source", "source2").drop("step_id", "timestamp")

    merged_deduplicated_assessments_df = assessments_request_deduplicated_df.join(
        assessments_retrieval_deduplicated_df,
        (assessments_request_deduplicated_df.request_id == assessments_retrieval_deduplicated_df.request_id2) &
        (assessments_request_deduplicated_df.source.id == assessments_retrieval_deduplicated_df.source2.id),
        "full"
    ).select(
        [str(col) for col in assessments_request_deduplicated_df.columns] +
        [assessments_retrieval_deduplicated_df.retrieval_assessments]
    )

    return merged_deduplicated_assessments_df
