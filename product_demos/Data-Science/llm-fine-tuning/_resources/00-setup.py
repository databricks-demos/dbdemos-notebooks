# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

if not spark.catalog.tableExists('training_dataset_question') or \
    not spark.catalog.tableExists('training_dataset_answer') or \
    not spark.catalog.tableExists('databricks_documentation'):
  DBDemos.download_file_from_git(volume_folder+"/training_dataset", "databricks-demos", "dbdemos-dataset", "llm/databricks-documentation")

  #spark.read.format('parquet').load(f"{volume_folder}/training_dataset/raw_documentation.parquet").write.saveAsTable("raw_documentation")
  spark.read.format('parquet').load(f"{volume_folder}/training_dataset/training_dataset_question.parquet").write.saveAsTable("training_dataset_question")
  spark.read.format('parquet').load(f"{volume_folder}/training_dataset/training_dataset_answer.parquet").write.saveAsTable("training_dataset_answer")
  spark.read.format('parquet').load(f"{volume_folder}/training_dataset/databricks_documentation.parquet").write.saveAsTable("databricks_documentation")

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  #You can programatically get a PAT token with the following
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  xp_root_path = f"/Shared/dbdemos/experiments/{demo_name}"
  try:
    r = w.workspace.mkdirs(path=xp_root_path)
  except Exception as e:
    print(f"ERROR: couldn't create a folder for the experiment under {xp_root_path} - please create the folder manually or  skip this init (used for job only: {e})")
    raise e
  xp = f"{xp_root_path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)

# COMMAND ----------

# Helper function
def check_model_exists(model_name):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import ResourceDoesNotExist

    w = WorkspaceClient()

    try:
        w.registered_models.get(model_name)
        return True
    except ResourceDoesNotExist:
        return False

# COMMAND ----------

# Helper function
def get_latest_model_version(model_name):
    from mlflow.tracking import MlflowClient
    mlflow_client = MlflowClient(registry_uri="databricks-uc")
    latest_version = 1
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

# COMMAND ----------

import warnings
import re
import pandas as pd

# Disable a few less-than-useful UserWarnings from setuptools and pydantic
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

# COMMAND ----------

#Helper fuinction to Wait for the fine tuning run to finish
def wait_for_run_to_finish(run):
  import time
  print_train = False
  for i in range(300):
    events = run.get_events()
    for e in events:
      if "FAILED" in e.type or "EXCEPTION" in e.type:
        raise Exception(f'Error with the fine tuning run, check the details in run.get_events(): {e}')
    if events[-1].type == 'TRAIN_FINISHED':
      print('Run finished')
      return events
    if i % 30 == 0:
      print(f'waiting for run {run.name} to complete...')
    if events[-1].type == 'TRAIN_UPDATED' and not print_train:
      print_train = True
      display(events)
    time.sleep(10)


#Format answer, converting MD to html
def display_answer(answer):
  import markdown
  displayHTML(markdown.markdown(answer['choices'][0]['message']['content']))

# COMMAND ----------

#Return the current cluster id to use to read the dataset and send it to the fine tuning cluster. See https://docs.databricks.com/en/large-language-models/foundation-model-training/create-fine-tune-run.html#cluster-id
def get_current_cluster_id():
  import json
  return json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']['clusterId']


# COMMAND ----------

import json
import re
# Extract the json array from the text, removing potential noise
def extract_json_array(text):
    # Use regex to find a JSON array within the text
    match = re.search(r'(\[.*?\])', text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    return []

