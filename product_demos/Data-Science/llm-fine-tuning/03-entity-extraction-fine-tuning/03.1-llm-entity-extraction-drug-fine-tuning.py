# Databricks notebook source
# MAGIC %md # Foundation Model fine-tuning: Named Entity Recognition
# MAGIC
# MAGIC In this demo, we will focus on Fine Tuning our model for Instruction Fine Tuning, specializing llama 3.2 3B to extract drug name from text. This process is call NER (Named Entity Recognition)
# MAGIC
# MAGIC Fine tuning an open-source model on a medical Named Entity Recognition task will make the model output
# MAGIC 1. More accurate, and
# MAGIC 2. More efficient, reducing model serving expenses

# COMMAND ----------

# DBTITLE 1,Library installs
# Let's start by installing our dependencies
%pip install databricks-genai==1.1.4 mlflow==2.16.2 langchain-community==0.2.0 transformers==4.31.0 datasets==2.16.1 huggingface-hub==0.27.1
%pip install databricks-sdk==0.40.0

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Preparing our dataset
# MAGIC
# MAGIC For simplicity in this example, we'll use an existing NER dataset from Huggingface. In commercial applications, it typically makes more sense to invest in data labeling to get enough samples to improve model performance. 
# MAGIC
# MAGIC Preparing your dataset for Instruction Fine Tuning is key. The Databricks Mosaic AI research team has published some [helpful guidelines](https://www.databricks.com/blog/limit-less-more-instruction-tuning) for developing a training data curation strategy. 

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

from datasets import load_dataset
import pandas as pd

hf_dataset_name = "allenai/drug-combo-extraction"

dataset_test = load_dataset(hf_dataset_name, split="test")

# Convert the dataset to a pandas DataFrame
df_test = pd.DataFrame(dataset_test)

# Extract the entities from the spans
df_test["human_annotated_entities"] = df_test["spans"].apply(lambda spans: [span["text"] for span in spans])

df_test = df_test[["sentence", "human_annotated_entities"]]

display(df_test)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Build our prompt template to extract entities

# COMMAND ----------

system_prompt = """
### INSTRUCTIONS:
You are a medical and pharmaceutical expert. Your task is to identify pharmaceutical drug names from the provided input and list them accurately. Follow these guidelines:

1. Do not add any commentary or repeat the instructions.
2. Extract the names of pharmaceutical drugs mentioned.
3. Place the extracted names in a Python list format. Ensure the names are enclosed in square brackets and separated by commas.
4. Maintain the order in which the drug names appear in the input.
5. Do not add any text before or after the list.
"""

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Extracting our entities with a baseline version (non fine-tuned)
# MAGIC
# MAGIC Let's start by performing a first entity extraction with our baseline, non fine-tuned model.
# MAGIC
# MAGIC We will be using the same endpoint `dbdemos_llm_not_fine_tuned_llama3p2_3B` as in the previous [../02-llm-evaluation]($../02-llm-evaluation) notebook to reduce cost. 
# MAGIC
# MAGIC **Make sure you run this notebook to setup the endpoint before.**

# COMMAND ----------

import mlflow
from langchain_community.chat_models.databricks import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
import json

# Make sure this model corresponds to the model you used in the ../02-llm-evaluation notebook
base_model_name = "meta-llama/Llama-3.2-3B-Instruct"

input_sentence = "{sentence}"

def build_chain(llm):
    # Mistral doesn't support the system role.
    if "mistral" in base_model_name:
        messages = [("user", f"{system_prompt} \n {input_sentence}")]
    else:
        messages = [("system", system_prompt),
                    ("user", input_sentence)]
    return ChatPromptTemplate.from_messages(messages) | llm | StrOutputParser()
  
def extract_entities(df, endpoint_name):
  llm = ChatDatabricks(endpoint=endpoint_name, temperature=0.1)
  chain = build_chain(llm)
  predictions = chain.with_retry(stop_after_attempt=2) \
                                      .batch(df[["sentence"]].to_dict(orient="records"), config={"max_concurrency": 4})
  # Extract the array from the text. See the ../resource notebook for more details.
  cleaned_predictions = [[x.strip() for x in prediction.strip('[]').split(',')] for prediction in predictions] #[extract_json_array(p) for p in predictions]
  return predictions, cleaned_predictions 

# Taking only a few examples from test set to collect benchmark metrics
from sklearn.model_selection import train_test_split

df_validation, df_test_small = train_test_split(df_test, test_size=0.2, random_state=42)

# This endpoint is created in the ../02-llm-evaluation notebook. It's the baseline llama 3.2 3B model, not fine tuned.
# Make sure you run the notebook before to deploy the baseline model first.
serving_endpoint_baseline_name = "dbdemos_llm_not_fine_tuned_llama3p2_3B"

predictions, cleaned_predictions = extract_entities(df_test_small, serving_endpoint_baseline_name)
df_test_small['baseline_predictions'] = predictions
df_test_small['baseline_predictions_cleaned'] = cleaned_predictions
display(df_test_small[["sentence", "baseline_predictions", "baseline_predictions_cleaned", "human_annotated_entities"]])

# COMMAND ----------

# MAGIC %md ## Evaluating our baseline model
# MAGIC
# MAGIC We can see that our model is extracting a good number of entities, but it also sometimes add some random text after/before the inferences.
# MAGIC
# MAGIC ### Precision & recall for entity extraction
# MAGIC
# MAGIC We'll benchmark our model by computing its accuracy and recall. Let's compute these value for each sentence in our test dataset.

# COMMAND ----------

from sklearn.metrics import precision_score, recall_score

def compute_precision_recall(prediction, ground_truth):
    prediction_set = set([str(drug).lower() for drug in prediction])
    ground_truth_set = set([str(drug).lower() for drug in ground_truth])
    all_elements = prediction_set.union(ground_truth_set)

    # Convert sets to binary lists
    prediction_binary = [int(element in prediction_set) for element in all_elements]
    ground_truth_binary = [int(element in ground_truth_set) for element in all_elements]
    
    precision = precision_score(ground_truth_binary, prediction_binary)
    recall = recall_score(ground_truth_binary, prediction_binary)

    return precision, recall
  
def precision_recall_series(row):
  precision, recall = compute_precision_recall(row['baseline_predictions_cleaned'], row['human_annotated_entities'])
  return pd.Series([precision, recall], index=['precision', 'recall'])

df_test_small[['baseline_precision', 'baseline_recall']] = df_test_small.apply(precision_recall_series, axis=1)
df_test_small[['baseline_precision', 'baseline_recall']].describe()

# COMMAND ----------

# MAGIC %md
# MAGIC *_NOTE: Results will vary from run to run_
# MAGIC
# MAGIC In the sample, we see that the baseline LLM generally having a Recall of 0.9652 which means that it successfully identifies about 96.52% of all actual drug names present in the text. This metric is crucial in healthcare and related fields where missing a drug name can lead to incomplete or incorrect information processing. 
# MAGIC
# MAGIC Precision of 0.9174 on avg means that the baseline LLM model identifies a token or a sequence of tokens as a drug name, about 91.74% of those identifications are correct.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Fine-tuning our model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fine tuning data preparation
# MAGIC
# MAGIC Before fine-tuning, we need to apply our prompt template to the samples in the training dataset, and extract the ground truth list of drugs into the list format we are targeting.
# MAGIC
# MAGIC We'll save this to our Databricks catalog as a table. Usually, this is part of a full Data Engineering pipeline.
# MAGIC
# MAGIC Remember that this step is key for your Fine Tuning, make sure your training dataset is of high quality!

# COMMAND ----------

dataset_train = load_dataset(hf_dataset_name, split="train")
df_train = pd.DataFrame(dataset_train)

# Convert the dataset to a pandas DataFrame
df_train = pd.DataFrame(df_train)

# Extract the entities from the spans
df_train["human_annotated_entities"] = df_train["spans"].apply(lambda spans: [span["text"] for span in spans])

df_train = df_train[["sentence", "human_annotated_entities"]]

df_train

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, to_json
import pandas as pd

@pandas_udf("array<struct<role:string, content:string>>")
def create_conversation(sentence: pd.Series, entities: pd.Series) -> pd.Series:
    def build_message(s, e):
        # Adjusting the logic to check for a specific model's behavior
        if "mistral" in base_model_name:
            # Mistral-specific behavior without system prompt
            return [
                {"role": "user", "content": f"{system_prompt} \n {s}"},
                {"role": "assistant", "content": e}]
        else:
            # Default behavior with system prompt
            return [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": str(s)},
                {"role": "assistant", "content": e}]
                
    # Apply build_message to each pair of sentence and entity
    return pd.Series([build_message(s, e) for s, e in zip(sentence, entities)])

# Assuming df_train is defined and correctly formatted as a Spark DataFrame with columns 'sentence' and 'entities'
training_dataset = spark.createDataFrame(df_train).withColumn("human_annotated_entities", to_json("human_annotated_entities"))

# Apply UDF, write to a table, and display it
training_dataset.select(create_conversation("sentence", "human_annotated_entities").alias('messages')).write.mode('overwrite').saveAsTable("ner_chat_completion_training_dataset")
display(spark.table("ner_chat_completion_training_dataset"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Prepare the eval dataset as well. We have the data available in `df_validation`

# COMMAND ----------

eval_dataset = spark.createDataFrame(df_validation).withColumn("human_annotated_entities", to_json("human_annotated_entities"))

# Apply UDF, write to a table, and display it
eval_dataset.select(create_conversation("sentence", "human_annotated_entities").alias('messages')).write.mode('overwrite').saveAsTable("ner_chat_completion_eval_dataset")
display(spark.table("ner_chat_completion_eval_dataset"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Fine-tuning
# MAGIC Once our data is ready, we can just call the fine tuning API

# COMMAND ----------

from databricks.model_training import foundation_model as fm

# Change the model name back to drug_extraction_ft after testing
registered_model_name = f"{catalog}.{db}.drug_extraction_ft_meta_llama_3_2_3b_instruct"

run = fm.create(
  data_prep_cluster_id = get_current_cluster_id(),  # Required if you are using delta tables as training data source. This is the cluster id that we want to use for our data prep job. See ./_resources for more details
  model=base_model_name,
  train_data_path=f"{catalog}.{db}.ner_chat_completion_training_dataset",
  eval_data_path=f"{catalog}.{db}.ner_chat_completion_eval_dataset",
  task_type = "CHAT_COMPLETION",
  register_to=registered_model_name,
  training_duration='50ep' # Duration of the finetuning run, 10 epochs only to make it fast for the demo. Check the training run metrics to know when to stop it (when it reaches a plateau)
)
print(run)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Tracking model fine tuning through your MLFlow experiment
# MAGIC Your can open the MLflow Experiment run to track your fine tuning experiment. This is useful for you to know how to tune the training run (ex: add more epoch if you see your model still improves at the end of your run).
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-experiment.png?raw=true" width="1200px">

# COMMAND ----------

# DBTITLE 1,track run
displayHTML(f'Open the <a href="/ml/experiments/{run.experiment_id}/runs/{run.run_id}/model-metrics">training run on MLflow</a> to track the metrics')
display(run.get_events())

# Helper function waiting on the run to finish - see the _resources folder for more details
wait_for_run_to_finish(run)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy Fine-Tuned model to serving endpoint

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput, AiGatewayConfig, AiGatewayInferenceTableConfig

serving_endpoint_name = "dbdemos_drug_extraction_fine_tuned_03_llama_3_2_3B_Instruct"
w = WorkspaceClient()
# Create the AI Gateway configuration
ai_gateway_config = AiGatewayConfig(
    inference_table_config=AiGatewayInferenceTableConfig(
        enabled=True,
        catalog_name=catalog,
        schema_name=db,
        table_name_prefix="fine_tuned_drug_extraction_llm_inference"
    )
)
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_entities=[
        ServedEntityInput(
            entity_name=registered_model_name,
            entity_version=get_latest_model_version(registered_model_name),
            min_provisioned_throughput=0, # The minimum tokens per second that the endpoint can scale down to.
            max_provisioned_throughput=100,# The maximum tokens per second that the endpoint can scale up to.
            scale_to_zero_enabled=True
        )
    ]
)

force_update = False #Set this to True to release a newer version (the demo won't update the endpoint to a newer model version by default)
existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), None
)
if existing_endpoint == None:
    print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config, ai_gateway=ai_gateway_config)
else:
  print(f"endpoint {serving_endpoint_name} already exist...")
  if force_update:
    w.serving_endpoints.update_config_and_wait(served_entities=endpoint_config.served_entities, name=serving_endpoint_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Post-fine-tuning evaluation
# MAGIC
# MAGIC The fine-tuned model was registered to Unity Catalog, and deployed to an endpoint with just a couple of clicks through the UI.
# MAGIC
# MAGIC ### Benchmarking recall & precision
# MAGIC
# MAGIC Let's now evaluate it again, comparing the new Precision and Recall compared to the baseline model.

# COMMAND ----------

# Run the predictions against the new finetuned endpoint
predictions, cleaned_predictions = extract_entities(df_test_small, serving_endpoint_name)
df_test_small['fine_tuned_predictions'] = cleaned_predictions
display(df_test_small[["sentence", "human_annotated_entities", "baseline_predictions", "baseline_predictions_cleaned", "fine_tuned_predictions"]])

# COMMAND ----------

# Compute precision & recall with the new model
def precision_recall_series(row):
  precision, recall = compute_precision_recall(row['fine_tuned_predictions'], row['human_annotated_entities'])
  return pd.Series([precision, recall], index=['precision', 'recall'])

df_test_small[['fine_tuned_precision', 'fine_tuned_recall']] = df_test_small.apply(precision_recall_series, axis=1)
df_test_small[['baseline_precision', 'fine_tuned_precision', 'baseline_recall', 'fine_tuned_recall']].describe()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Measuring token output
# MAGIC
# MAGIC Let's see if our new model behaves as expected.

# COMMAND ----------

df_test_small['baseline_predictions_len'] = df_test_small['baseline_predictions_cleaned'].apply(lambda x: len(x))
df_test_small['fine_tuned_predictions_len'] = df_test_small['fine_tuned_predictions'].apply(lambda x: len(x))
df_test_small[['baseline_predictions_len', 'fine_tuned_predictions_len']].describe()

# COMMAND ----------

# MAGIC %md 
# MAGIC We also slightly cut the output down, removing extra text (hence price) on top of improving accuracy!

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion:
# MAGIC
# MAGIC In this notebook, we saw how Databricks simplifies Fine Tuning and LLM deployment using Instruction Fine tuning for Entity Extraction.
# MAGIC
# MAGIC We covered how Databricks makes it easy to evaluate our performance improvement between the baseline and fine tuned model. 
# MAGIC
# MAGIC Fine Tuning can be applied to a wild range of use-cases. Using the Chat API simplifies fine tuning as the system will codify the prompt for us out of the box, use it whenever you can!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC License:
# MAGIC This datasets leverage the following Drug Dataset:
# MAGIC
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC @inproceedings{Tiktinsky2022ADF,
# MAGIC     title = "A Dataset for N-ary Relation Extraction of Drug Combinations",
# MAGIC     author = "Tiktinsky, Aryeh and Viswanathan, Vijay and Niezni, Danna and Meron Azagury, Dana and Shamay, Yosi and Taub-Tabib, Hillel and Hope, Tom and Goldberg, Yoav",
# MAGIC     booktitle = "Proceedings of the 2022 Conference of the North American Chapter of the Association for Computational Linguistics: Human Language Technologies",
# MAGIC     month = jul,
# MAGIC     year = "2022",
# MAGIC     address = "Seattle, United States",
# MAGIC     publisher = "Association for Computational Linguistics",
# MAGIC     url = "https://aclanthology.org/2022.naacl-main.233",
# MAGIC     doi = "10.18653/v1/2022.naacl-main.233",
# MAGIC     pages = "3190--3203",
# MAGIC }
# MAGIC ```
