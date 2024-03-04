# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Running inference at scale and in realtime
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-flow-5.png?raw=true" width="700px" style="float: right"/>
# MAGIC
# MAGIC We have now deployed our model to our Registry. The registry provides governance and ACL, simplifying and accelerating all downstream pipeline developments.
# MAGIC
# MAGIC Other teams don't have to worry about the model itself, they can focus on Ops task & model serving, while Data Scientists can release new models when then feel ready. 
# MAGIC
# MAGIC Models are typically used in 2 ways:
# MAGIC
# MAGIC - At scale, a cluster (in batch or streaming, including within Delta Live Tables)
# MAGIC - For realtime, low-latencies use-cases, served behind a REST API.
# MAGIC
# MAGIC Databricks provides and simplify both options.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fcomputer-vision-dl%2Finferences&dt=ML">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Batch & Streaming Model scoring
# MAGIC
# MAGIC Let's start with Batch / Streaming inference. We'll use spark distributed capabilities to run our inferences at scale.
# MAGIC
# MAGIC To do so, we need to load our model from the MLFlow registry, and build a Pandas UDF to distribute the inference on multiple instances (typically on GPUs).
# MAGIC
# MAGIC The first step consist on installing the model dependencies to make sure we're loading the model using the same librairies versions.

# COMMAND ----------

# DBTITLE 1,Load the pip requirement from the model registry
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os
requirements_path = ModelsArtifactRepository("models:/dbdemos_pcb_classification/Production").download_artifacts(artifact_path="requirements.txt") # download model requirement from remote registry

if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# DBTITLE 1,Install the requirements
# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# DBTITLE 1,Init the demo
# MAGIC %run ./_resources/00-init $reset_all_data=false $db=dbdemos $catalog=manufacturing_pcb

# COMMAND ----------

# DBTITLE 1,Load the model from the Registry
import torch
#Make sure to legerage the GPU when available
model_uri = "models:/dbdemos_pcb_classification/Production"
device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
pipeline = mlflow.transformers.load_model(model_uri, device=device.index)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Running our model inference locally (non distributed)
# MAGIC
# MAGIC Let's first run prediction locally on a standard pandas dataframe:

# COMMAND ----------

import io
from PIL import Image
from torchvision.transforms.functional import to_tensor, to_pil_image

#Call the pipeline and returns the main class with its probability
def predict_byte_series(content_as_byte_series, pipeline):
  #Transform as a list of PIL Images for our huggingface pipeline:
  image_list = content_as_byte_series.apply(lambda b: Image.open(io.BytesIO(b))).to_list()
  #the pipeline returns the probability for all the class
  predictions = pipeline.predict(image_list)
  #Filter & returns only the class with the highest score [{'score': 0.999038815498352, 'label': 'normal'}, ...]
  return pd.DataFrame([max(r, key=lambda x: x['score']) for r in predictions])  


df = spark.read.table("training_dataset_augmented").limit(50)
#Switch our model in inference mode
pipeline.model.eval()
with torch.set_grad_enabled(False):
  predictions = predict_byte_series(df.limit(10).toPandas()['content'], pipeline)
display(predictions)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Distribute the inference with Spark and a Pandas UDF (batch/streaming inference)
# MAGIC  
# MAGIC Let's parallelize our inferences by wrapping the function using a pandas UDF:

# COMMAND ----------

import numpy as np
import torch
from typing import Iterator

#Only batch the inferences in our udf by 1000 as images can take some memory
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000)
@pandas_udf("struct<score: float, label: string>")
def detect_damaged_pcb(images_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
  #pipeline = mlflow.transformers.load_model(model_uri, device=device.index) #Load pipeline outside for now to avoid error (ES-750064)
  #Switch pipeline to eval mode
  pipeline.model.eval()
  with torch.set_grad_enabled(False):
    for images in images_iter:
      yield predict_byte_series(images, pipeline)

# COMMAND ----------

display(df.select('filename', 'content').withColumn("prediction", detect_damaged_pcb("content")))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Realtime inferences with REST API
# MAGIC
# MAGIC Many use-case requires real-time capabilities. Think about realtime analysis in our PCB manufacturing system. A picture is taken and we need to instantly check for potential defects. 
# MAGIC
# MAGIC To be able to do that, we'll need to serve our inference behind a REST API. The system can then send our images, and the endpoint answer with the prediction.
# MAGIC
# MAGIC To do that, we'll have to encode our image byte as base64 and send them over the REST API.
# MAGIC
# MAGIC This implies that our model should decode the base64 back as PIL image. To make this easy, we can create a custom model wrapper having the transformers pipeline and a simple method transforming the base64 before calling our pipeline.
# MAGIC
# MAGIC This is done as usual extending the `mlflow.pyfunc.PythonModel` class:

# COMMAND ----------

# DBTITLE 1,Model Wrapper for base64 image decoding
from io import BytesIO
import base64

# Model wrapper
class RealtimeCVModelWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        # instantiate model in evaluation mode
        self.pipeline.model.eval()

    #images will contain a series of images encoded in b64
    def predict(self, context, images):
        with torch.set_grad_enabled(False):
          #Convert the base64 to PIL images
          images = images['data'].apply(lambda b: Image.open(BytesIO(base64.b64decode(b)))).to_list()
          predictions = self.pipeline(images)
          return pd.DataFrame([max(r, key=lambda x: x['score']) for r in predictions])
        
#Build our final hugging face pipeline. Load it as CPU as our endpoint will be cpu for now
pipeline_cpu = mlflow.transformers.load_model(model_uri, return_type="pipeline", device=torch.device("cpu").index)
rt_model = RealtimeCVModelWrapper(pipeline_cpu)

def to_base64(b):
  return base64.b64encode(b).decode("ascii")

#Let's try locally before deploying our endpoint to make sure it works as expected:
pdf = df.toPandas()

#Transform our input as a pandas dataframe containing base64 as this is what our serverless model endpoint will receive.
df_input = pd.DataFrame(pdf["content"].apply(to_base64).to_list(), columns=["data"])
predictions = rt_model.predict(None, df_input)
display(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that our wrapper is ready, let's deploy it as a new model in the registry.
# MAGIC
# MAGIC If realtime serving is your main usage, you would typically do that in the training step while registring your model. For this demo, we'll make 2 separate models: 1 for batch and 1 with the base64 wrapper for realtime inferences.

# COMMAND ----------

# DBTITLE 1,Save or RT model taking base64 in the registry
from mlflow.models.signature import infer_signature
init_experiment_for_batch("computer-vision-dl", "pcb")

with mlflow.start_run(run_name="hugging_face_rt") as run:
  signature = infer_signature(df_input, predictions)
  #log the model to MLFlow
  reqs = mlflow.pyfunc.log_model("model", python_model = rt_model, pip_requirements=requirements_path, input_example=df_input, signature = signature)
  mlflow.set_tag("dbdemos", "pcb_classification")
  mlflow.set_tag("rt", "true")

#Save the model in the registry & move it to Production
model_registered = mlflow.register_model("runs:/"+run.info.run_id+"/model", "dbdemos_pcb_classification")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Deploying a Model Endpoint Serving
# MAGIC
# MAGIC Our new model wrapper is available in the registry. We can deploy this new version as a model endpoint to start out realtime model serving.
# MAGIC

# COMMAND ----------

#Simple wrapper on top of the REST API. See the _resource/00-init companion notebook for more details or the Databricks endpoint API documentation
serving_client = EndpointApiClient()

# Start the endpoint using the REST API (you can do it using the UI directly)
# The first run might take some time as it's building the image. Further deployment are very fast even when scaled to zero.
serving_client.create_endpoint_if_not_exists("dbdemos_pcb_classification_endpoint", model_name="dbdemos_pcb_classification", model_version = model_registered.version, workload_size="Small", scale_to_zero_enabled=True, wait_start = True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Our endpoint is ready
# MAGIC
# MAGIC You can access and configure your endpoint using the [Model Serving UI](/#mlflow/endpoints/dbdemos_pcb_classification_endpoint) or the API. The Model Endpoint is serverless, stopping and starting almost instantly. In our case, we chose to scale it down to zero when not used (ideal for test/dev environements). 
# MAGIC
# MAGIC
# MAGIC *Note that Databricks Model Serving lets you host multiple model versions, simplifying A/B testing and new model deployment.*

# COMMAND ----------

import timeit

endpoint_url = f"{serving_client.base_url}/realtime-inference/dbdemos_pcb_classification_endpoint/invocations"
print(f"Sending requests to {endpoint_url}")
for i in range(3):
    rest_input = df_input[2*i:2*i+2]
    starting_time = timeit.default_timer()
    inferences = requests.post(endpoint_url, json={"dataframe_records": rest_input.to_dict(orient='records')}, headers=serving_client.headers).json()
    print(f"Inference time, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms")
    print("  "+str(inferences))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion
# MAGIC
# MAGIC We covered how Databricks makes it easy to deploy deep learning model at scale, including behind a REST endpoint with Databricks Serverless Model Serving.
# MAGIC
# MAGIC ### Next step: model explainability
# MAGIC
# MAGIC As next step, let's discover how to explain and highlight the pixels our model consider as damaged.
# MAGIC
# MAGIC Open the [04-explaining-inference notebook]($./04-explaining-inference) to discover how to use SHAP to analyze our prediction.
# MAGIC
# MAGIC ### Going further
# MAGIC
# MAGIC Working with huggingface might not be enough for you. For deeper, custom integration, you can also leverage libraries like pytorch.
# MAGIC
# MAGIC Open the [05-torch-lightning-training-and-inference]($./05-torch-lightning-training-and-inference) notebook to discover how to train and deploy a Pytorch model with [PyTorch Lightning](https://www.pytorchlightning.ai/index.html).
