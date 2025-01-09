# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Detecting Severity of Damaged Vehicle Using Accident Images
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-flow-4.png?raw=true" style="float: right" width="1000px">
# MAGIC
# MAGIC At this point, our data ingestion pipeline has brought in the relevant datasets including policy, claim, and telematics data. 
# MAGIC
# MAGIC To improve our claim processing, we want to be able to automatically detect the incident severity and use this information in our process rules.
# MAGIC
# MAGIC ### Leveraging Databricks Deep Learning AI capabilities on unstructured images
# MAGIC
# MAGIC When our customers fill claims, they can add images of the incident using their smartphone. These images are captured and saved as part of the claims.
# MAGIC
# MAGIC In this notebook, we will be using a training dataset containing images with the corresponding incident severity as label (good condition, minor damages or major damages). 
# MAGIC
# MAGIC We'll leverage the Data Intelligence Platform capabilities to fine-tune a state of the art model (`ResNet-50`) to classify our claims images. 
# MAGIC
# MAGIC The model is an AI asset that will be saved in a catalog under Unity Catalog for <b> centralized governance and control.</b>
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=02.1-Model-Training&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.39.0 datasets==2.20.0 transformers==4.42.4 tf-keras==2.17.0 accelerate==0.32.1 mlflow==2.19.0 torchvision==0.20.1 deepspeed==0.14.4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading and preparing our dataset from Unity Catalog Volume
# MAGIC
# MAGIC Let's start by reviewing our training dataset. The raw images have been stored and secured using Databricks Volumes, under your Unity Catalog `catalog`.`schema`.
# MAGIC
# MAGIC Let's ingest our raw images and save them as a Delta table within Unity Catalog. We'll apply some transformation to prepare our training dataset. 
# MAGIC
# MAGIC As our Deep learning model ResNet-50 was trained on 224x224 images, we will resize each picture, adding black background if required. This process will parallelize using spark User Defined Functions (UDF), persisting the binary data back into a Delta Lake table.

# COMMAND ----------

from pyspark.sql.functions import regexp_extract
training_df = spark.read.format('binaryFile').load(f"/Volumes/{catalog}/{db}/{volume_name}/Images")
#Extract label from image name
training_df = training_df.withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)\.png$", 2))
display(training_df.limit(1))

# COMMAND ----------

# DBTITLE 1,Image resizing and preparation
import io
from pyspark.sql.functions import pandas_udf, col
IMAGE_RESIZE = 224

#Resize UDF function
@pandas_udf("binary")
def resize_image_udf(content_series):
  def resize_image(content):
    from PIL import Image
    """resize image and serialize back as jpeg"""
    #Load the PIL image
    image = Image.open(io.BytesIO(content))
    width, height = image.size   # Get dimensions
    new_size = min(width, height)
    # Crop the center of the image
    image = image.crop(((width - new_size)/2, (height - new_size)/2, (width + new_size)/2, (height + new_size)/2))
    #Resize to the new resolution
    image = image.resize((IMAGE_RESIZE, IMAGE_RESIZE), Image.NEAREST)
    #Save back as jpeg
    output = io.BytesIO()
    image.save(output, format='JPEG')
    return output.getvalue()
  return content_series.apply(resize_image)


# add the metadata to enable the image preview
image_meta = {"spark.contentAnnotation" : '{"mimeType": "image/jpeg"}'}

(training_df
      .withColumn("content", resize_image_udf(col("content")).alias("content", metadata=image_meta))
      .write.mode('overwrite').saveAsTable("training_dataset"))

# COMMAND ----------

display(spark.table("training_dataset").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Fine tuning our model with Hugging Face transformers (transfer learning)
# MAGIC
# MAGIC Our dataset is now ready, and properly labeled.
# MAGIC
# MAGIC Let's load our Delta Table containing the images and prepare them for hugging face. 
# MAGIC
# MAGIC Databricks makes it easy to convert a Table to a transformers Dataset using the `dataset` libraries.
# MAGIC
# MAGIC We'll then retrain the base model from its latest checkpoint.

# COMMAND ----------

from datasets import Dataset
#Setup the training experiment
DBDemos.init_experiment_for_batch("lakehouse-fsi-smart-claims", "hf")

#Note: from_spark support coming with serverless compute - we'll use from_pandas for this simple demo having a small dataset
#dataset = Dataset.from_spark(spark.table("training_dataset"), cache_dir="/tmp/hf_cache/train").rename_column("content", "image")
dataset = Dataset.from_pandas(spark.table("training_dataset").toPandas()).rename_column("content", "image")

splits = dataset.train_test_split(test_size=0.2, seed = 42)
train_ds = splits['train']
val_ds = splits['test']

# COMMAND ----------

# DBTITLE 1,Prepare the dataset for our model
import torch
from transformers import AutoFeatureExtractor, AutoImageProcessor

# pre-trained model from which to fine-tune
# Check the hugging face repo for more details & models: https://huggingface.co/microsoft/resnet-50
model_checkpoint = "microsoft/resnet-50"

from PIL import Image
import io
from torchvision.transforms import CenterCrop, Compose, Normalize, RandomResizedCrop, Resize, ToTensor, Lambda

#Extract the model feature (contains info on pre-process step required to transform our data, such as resizing & normalization)
#Using the model parameters makes it easy to switch to another model without any change, even if the input size is different.
model_def = AutoFeatureExtractor.from_pretrained(model_checkpoint)

#Transformations on our training dataset. we'll add some crop here
transforms = Compose([Lambda(lambda b: Image.open(io.BytesIO(b)).convert("RGB")), #byte to pil
                        ToTensor(), #convert the PIL img to a tensor
                        Normalize(mean=model_def.image_mean, std=model_def.image_std)
                        ])

# Add some random resiz & transformation to our training dataset
def preprocess(batch):
    """Apply train_transforms across a batch."""
    batch["image"] = [transforms(image) for image in batch["image"]]
    return batch
   
#Set our training / validation transformations
train_ds.set_transform(preprocess)
val_ds.set_transform(preprocess)

# COMMAND ----------

# DBTITLE 1,Load the model
from transformers import AutoModelForImageClassification, TrainingArguments, Trainer

#Mapping between class label and value (huggingface use it during inference to output the proper label)
label2id, id2label = dict(), dict()
for i, label in enumerate(set(dataset['label'])):
    label2id[label] = i
    id2label[i] = label
    
#Load the base model from its checkpoint
model = AutoModelForImageClassification.from_pretrained(
    model_checkpoint, 
    label2id=label2id,
    id2label=id2label,
    num_labels=len(label2id),
    ignore_mismatched_sizes = True # provide this in case you're planning to fine-tune an already fine-tuned checkpoint
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fine tuning our model 
# MAGIC
# MAGIC Our dataset and model is ready. We can now start the training step to fine-tune the model.
# MAGIC
# MAGIC *Note that for production-grade use-case, we would typically to do some [hyperparameter](https://huggingface.co/docs/transformers/hpo_train) tuning here. We'll keep it simple for this first example and run it with fixed settings.*
# MAGIC

# COMMAND ----------

model_name = model_checkpoint.split("/")[-1]

from transformers import TrainingArguments
args = TrainingArguments(
    f"/tmp/huggingface/pcb/{model_name}-finetuned",
    no_cuda=True, #Run on CPU for resnet to make it easier
    remove_unused_columns=False,
    evaluation_strategy = "epoch",
    save_strategy = "epoch",
    num_train_epochs=20,
    load_best_model_at_end=True
)

# COMMAND ----------

# DBTITLE 1,Model wrapper to package our transform steps with the model
import mlflow
# This wrapper adds steps before and after the inference to simplify the model usage
# Before calling the model: apply the same transform as the training, resizing the image
# After callint the model: only keeps the main class with the probability as output
class ModelWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        # instantiate model in evaluation mode
        self.pipeline.model.eval()

    def predict(self, context, images):
        from PIL import Image
        with torch.set_grad_enabled(False):
            #Convert the byte to PIL images
            images = images['content'].apply(lambda b: Image.open(io.BytesIO(b))).to_list()
            #the pipeline returns the probability for all the class
            predictions = self.pipeline.predict(images)
            #Filter & returns only the class with the highest score [{'score': 0.999038815498352, 'label': 'normal'}, ...]
            return pd.DataFrame([max(r, key=lambda x: x['score']) for r in predictions])

# COMMAND ----------

# DBTITLE 1,Start our Training and log the model to MLFlow
from transformers import pipeline, DefaultDataCollator, EarlyStoppingCallback
from mlflow.models import infer_signature

mlflow.autolog(disable=True)
with mlflow.start_run(run_name="hugging_face") as run:
  mlflow.log_input(mlflow.data.from_huggingface(train_ds, "training"))
  def collate_fn(examples):
    pixel_values = torch.stack([e["image"] for e in examples])
    labels = torch.tensor([label2id[e["label"]] for e in examples], dtype=torch.float)
    labels = torch.nn.functional.one_hot(labels.long(), 3).float()
    return {"pixel_values": pixel_values, "labels": labels}

  trainer = Trainer(model, args, train_dataset=train_ds, eval_dataset=val_ds, tokenizer=model_def, data_collator=collate_fn) 

  train_results = trainer.train()
  #Build our final hugging face pipeline
  classifier = pipeline("image-classification", model=trainer.state.best_model_checkpoint, tokenizer = model_def)

  #Wrap the model to easily ingest images and output better results
  wrapped_model = ModelWrapper(classifier)
  test_df = spark.table("training_dataset").select('content').toPandas()
  predictions = wrapped_model.predict(None, test_df)
  signature = infer_signature(test_df, predictions)
    
  reqs = mlflow.transformers.get_default_pip_requirements(model)
  #log the model to MLFlow
  mlflow.pyfunc.log_model(artifact_path="model", python_model=wrapped_model, pip_requirements=reqs, signature=signature)
  mlflow.log_metrics(train_results.metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's now save our model to Unity Catalog
# MAGIC
# MAGIC Our model is ready, we can easily add it to Unity Catalog and ensure other users can access it.
# MAGIC
# MAGIC Our Data Engineers or MLOps team will be able to load it to run the inferences.

# COMMAND ----------

from mlflow.tracking import MlflowClient

model_name = "dbdemos_claims_damage_level"

#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()
#Add model within our catalog
latest_model = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
# Flag it as Production ready using UC Aliases
client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Test by loading the model for inferencing

# COMMAND ----------

#Load back the model
predict_damage_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@prod")
columns = predict_damage_udf.metadata.get_input_schema().input_names()
#Run the inferences
predictions = spark.table('training_dataset').withColumn("damage_prediction", predict_damage_udf(*columns)).cache()
display(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model validation
# MAGIC We can validate our model using simple visualizations that we store on MLFlow against our actual model binary, input parameters and metrics. This context will be useful when reviewing / auditing our approach.
# MAGIC
# MAGIC Given the small amount of data our training dataset has, the model is good enough to ship it and start running inferences. 

# COMMAND ----------

results = predictions.selectExpr("path", "label", "damage_prediction.label as predictions", "damage_prediction.score as score").toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# create confusion matrix
confusion_matrix = pd.crosstab(results['label'], results['predictions'])

# plot confusion matrix
fig = plt.figure()
sns.heatmap(confusion_matrix, annot=True, cmap="Blues", fmt='d')
client.log_figure(run.info.run_id, fig, "confusion_matrix.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC We demonstrated how Databricks handle unstructured text and let you fine tune Deep Learning model, leveraging <b> Delta and MLFlow </b> to make process easy and reproducible. 
# MAGIC
# MAGIC Our fine-tunbed model is now saved within Unity Catalog. Subsequent access and changes to it can be audited and managed centrally. 
# MAGIC
# MAGIC Now that the model is available, let's see how we can use it for inferencing on new image data.
# MAGIC
# MAGIC Open the [02.2-Batch-Scoring]($./02.2-Batch-Scoring) notebook to compute the incident severity on all our claims.
