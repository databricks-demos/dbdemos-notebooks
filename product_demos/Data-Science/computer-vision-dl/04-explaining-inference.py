# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Explaining inferences to highlight PCB default
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-explainer.png?raw=true" width="500px" style="float: right"/>
# MAGIC
# MAGIC
# MAGIC Knowing that a PCB has been flagged as `damaged` by the model is a great first step. 
# MAGIC
# MAGIC Being able to highlight which part is considered as damaged in the picture can help further, providing extra information to the operator.
# MAGIC
# MAGIC This falls into the domain of model explainability. The most popular package for explanation are SHAP or LIME. 
# MAGIC
# MAGIC In this notebook, we'll use SHAP to explore how we can explain our huggingface pipeline prediction.
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fcomputer-vision-dl%2Fexplainer&dt=ML">

# COMMAND ----------

# MAGIC %pip install opencv-python
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Init the demo
# MAGIC %run ./_resources/00-init $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Load the pip requirements from the model registry
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

# Use the Unity Catalog model registry
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = f"{catalog}.{db}.dbdemos_pcb_classification"
MODEL_URI = f"models:/{MODEL_NAME}@Production"

# download model requirement from remote registry
requirements_path = ModelsArtifactRepository(MODEL_URI).download_artifacts(artifact_path="requirements.txt") 

if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# DBTITLE 1,Install the requirements
# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# DBTITLE 1,Load the model from the Registry
import torch
#Make sure to leverage the GPU when available
device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")

pipeline = mlflow.transformers.load_model(
  MODEL_URI, 
  device=device.index)
print(f"Model loaded from {MODEL_URI} to device {device}")

# COMMAND ----------

df = spark.read.table("training_dataset_augmented").limit(50).toPandas()

# COMMAND ----------

def nhwc_to_nchw(x: torch.Tensor) -> torch.Tensor:
    return x if x.shape[1] == 3 else x.permute(0, 3, 1, 2)

def nchw_to_nhwc(x: torch.Tensor) -> torch.Tensor:
    return x if x.shape[3] == 3 else x.permute(0, 2, 3, 1)

# COMMAND ----------

import io
from PIL import Image
import shap
import torchvision.transforms as tf
from torchvision.transforms.functional import to_tensor
mlflow.autolog(disable=True)
mean=[0.485, 0.456, 0.406]
std=[0.229, 0.224, 0.225]
transform = tf.Compose([
    tf.Lambda(lambda b: to_tensor(Image.open(io.BytesIO(b)))[None, :]),
    tf.Lambda(nhwc_to_nchw),
    tf.Resize((pipeline.image_processor.size['height'], pipeline.image_processor.size['width'])),
    tf.Normalize(mean=pipeline.image_processor.image_mean, std=pipeline.image_processor.image_std),
    tf.Lambda(nchw_to_nhwc),
])
inv_transform= tf.Compose([
    tf.Lambda(nhwc_to_nchw),
    tf.Normalize(mean = (-1 * np.array(pipeline.image_processor.image_mean) / np.array(pipeline.image_processor.image_std)).tolist(), 
                 std = (1 / np.array(pipeline.image_processor.image_std)).tolist()),
    tf.Lambda(nchw_to_nhwc),
])

#N
def hf_model_wrapper(img_vector):
  img_vector = torch.from_numpy(img_vector)
  revert_img_back = img_vector.permute(0, 3, 1, 2).to(device)
  output = pipeline.model(revert_img_back)
  return output.logits

class_names = ['damaged', 'normal']
mask_size = transform(df.iloc[0]['content'])[0].size()
masker_blur = shap.maskers.Image("blur(128,128)", mask_size)

# create an explainer with model and image masker
explainer = shap.Explainer(hf_model_wrapper, masker_blur, output_names=class_names)


def explain_image(image_to_explain, explainer, class_names):
  topk = 4
  batch_size = 50
  n_evals = 10000
  # feed only one image
  # here we explain two images using 100 evaluations of the underlying model to estimate the SHAP values
  shap_values = explainer(image_to_explain, max_evals=n_evals, batch_size=batch_size,
                          outputs=shap.Explanation.argsort.flip[:topk])
  
  shap_values.data = inv_transform(shap_values.data).cpu().numpy()
  shap_values.values = [val for val in np.moveaxis(shap_values.values,-1, 0)]

  shap.image_plot(shap_values=shap_values.values, pixel_values=shap_values.data, labels=class_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluating a damaged PCB 
# MAGIC We can see in red the damaged part (clearly identifiable in the picture )

# COMMAND ----------

# DBTITLE 1,PCB number 010 has been flagged as damaged.
# We can clearly see in red the part where the anomaly is, and in blue its oposite (not contributing to 'normal').
test = spark.read.table("training_dataset_augmented").where("filename = '010.JPG'").toPandas()
image_bytes = test.iloc[0]['content']
predictions =  pipeline(Image.open(io.BytesIO(image_bytes)))
print(f"Prediction for image 010.JPG: {predictions}")

explain_image(transform(image_bytes), explainer, class_names)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion
# MAGIC
# MAGIC Not only we can predict our damaged parts, but we can also understand where the damage is and help resolve defects!
# MAGIC
# MAGIC ### Going further
# MAGIC
# MAGIC Learn how to implement a Computer Vision model with Databricks and pytorch lightning: [05-ADVANCED-pytorch-training-and-inference]($./05-ADVANCED-pytorch-training-and-inference)
