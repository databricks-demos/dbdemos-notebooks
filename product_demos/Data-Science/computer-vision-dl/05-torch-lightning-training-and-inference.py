# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Distributed Model training with torch lightning
# MAGIC
# MAGIC In this demo, we will cover how to leverage Lightning to distribute our model training using multiple instances.
# MAGIC
# MAGIC We will cover the same steps as what we did with Hugging Face transformers library, but implementing the training steps ourselves.
# MAGIC
# MAGIC This notebook is more advanced as the Hugging Face one, but gives you more control over the training.
# MAGIC
# MAGIC %md-sandbox
# MAGIC <div style="background-color: #d9f0ff; border-radius: 10px; padding: 15px; margin: 10px 0; font-family: Arial, sans-serif;">
# MAGIC   <strong>Note:</strong> This advanced deep learning content has been tailored to work on GPUs using Databricks Classic compute (not serverless). <br/>
# MAGIC   We'll revisit this content soon to support the serverless AI runtime - stay tuned.
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fcomputer-vision-dl%2Flightning&dt=ML">

# COMMAND ----------

# DBTITLE 1,Setup - Install deltatorch  & lightning
# MAGIC %pip install pytorch-lightning==2.5.0 git+https://github.com/delta-incubator/deltatorch.git databricks-sdk==0.39.0 datasets==2.20.0 transformers==4.49.0 tf-keras==2.17.0 accelerate==1.4.0 mlflow==2.20.2 torchvision==0.20.1 deepspeed==0.14.4 evaluate==0.4.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Demo setup
# MAGIC %run ./_resources/00-init $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Review our training dataset
df = spark.read.table("training_dataset_augmented")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create our Dataset
# MAGIC
# MAGIC In this example, we will be using Deltatorch to load our dataset. Deltatorch reads directly from Delta Lake tables using a native reader. 
# MAGIC
# MAGIC Deltatorch splits the dataframe in multiple chuncks to spread it across multiple nodes in an efficient way. This makes distributed training on multi-node clusters easy. 
# MAGIC
# MAGIC To be able to do this split in the most efficient way, deltatorch needs a unique, incremental ID without gaps. We can add it to our existing dataset with a rank and directly split our test/training datasets:

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import monotonically_increasing_id, when, row_number, rand

training_df = df.withColumnRenamed("label", "labelName") \
                .withColumn("label", when(col("labelName").eqNullSafe("normal"), 0).otherwise(1))

train, test = training_df.randomSplit([0.7, 0.3], seed=42)

# Split the data and write them in a dbfs folder or S3 bucket available by all our nodes. This could be an external location.
# Note that for deltatorch to be able to split and distribute the data, we're adding an extra incremental id column.
w = Window().orderBy(rand())

train_path = f"/Volumes/{catalog}/{db}/{volume_name}/pcb_torch_delta/train"
test_path = f"/Volumes/{catalog}/{db}/{volume_name}/pcb_torch_delta/test"

train.withColumn("id", row_number().over(w)).write.mode("overwrite").save(train_path)
test.withColumn("id", row_number().over(w)).write.mode("overwrite").save(test_path)

# COMMAND ----------

# DBTITLE 1,Data Loaders using deltatorch
# Deltatorch makes it easy to load Delta Dataframe to torch and efficiently distribute it among multiple nodes.
# This requires deltatorch to be installed. 
# Note: For small dataset, a LightningDataModule example directly using hugging face transformers is also available in the _resources/00-init notebook.
from PIL import Image
import pytorch_lightning as pl
from deltatorch import create_pytorch_dataloader
from deltatorch import FieldSpec
import torchvision.transforms as tf

class DeltaDataModule(pl.LightningDataModule):
    #Creating a Data loading module with Delta Torch loader 
    def __init__(self, train_path, test_path):
        self.train_path = train_path 
        self.test_path = test_path 
        super().__init__()

        self.transform = tf.Compose([
                tf.Lambda(lambda x: x.convert("RGB")),
                tf.Resize(256),
                tf.CenterCrop(224),
                tf.ToTensor(),
                tf.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
            ])
        
    def dataloader(self, path: str, batch_size=32):
        return create_pytorch_dataloader(
            path,
            id_field="id",
            fields=[
                FieldSpec("content", load_image_using_pil=True, transform=self.transform),
                FieldSpec("label"),
            ],
            shuffle=True,
            batch_size=batch_size,
        )

    def train_dataloader(self):
        return self.dataloader(self.train_path, batch_size=64)

    def val_dataloader(self):
        return self.dataloader(self.test_path, batch_size=64)

    def test_dataloader(self):
        return self.dataloader(self.test_path, batch_size=64)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define our base model

# COMMAND ----------

import io
import torch
from torchvision import models
from torchmetrics import Accuracy
from torch.nn import functional as nnf

class CVModel(pl.LightningModule):
    """
    We are going to define our model class here 
    """
    def __init__(self, num_classes: int = 2, learning_rate: float = 2e-4, momentum:float=0.9, family:str='mobilenet'):
        super().__init__()

        self.save_hyperparameters() # LightningModule allows you to automatically save all the hyperparameters 
        self.learning_rate = learning_rate
        self.momentum = momentum
        self.accuracy = Accuracy(task="multiclass", num_classes=num_classes)
        self.model = self.get_model(num_classes, learning_rate, family)
        self.family = family
 
    def get_model(self, num_classes, learning_rate, family):
        """
        
        This is the function that initialises our model.
        If we wanted to use other prebuilt model libraries like timm we would put that model here
        """
        if family == 'mobilenet':
            model = models.mobilenet_v2(pretrained=True)
        elif family == 'resnext':
            model = models.resnext50_32x4d(pretrained=True)
        
        # Freeze parameters in the feature extraction layers and replace the last layer
        for param in model.parameters():
            param.requires_grad = False
    
        # New modules have `requires_grad = True` by default
        if family == 'mobilenet':
            model.classifier[1] = torch.nn.Linear(model.classifier[1].in_features, num_classes)
        elif family == 'resnext':
            model.fc = torch.nn.Linear(model.fc.in_features, num_classes)
        return model

    def forward(self, x):
        x = self.model(x)
        return x

    def training_step(self, batch, batch_idx):
        x = batch["content"]
        y = batch["label"]
        pred = self(x)
        loss = nnf.cross_entropy(pred, y)
        acc = self.accuracy(pred, y)
        self.log("train_loss", torch.tensor([loss]), on_step=True, on_epoch=True, logger=True)
        self.log("train_acc", torch.tensor([acc]), on_step=True, on_epoch=True, logger=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x = batch["content"]
        y = batch["label"]
        pred = self(x)
        loss = nnf.cross_entropy(pred, y)
        acc = self.accuracy(pred, y)
        self.log("val_loss", torch.tensor([loss]), prog_bar=True)
        self.log("val_acc", torch.tensor([acc]), prog_bar=True)
        return {"loss": loss, "acc": acc}

    def configure_optimizers(self):
        if self.family == 'mobilenet':
            params = self.model.classifier[1].parameters()
        elif self.family == 'resnext':
            params = self.model.fc.parameters()
        
        optimizer = torch.optim.SGD(params, lr=self.learning_rate, momentum=self.momentum)
        
        return optimizer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model training
# MAGIC
# MAGIC The following cells implement an ML model leveraging pytorch. 
# MAGIC
# MAGIC The model is a standard Computer Vision one, implemented using PyTorch Lightning. For more details, it is defined in the [utils]($./_resources/01-utils) notebook.
# MAGIC
# MAGIC Note that we'll be using MLflow to automatically log our experiment metrics and register our final model.
# MAGIC
# MAGIC We can train our model in three different ways.
# MAGIC - Single-node, single-GPU training.
# MAGIC - Single-node, multiple-GPU training.
# MAGIC - Multinode training, leveraging the `TorchDistributor` included in PySpark ML in DBR ML > 13.0
# MAGIC
# MAGIC As mentioned [here](https://docs.databricks.com/machine-learning/train-model/dl-best-practices.html#best-practices-for-training-deep-learning-models), it's usually better to start by using a single node on a GPU and then increasing the number of GPUs in a single node if needed. If the dataset is large enough to make training slow on a single machine, consider moving to multi-GPU and even distributed compute.
# MAGIC
# MAGIC A Single Node (driver only) GPU cluster is typically fastest and most cost-effective for deep learning model development. One node with 4 GPUs is likely to be faster for deep learning training that 4 worker nodes with 1 GPU each. This is because distributed training incurs network communication overhead.

# COMMAND ----------

# MAGIC %md
# MAGIC The following `train` function can be used as it is to perform single-node training, with one or multiple GPUs, leveraging DDP. And also can be used with the aforementioned `TorchDistributor` for multinode training. Bear in mind that `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables need to be defined inside the function, so they can be fed to the parallel subprocesses when running it distributed.

# COMMAND ----------

# MAGIC %md ### Train function

# COMMAND ----------

from pytorch_lightning.loggers import MLFlowLogger
from mlflow.utils.file_utils import TempDir
import cloudpickle
MAX_EPOCH_COUNT = 20
BATCH_SIZE = 16
STEPS_PER_EPOCH = 2
EARLY_STOP_MIN_DELTA = 0.01
EARLY_STOP_PATIENCE = 10
NUM_WORKERS = 8

xp = DBDemos.init_experiment_for_batch("computer-vision-dl", "pcb_torch")
## Specifying the mlflow host server and access token 
# We put them to a variable to feed into horovod later on
db_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

ckpt_path = f"/Volumes/{catalog}/{db}/{volume_name}/cv_torch/checkpoints"

def train_model(dm, num_gpus=1, single_node=True):
    # We put them into these environment variables as this is where mlflow will look by default
    os.environ['DATABRICKS_HOST'] = db_host
    os.environ['DATABRICKS_TOKEN'] = db_token
    torch.set_float32_matmul_precision("medium")
    if single_node or num_gpus == 1:
        num_devices = num_gpus
        num_nodes = 1
        strategy="auto"
    else:
        num_devices = 1
        num_nodes = num_gpus
        strategy = 'ddp_notebook' # check this is ddp or ddp_notebook

    checkpoint_callback = pl.callbacks.ModelCheckpoint(
        save_top_k=2,
        mode="min",
        monitor="val_loss", # this has been saved under the Model Trainer - inside the validation_step function 
        dirpath=ckpt_path,
        filename="sample-cvops-{epoch:02d}-{val_loss:.2f}"
    )
    early_stop_callback = pl.callbacks.EarlyStopping(
        monitor="train_loss",
        min_delta=EARLY_STOP_MIN_DELTA,
        patience=EARLY_STOP_PATIENCE,
        stopping_threshold=0.001,
        strict=True,
        verbose=True,
        mode="min",
        check_on_train_epoch_end=True,
        log_rank_zero_only=True
    )

    tqdm_callback = pl.callbacks.TQDMProgressBar(
        refresh_rate=STEPS_PER_EPOCH,
        process_position=0
    )
    # make your list of choices that you want to add to your trainer 
    callbacks = [early_stop_callback, checkpoint_callback, tqdm_callback]

    # AutoLog does not work with DDP 
    mlflow.pytorch.autolog(disable=False, log_models=False)
    with mlflow.start_run(run_name="torch") as run:
      
      mlf_logger = MLFlowLogger(experiment_name=xp.name, run_id=run.info.run_id)
      # Initialize a trainer
      trainer = pl.Trainer(
          default_root_dir=ckpt_path,
          accelerator="gpu",
          max_epochs=50,
          check_val_every_n_epoch=2,
          devices=num_devices,
          callbacks=[early_stop_callback, checkpoint_callback, tqdm_callback],
          strategy=strategy,
          num_nodes=num_nodes,
          logger=mlf_logger)

      print(f"Global Rank: {trainer.global_rank} - Local Rank: {trainer.local_rank} - World Size: {trainer.world_size}")
      device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
      model = CVModel(2)
      model.to(device)
      
      trainer.fit(model, dm)
      print("Training is done ")
      val_metrics = trainer.validate(model, dataloaders=dm.val_dataloader(), verbose=False)
      #index of the current process across all nodes and devices. Only log on rank 0
      if trainer.global_rank == 0:
        print("Logging our model")
        reqs = mlflow.pytorch.get_default_pip_requirements() + ["pytorch-lightning==" + pl.__version__]
        mlflow.pytorch.log_model(artifact_path="model", pytorch_model=model.model, pip_requirements=reqs)
        #Save the test/validate transform as we'll have to apply the same transformation in our pipeline.
        #We could alternatively build a model wrapper to encapsulate these transformations as part as of our model (that's what we did with the huggingface implementation).
        with TempDir(remove_on_exit=True) as local_artifacts_dir:
          # dump tokenizer to file for mlflow
          transform_path = local_artifacts_dir.path("transform.pkl")
          with open(transform_path, "wb") as fd:
            cloudpickle.dump(dm.transform, fd)
          mlflow.log_artifact(transform_path, "model")
        mlflow.set_tag("dbdemos", "cpb_torch")
        #log and returns model accuracy
        mlflow.log_metrics(val_metrics[0])
        return run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Single node training
# MAGIC The number of GPUs can be easily changed when running the function.

# COMMAND ----------

delta_dataloader = DeltaDataModule(train_path, test_path)

# COMMAND ----------

run = train_model(delta_dataloader, 1, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multinode training
# MAGIC For scaling to multiple nodes, we only have to use `TorchDistributor` to run our `train` function, with no further changes needed

# COMMAND ----------

# Note: we won't run this as our demo starts a single node GPU by default. To make it work, starts a cluster with multiple GPU instead!
# For now disable autolog with TorchDistributor
# mlflow.pytorch.autolog(disable=True)
# distributed = TorchDistributor(num_processes=2, local_mode=False, use_gpu=True)
# distributed.run(train_model, dm, 1, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model deployment
# MAGIC
# MAGIC Our model is now trained. All we have to do is get the best model (based on the `train_acc` metric) and deploy it in MLFlow registry.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registering our model
# MAGIC

# COMMAND ----------

#Save the model in the registry & move it to Production

# Register models in Unity Catalog
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = f"{catalog}.{db}.dbdemos_pcb_torch_classification"

model_registered = mlflow.register_model("runs:/"+run.info.run_id+"/model", MODEL_NAME)
print("registering model version "+model_registered.version+" as production model")

## Alias the model version as the Production version
client = mlflow.tracking.MlflowClient()
client.set_registered_model_alias(
  name = MODEL_NAME, 
  version = model_registered.version,
  alias = "Production")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We have now deployed our model to our Registry. This will give model governance, simplifying and accelerating all downstream pipeline developments.
# MAGIC
# MAGIC The model is now ready to be used in any data pipeline (streaming, batch or real time with Databricks Model Serving).

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Model scoring
# MAGIC
# MAGIC After training, registering and finally flagging our model as production-ready, we can use it to infer labels on new data. We will show how to do it for:
# MAGIC * Batch inference, leveraging Pandas UDF (same approach would work for streaming, also using DLT)
# MAGIC * Real-time, leveraging Model Serving (ms latency)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Loading context and installing libraries
# MAGIC
# MAGIC Serving the model requires using the same library. You can get the dependencies from the MLFlow `requirements.txt` file. Check the Hugging face model serving demo for an example.

# COMMAND ----------

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import pandas_udf
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

# Use the Unity Catalog model registry
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = f"{catalog}.{db}.dbdemos_pcb_torch_classification"
MODEL_URI = f"models:/{MODEL_NAME}@Production"

# download model requirement from remote registry
requirements_path = ModelsArtifactRepository(MODEL_URI).download_artifacts(artifact_path="requirements.txt") 

#%pip install $requirements_path

#loaded_model = torch.load(local_path+"data/model.pth", map_location=torch.device(device))

# COMMAND ----------

# MAGIC  %md ### Pandas UDF, for batch/streaming inference
# MAGIC  Using Pandas UDF, we can apply our model to any number of rows on our DataFrame. 
# MAGIC  
# MAGIC  For easier distribution, we are going to broadcast our model. This can significantly speed up things for big datasets.

# COMMAND ----------

model = mlflow.pytorch.load_model(MODEL_URI)
device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
model.to(device)
print(f"Model loaded from {MODEL_URI} to device {device}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Try our model inference with a pandas dataframe (non distributed)
# MAGIC
# MAGIC Let's first run prediction locally on a pandas dataframe:

# COMMAND ----------

# DBTITLE 1,Load back the transform
# Again, another option is to ship this as part of the model so that the transformation is done in the model directly.
transform_path = ModelsArtifactRepository(MODEL_URI).download_artifacts(artifact_path="transform.pkl") 

with open(transform_path, "rb") as f:
  transform = cloudpickle.loads(f.read())

# COMMAND ----------

id2label = {0: "normal", 1: "anomaly"}
def predict_byte_series(content_as_byte_series, model, device = torch.device("cpu")):
  #Transform the bytes as PIL images
  image_list = content_as_byte_series.apply(lambda b: Image.open(io.BytesIO(b))).to_list()
  #Apply our transformations & stack them for our model
  vector_images = torch.stack([transform(b).to(device) for b in image_list])
  #Call the model to get our inference
  outputs = model(vector_images)
  #Returns a proper results with Score/Label/Name
  preds = torch.max(outputs, 1)[1].tolist()
  probs = torch.nn.functional.softmax(outputs, dim=-1)[:, 1].tolist()
  return pd.DataFrame({"score": probs, "label": preds, "labelName": [id2label[p] for p in preds]})

df = spark.read.table("training_dataset_augmented")
display(predict_byte_series(df.limit(10).toPandas()['content'], model, device))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Distribute the inference with Spark and a Pandas UDF (batch/streaming inference)
# MAGIC  
# MAGIC Let's parallelize our inferences by wrapping the function using a pandas UDF:

# COMMAND ----------

@pandas_udf("struct<score: float, label: int, labelName: string>")
def detect_damaged_pcb(images_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    device = torch.device("cuda:0") if torch.cuda.is_available() else torch.device("cpu")
    model = mlflow.pytorch.load_model(MODEL_URI)
    model.to(device)
    model.eval()
    with torch.set_grad_enabled(False):
        for images in images_iter:
            yield predict_byte_series(images, model, device)

# COMMAND ----------

# Reduce the number of images we send at once to avoid memory issue
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000)

display(df.select('filename', 'content').withColumn("prediction", detect_damaged_pcb("content")).limit(50))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Model Serving, for real-time inference
# MAGIC
# MAGIC The second option is to use Databricks Model Serving capabilities. Within 1 click, Databricks will start a serverless REST API serving the model define in MLFlow. You just have to open your model registry and click on Model Serving.
# MAGIC
# MAGIC This will grant you real-time capabilities without any infra setup.
# MAGIC
# MAGIC To include image preprocessing in the inference step, we are using an MLflow model wrapper.

# COMMAND ----------

from io import BytesIO
import base64

# Model wrapper
class RealtimeCVTorchModelWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, model):
        self.model = model
        # instantiate model in evaluation mode
        self.model.eval()

    def predict(self, context, images):
        with torch.set_grad_enabled(False):
          #Convert the base64 to PIL images
          images = images['data'].apply(lambda b: base64.b64decode(b))
          return predict_byte_series(images, self.model)


#Load it as CPU as our endpoint will be cpu for now
model_cpu = mlflow.pytorch.load_model(MODEL_URI).to(torch.device("cpu"))
rt_model = RealtimeCVTorchModelWrapper(model_cpu)

def to_base64(b):
  return base64.b64encode(b).decode("ascii")

#Let's try locally before deploying our endpoint to make sure it works as expected:
pdf = df.limit(10).toPandas()

#Transform our input as a pandas dataframe containing base64 as this is what our serverless model endpoint will receive.
df_input = pd.DataFrame(pdf["content"].apply(to_base64).to_list(), columns=["data"])
predictions = rt_model.predict(None, df_input)
display(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC Your model is now ready to be deployed using Databricks Model Endpoint. For more details on how this can be done, open the [03-running-cv-inferences notebooks]($./03-running-cv-inferences) and follow the same steps
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC We've seen how Databricks makes it easy to :
# MAGIC - Load and distribute Delta Lake tables using deltatorch
# MAGIC - Run distributed training with `TorchDistributor`
# MAGIC - Save our model to MLFlow
# MAGIC - Deploy our model in production for batch and realtime inferences
