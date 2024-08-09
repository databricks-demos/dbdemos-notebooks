# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Computer vision - quality inspection of PCB
# MAGIC
# MAGIC <div style="float:right">
# MAGIC <img width="300px" src="https://raw.githubusercontent.com/databricks-industry-solutions/cv-quality-inspection/main/images/PCB1.png">
# MAGIC </div>
# MAGIC
# MAGIC In this demo, we will show you how Databricks can help you build end to end a computer vision model to inspect the quality of Printed Circuit Boards (PCBs). 
# MAGIC
# MAGIC We will use the [Visual Anomaly (VisA)](https://registry.opendata.aws/visa/) detection dataset, and build a pipeline to detect anomalies in our PCB images. 
# MAGIC
# MAGIC Computer vision is a field of study that has been advancing rapidly in recent years, thanks to the availability of large amounts of data, powerful GPUs, pre-trained deep learning models, transfer learning and higher level frameworks. However, training and serving a computer vision model can be very hard because of different challenges:
# MAGIC - Data ingestion and preprocessing at scale
# MAGIC - Data volumes that require multiple GPUs for training
# MAGIC - Governance and production-readiness requirement MLOps pipelines for the the end-to-end model lifecycle.
# MAGIC - Demanding SLAs for streaming or real-time inferences
# MAGIC
# MAGIC Databricks Lakehouse is designed to make this overall process simple, letting Data Scientists focus on the core use-case.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fcomputer-vision-dl%2Fintro&dt=ML">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Implementing our Deep Learning pipeline
# MAGIC
# MAGIC For building this model and easily solve those challenges, we will implement the following steps:
# MAGIC
# MAGIC - Ingestion pipeline with [Autoloader](https://docs.databricks.com/ingestion/auto-loader/index.html) to incrementally load and process our images into Delta Lake format.
# MAGIC - Build a ML model with [Hugging Face Transformers](https://huggingface.co/docs/transformers/index) using the Spark Dataset.
# MAGIC - Run inference in batch or real-time using PandasUDF and [Databricks Model Serving](https://docs.databricks.com/machine-learning/model-serving/index.html) for real-time inference.
# MAGIC - Advanced: complete Torch version with [Spark Torch Distributor](https://docs.databricks.com/machine-learning/train-model/distributed-training/spark-pytorch-distributor.html) for distributed training.
# MAGIC
# MAGIC ### Data flow
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-flow-0.png?raw=true" width="1100px" />
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1/ Ingestion and ETL
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-1.png?raw=true" width="500px" style="float: right" />
# MAGIC
# MAGIC Our first step is to ingest the images. We will leverage Databricks Autoloader to ingest the images and the labels (as csv file).
# MAGIC
# MAGIC Our data will be saved as a Delta Table, allowing easy governance with Databricks Unity Catalog.
# MAGIC
# MAGIC We'll also apply initial transformations to reduce the image size and prepare our dataset for model training.

# COMMAND ----------

# MAGIC %md 
# MAGIC Open the [01-ingestion-and-ETL notebook]($./01-ingestion-and-ETL)  to get started.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Building our model with huggingface transformer
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-2.png?raw=true" width="500px" style="float: right" />
# MAGIC
# MAGIC Now that our data is ready, we can leverage huggingface transformer library and do fine-tuning in an existing state-of-the art model.
# MAGIC
# MAGIC This is a very efficient first approach to quickly deliver results without having to go into pytorch details.

# COMMAND ----------

# MAGIC %md 
# MAGIC Open the [02-huggingface-model-training notebook]($./02-huggingface-model-training)  to train the model.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3/ Running inference in batch and deploying a realtime Serverless Model Endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-3.png?raw=true" width="500px" style="float: right" />
# MAGIC
# MAGIC Now that our model is created and available in our MLFlow registry, we'll be able to use it.
# MAGIC
# MAGIC Typical servinng use-case include:
# MAGIC
# MAGIC - Batch and Streaming use-cases (including with Delta Live Table)
# MAGIC - Realtime model serving with Databricks Serverless Endpoints
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [03-running-cv-inferences notebook]($./03-running-cv-inferences) to run distributed and realtime inferences.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 4/ Explaining our model prediction and highlighting damanged PCB pixels
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/computer-vision/deeplearning-cv-pcb-explainer.png?raw=true" width="500px" style="float: right" />
# MAGIC
# MAGIC Having inferences and score on each image is great, but our operators will need to know which part is considered as damaged for manual verification and potential fix.
# MAGIC
# MAGIC We will be using SHAP as explainer to highlight pixels having the most influence on the model prediction. 

# COMMAND ----------

# MAGIC %md
# MAGIC Open the [04-explaining-inference notebook]($./04-explaining-inference) to learn how to explain our inferences.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 4/ Going further with pytorch lightning and distributed workload
# MAGIC
# MAGIC More advanced use-cases might require leveraging other libraries such as pytorch or lightning. In this example, we will show how to implement a model fine tuning and inference with pytorch lightning.
# MAGIC
# MAGIC We will leverage delta-torch to easily build our torch dataset from the Delta tables.
# MAGIC
# MAGIC In addition, we'll also demonstrate how Databricks makes it easy to distribute the training on multiple GPUs using `TorchDistributor`.
# MAGIC
# MAGIC Open the [05-torch-lightning-training-and-inference]($./05-torch-lightning-training-and-inference) to see how to train your model & run distributed and realtime inferences.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC We covered how the Lakehouse is uniquely positioned to solve your ML and Deep Learning challenge:
# MAGIC
# MAGIC - Accelerate your data ingestion and transformation at scale
# MAGIC - Simplify model training and governance
# MAGIC - One-click model deployment for all use-cases, from batch to realtime inferences
