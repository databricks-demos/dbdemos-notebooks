# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "computer-vision-pcb",
  "category": "data-science",
  "title": "Image classification - Fault detection",
  "description": "Deep Learning using Databricks Lakehouse: detect faults in PCBs with Hugging Face transformers and PyTorch Lightning.",
  "fullDescription": "Being able to analyze factory faults in real time is a critical task to increase production line quality and reducing defects.<br/>Implementing such a use case with deep learning for computer vision can be challenging at scale, especially when it comes to data preprocessing and building production-grade pipelines.<br/>Databricks simplifies this process end to end, making all the operational tasks simple so that you can focus on improving the model performance.<br/>In this demo, we will cover how to implement a complete deep learning pipeline to detect printed circuit board (PCB) defaults, from the image ingestion to real-time inferences (over REST API):<br /><br/><br/><ul><li>Simplify data and image ingestions using Databricks Auto Loader and Delta Lake</li><li>Learn how to do image preprocessing at scale</li><li>Train and deploy a computer vision pipeline with Hugging Face and the new Spark DataFrame data set for transformers</li><li>Deploy the pipeline for batch or streaming inferences and real-time serving with Databricks Serverless model endpoints</li><li>Understand which pixels are flagged as damaged PCBs to highlight potential default</li><li>A complete training and inference example using PyTorch Lightning if the Hugging Face library isn’t enough for your requirements, including deltatorch and distributed training with TorchDistributor</li></ul>",
  "usecase": "Data Science & AI",
  "products": ["Feature Store","MLFLow", "Auto ML"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Implement a Vision Language Model from Scratch", "url": "https://huggingface.co/blog/AviSoori1x/seemore-vision-language-model"}],
  "recommended_items": ["llm-dolly-chatbot", "pandas-on-spark", "mlops-end2end"],
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_computer_vision_dl",
  "demo_assets": [],
  "bundle": True,
  "tags": [{"ds": "Data Science"}],
  "notebooks": [
    {
      "path": "_resources/00-init",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "_resources/01-load-data",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Load demo dataset."
    },
    {
      "path": "00-introduction-deep-learning-vision", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Computer Vision - Introduction", 
      "description": "Start here to build your Deep Learning pipeline"
    },
    {
      "path": "01-ingestion-and-ETL", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Images ingestion and preparation", 
      "description": "Data pipeline to ingest and prepare training dataset"
    },
    {
      "path": "02-huggingface-model-training", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Model Training with Hugging Face", 
      "description": "Build, fine tune and deploy your transformers pipeline"
    },
    {
      "path": "03-running-cv-inferences", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Deploy the pipeline for inference", 
      "description": "Run batch/streaming or realtime inference (Model Endpoint)"
    },
    {
      "path": "04-explaining-inference", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Hihlight default in PCB image", 
      "description": "Add model explainer to highglight pixels having a potential default"
    },
    {
      "path": "05-torch-lightning-training-and-inference", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Train & deploy a Pytorch Lightning", 
      "description": "Extra example to train torch model, distributing on multiple node."
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_version": "15.4.x-gpu-ml-scala2.12",
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*, 4]"
    },
    "node_type_id": {"AWS": "Standard_NC4as_T4_v3", "AZURE": "Standard_NC4as_T4_v3", "GCP": "a2-highgpu-1g"},
    "driver_node_type_id": {"AWS": "Standard_NC4as_T4_v3", "AZURE": "Standard_NC4as_T4_v3", "GCP": "a2-highgpu-1g"},
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
