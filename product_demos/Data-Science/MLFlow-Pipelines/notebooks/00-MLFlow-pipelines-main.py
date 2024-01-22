# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow Regression Pipeline Databricks Notebook
# MAGIC 
# MAGIC This notebook demonstrate how to leverage MLFlow Pipeline to structure a MLF project training a regression model
# MAGIC 
# MAGIC MLFlow pipelines let you organize your project in a standardized way to:
# MAGIC 
# MAGIC * Simplify and accelerate project deployments with a best practices
# MAGIC * Faster computation with a Pipeline engine caching intermediate steps, only recomputing what's required
# MAGIC * Out of the box cards at each steps providing data & model training insight
# MAGIC * Full integration with MLFlow, saving all steps, cards and code for simple retrival 
# MAGIC 
# MAGIC For more information about the MLflow Regression Pipeline, including usage examples,
# MAGIC see the [Regression Pipeline overview documentation](https://mlflow.org/docs/latest/pipelines.html#regression-pipeline)
# MAGIC and the [Regression Pipeline API documentation](https://mlflow.org/docs/latest/python_api/mlflow.pipelines.html#module-mlflow.pipelines.regression.v1.pipeline).
# MAGIC 
# MAGIC ## Predicting insurance charges with MLFLow Pipeline
# MAGIC 
# MAGIC In this demo, we'll leverage MLFlow Pipeline to create a model predicting health insurance charge based on a couple of features (ex: age, cigarets per day etc)
# MAGIC 
# MAGIC 
# MAGIC MLFlow pipeline splits the model training and deployment in 5 steps:
# MAGIC 
# MAGIC * **Ingest**: Load data from any source
# MAGIC * **Split**: split data in test/train/validation
# MAGIC * **Transform**: add extra feature transformation (ex: OneHotEncoder)
# MAGIC * **Train**: train the model
# MAGIC * **Evaluate**: get model performance
# MAGIC * **Register**: save model in MLFlow once performance reach your threshold
# MAGIC 
# MAGIC All these steps are implemented under the `steps` folders, we'll go over these steps in details.
# MAGIC 
# MAGIC This is the flow MLFlow Pipeline is enforcing and that we'll use for our model training & deployment:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-0.png" width="1000px" />
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlflow-pipelines%2Fnotebook_main&dt=MLFLOW_PIPELINES">

# COMMAND ----------

# DBTITLE 1,Let's install mlflow pipelines and our requirements
# MAGIC %pip install mlflow[pipelines]
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leveraging Feature store
# MAGIC MLFlow Pipeline encourage separation between model training and feature preparation.
# MAGIC 
# MAGIC This means that our features should be ready at this stage and available in the feature store or as a Delta table in our catalog.
# MAGIC 
# MAGIC *Note that last-mile/adhoc feature preparation (ex: computing sin / cos on a date) can be added directly in the `transform` step (see below).*
# MAGIC 
# MAGIC For this demo, our feature table is available as a Delta Table: `insurance_charge`. Let's review our incoming dataset:
# MAGIC 
# MAGIC *Note: we could have used the Feature Store too*

# COMMAND ----------

# MAGIC %sql select * from insurance_charge

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 1: ingesting the data
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-1.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC MLFlow pipeline support built-in input:
# MAGIC 
# MAGIC * Parquet folder
# MAGIC * Delta Lake folder
# MAGIC * Spark SQL
# MAGIC 
# MAGIC For this demo, our features are available as a Delta Table in our catalog.  Therefore, we'll be using Spark SQL to load our training dataset. If you need to load a more specific data source, you can add a custom ingestion function it in the `./steps/ingest.py` step.
# MAGIC 
# MAGIC To have a quick training & ingestion during the dev phase, we'll be using a subset of the data with the `TABLESAMPLE` function, and 100% will be used during the  final training.
# MAGIC 
# MAGIC Let's define the data input in the  `./pipeline.yaml` configuration file:
# MAGIC 
# MAGIC <div style="clear: both; width: 1010px;height: 22px;font-weight: bold;background: #c7c7c7;border: 1px solid #ddd;color: #553535;border-bottom: 0px;border-left: 3px solid #ff0c0c;padding-left: 10px;font-family: Courier New">
# MAGIC pipeline.yaml
# MAGIC </div>
# MAGIC <div style="width: 1020px;background: #fbfbfb;border: 1px solid #ddd;border-left: 3px solid #ff0c0c;color: #242424;">
# MAGIC   <pre>
# MAGIC   data:
# MAGIC     format: {{INGEST_DATA_FORMAT|default('parquet')}}
# MAGIC     sql: SELECT * FROM {{INGEST_TABLE}} TABLESAMPLE ({{INGEST_TABLE_SAMPLE_PERCENT}} PERCENT) REPEATABLE (42)
# MAGIC   </pre>
# MAGIC </div>
# MAGIC 
# MAGIC We'll define the variables in our `./profiles/dev.yaml` environment file:
# MAGIC 
# MAGIC <div style="width: 1010px;height: 22px;font-weight: bold;background: #c7c7c7;border: 1px solid #ddd;color: #553535;border-bottom: 0px;border-left: 3px solid #ff0c0c; padding-left: 10px;font-family: Courier New">
# MAGIC profiles/dev.yaml
# MAGIC </div>
# MAGIC <div style="width: 1020px;background: #fbfbfb;border: 1px solid #ddd;border-left: 3px solid #ff0c0c; color: #242424;">
# MAGIC   <pre>
# MAGIC   INGEST_TABLE: insurance_charge
# MAGIC   INGEST_TABLE_SAMPLE_PERCENT: 30
# MAGIC   INGEST_DATA_FORMAT: spark_sql
# MAGIC   </pre>
# MAGIC </div>
# MAGIC 
# MAGIC That's it, our ingestion step is read. MLFlow pipeline will run this part once and cache the relevant data to avoid unecessary redundent operations.

# COMMAND ----------

# DBTITLE 1,Defines the Pipeline object
from mlflow.pipelines import Pipeline

p = Pipeline(profile="dev")

# COMMAND ----------

# DBTITLE 1,Start the first ingestion step
p.clean() #Removes all pipeline steps from cache
#Run the first ingestion step: consume the data and cache it
p.run("ingest")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 2: Split in test/train/validation
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-2.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC In all ML deployments, we need to split our dataset between training, test and validation.
# MAGIC 
# MAGIC MLFlow pipeline does that for you out of the box. You can specify your split ratios if you need to customize them.
# MAGIC 
# MAGIC Note that after this step, the ingestion part won't be recomputed multiple time. 
# MAGIC 
# MAGIC The pipeline engine has cached the data and won't ingest it multiple time unless you call a `p.clean()` to force all the steps to be recomputed.
# MAGIC 
# MAGIC Let's see how this can be done:
# MAGIC 
# MAGIC <br/><br/>
# MAGIC The split ratios are defined in our main `pipeline.yaml` file:
# MAGIC 
# MAGIC <div style="clear: both; width: 1010px;height: 22px;font-weight: bold;background: #c7c7c7;border: 1px solid #ddd;color: #553535;border-bottom: 0px;border-left: 3px solid #ff0c0c; padding-left: 10px;font-family: Courier New">
# MAGIC pipeline.yaml
# MAGIC </div>
# MAGIC <div style="width: 1020px;background: #fbfbfb;border: 1px solid #ddd;border-left: 3px solid #ff0c0c; color: #242424;">
# MAGIC   <pre>
# MAGIC   steps:
# MAGIC     split:
# MAGIC       # Train/validation/test split ratios
# MAGIC       split_ratios: {{SPLIT_RATIOS|default([0.75, 0.125, 0.125])}}
# MAGIC       # Specifies the method to use to perform additional processing and cleaning on split datasets
# MAGIC       post_split_method: steps.split.process_splits  </pre>
# MAGIC </div>
# MAGIC 
# MAGIC The split step can be used to apply post-transformation on the dataset (the function called is defined in the `post_split_method` config). This is typically used to do some data cleaning (ex: removing abnormal data). 
# MAGIC 
# MAGIC Note that the logic defined here won't be shipped in the model (it won't be applied on inference)
# MAGIC 
# MAGIC `post_split_method` is define in the `./steps/split.py` step file:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <div style="width: 1010px;height: 22px;font-weight: bold;background: #c7c7c7;border: 1px solid #ddd;color: #553535;border-bottom: 0px;border-left: 3px solid #ff0c0c; padding-left: 10px;font-family: Courier New">
# MAGIC steps/split.py
# MAGIC </div>
# MAGIC <div style="width: 1020px;background: #fbfbfb;border: 1px solid #ddd;border-left: 3px solid #ff0c0c; color: #242424;">
# MAGIC   <pre>
# MAGIC   def process_splits(train_df: DataFrame, validation_df: DataFrame, test_df: DataFrame) 
# MAGIC                       -> (DataFrame, DataFrame, DataFrame):
# MAGIC     def clean(df):
# MAGIC       #optional: do some data cleaning
# MAGIC       return df
# MAGIC     return clean(train_df), clean(validation_df), clean(test_df)
# MAGIC   </pre>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Call the split step
p.run("split")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 3: Transform
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-3.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC We now have our data splitted. The next step is to apply our transform method to prepare the features for the model (ex: OneHotEncoder or similar transformations). 
# MAGIC 
# MAGIC As usual this is implemented in the `steps/transform.py` file and should returns an *unfitted* transformer that defines `fit()` and `transform()` methods, typically a SKlearn Pipeline object.
# MAGIC 
# MAGIC Note that this object is part of the model, so the transformers will be fit during the training and the same transformation will be applied during inference.

# COMMAND ----------

p.run("transform")

# COMMAND ----------

# DBTITLE 1,Let's review the data after the transformation
train_df = p.get_artifact("transformed_training_data")
display(train_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 4: Train
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-4.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC This is the final part of the model definition.
# MAGIC 
# MAGIC The `steps/train.py` is used to defined our model training.
# MAGIC 
# MAGIC It should return an object having a `fit()` and `predict()` method.
# MAGIC 
# MAGIC Any scikit-learn model can be used, in our case we'll add a simple XGBRegressor:
# MAGIC 
# MAGIC <div style="width: 410px;height: 22px;font-weight: bold;background: #c7c7c7;border: 1px solid #ddd;color: #553535;border-bottom: 0px;border-left: 3px solid #ff0c0c; padding-left: 10px;font-family: Courier New">
# MAGIC steps/train.py
# MAGIC </div>
# MAGIC <div style="width: 420px;background: #fbfbfb;border: 1px solid #ddd;border-left: 3px solid #ff0c0c; color: #242424;">
# MAGIC   <pre>
# MAGIC   def estimator_fn():
# MAGIC     from xgboost import XGBRegressor
# MAGIC     return XGBRegressor()
# MAGIC </div>
# MAGIC 
# MAGIC Note that MLFlow Pipeline automatically gives you a run summary including an overview of the rows having the largest prediction error. This helps troubleshooting and improving your model.

# COMMAND ----------

p.run("train")
#We can retrive the model as artifact if required
p.get_artifact("model")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 5: Evaluation
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-5.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC Once our model is trained, the evaluation step will compute the model metrics.
# MAGIC 
# MAGIC The validation criteria defined in `pipeline.yaml` will validate or not the training.
# MAGIC 
# MAGIC <div style="width: 610px;height: 22px;font-weight: bold;background: #c7c7c7;border: 1px solid #ddd;color: #553535;border-bottom: 0px;border-left: 3px solid #ff0c0c; padding-left: 10px;font-family: Courier New">
# MAGIC pipeline.yaml
# MAGIC </div>
# MAGIC <div style="width: 620px;background: #fbfbfb;border: 1px solid #ddd;border-left: 3px solid #ff0c0c; color: #242424;">
# MAGIC   <pre>
# MAGIC   evaluate:
# MAGIC     # Sets performance thresholds that a trained model must meet in order
# MAGIC     # to be eligible for registration to the MLflow Model Registry
# MAGIC     validation_criteria:
# MAGIC       - metric: root_mean_squared_error
# MAGIC         threshold: 1000
# MAGIC </div>
# MAGIC   
# MAGIC Shap explanation will automatically be added too.  
# MAGIC 
# MAGIC Note that MLFlow Pipeline automatically gives you a run summary including an overview of the rows having the largest prediction error. This helps troubleshooting and improving your model.
# MAGIC 
# MAGIC It'll also check your defined threshold and register the model only if these threshold are passed (you can check the threshold in the evaluation card).

# COMMAND ----------

#We can retrive the model as artifact if required
p.run("evaluate")
print(p.get_artifact("run").data.metrics["max_error_on_data_test"])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Step 6: Registring the model
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-6.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC All we now have to do is save the model in MLFlow registry.
# MAGIC 
# MAGIC As our pipeline fully integreates with MLFlow, all we have to do is call the next step and the engine will package everything for us.
# MAGIC 
# MAGIC You can open MLFlow UI to explore how the entire pipeline has been saved, including all the artifacts and the cards for each steps, allowing advanced analysis.

# COMMAND ----------

#We can retrive the model as artifact if required
p.run("register")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Deploying model in production
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlflow-pipelines/mlflow-pipelines-7.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC We now have our model saved in the registry.
# MAGIC 
# MAGIC The nexts steps are the usual steps within your MLOps pipeline. When your MLOps cycle is validated (ex: tests on model, check on signature & metrics) you can move your model in the `Production` stage.
# MAGIC 
# MAGIC We'll do that using the API, but this can be done using the UI.

# COMMAND ----------

# DBTITLE 1,Move the model in the Production stage
import mlflow
client = mlflow.tracking.MlflowClient()
model = p.get_artifact("registered_model_version")
print("registering model version "+model.version+" as production model")
client.transition_model_version_stage(name = model.name, version = model.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pandas inference
# MAGIC Our model can be loaded from the registry to run inferences as usual

# COMMAND ----------

pymodel = mlflow.pyfunc.load_model("models:/field_demos_insurance_charge/Production")
model_input_names = pymodel.metadata.signature.inputs.input_names()
df = spark.table("insurance_charge").select(model_input_names).toPandas()
pymodel.predict(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inferences at scale using spark and distributed inference
# MAGIC Scaling to distributed / SQL inference can be done within 1 line of code:
# MAGIC 
# MAGIC *Note: if you see a jinja error, try lo lower the jinja2 version with `%pip install Jinja2==3.0.3`, but you'll lose the previous cards output*

# COMMAND ----------

from pyspark.sql import functions as F

pymodel = mlflow.pyfunc.load_model("models:/field_demos_insurance_charge/Production")
model_input_names = pymodel.metadata.signature.inputs.input_names()

batch_inference_udf = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_insurance_charge/Production")
spark.table("insurance_charge").withColumn("predicted_charge", batch_inference_udf(*([F.col(f) for f in model_input_names]))).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reviewing the full MLFlow pipeline model
# MAGIC 
# MAGIC That's it, our model has been easily deployed leveraging MLFlow Pipeline.
# MAGIC 
# MAGIC More importantly, the project structured is now standardized and can easily be reused across multiple projects.
# MAGIC 
# MAGIC We can review the full pipeline definition as defined in the 

# COMMAND ----------

p.inspect()
