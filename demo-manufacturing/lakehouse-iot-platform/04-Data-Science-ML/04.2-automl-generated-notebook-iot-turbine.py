# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Use the best Auto-ML generated notebook to bootstrap our ML Project
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-ml-experiment.png" style="float: right" width="600px">
# MAGIC
# MAGIC Databricks Auto-ML tries many models and generate notebooks containing the code used to build the model.
# MAGIC
# MAGIC Here, we selected the notebook from best run from the Auto ML experiment.
# MAGIC
# MAGIC All the code below has been automatically generated. As Data Scientist, we can tune it based on our business knowledge, or use the model generated as it is.
# MAGIC
# MAGIC This saves Data scientists hours of development and allows teams to quickly bootstrap and validate new project.
# MAGIC
# MAGIC *Make sure you run the previous notebook to be able to access the data.*
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.2-automl-generated-notebook-iot-turbine&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %pip install mlflow==2.22.0 cloudpickle==2.2.1 databricks-sdk==0.40.0
# MAGIC # hardcode the ml 16.4 LTS libraries versions here for demo stability
# MAGIC %pip install category-encoders==2.6.3 cffi==1.16.0 databricks-automl-runtime==0.2.21 defusedxml==0.7.1 holidays==0.54 lightgbm==4.5.0 lz4==4.3.2 matplotlib==3.8.4 numpy==1.26.4 pandas==1.5.3 psutil==5.9.0 pyarrow==15.0.2 scikit-learn==1.4.2 scipy==1.13.1 shap==0.46.0 https://github.com/databricks-demos/dbdemos-resources/raw/refs/heads/main/hyperopt-0.2.8-py3-none-any.whl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC # LightGBM training
# MAGIC This is an auto-generated notebook. To reproduce these results, attach this notebook to the **Shared Autoscaling Americas cluster** cluster and rerun it.
# MAGIC - Compare trials in the [MLflow experiment](#mlflow/experiments/4380395087402942)
# MAGIC - Navigate to the parent notebook [here](#notebook/4380395087402943) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.
# MAGIC
# MAGIC Runtime Version: _11.3.x-cpu-ml-scala2.12_

# COMMAND ----------

import mlflow
import databricks.automl_runtime
#Added for the demo purpose
xp = DBDemos.get_last_experiment("lakehouse-iot-platform")
# Use MLflow to track experiments
mlflow.set_experiment(xp["path"])

#Run containing the data analysis notebook to get the data from artifact
data_run = mlflow.search_runs(filter_string="tags.mlflow.source.name='Notebook: DataExploration'", order_by=['attributes.start_time DESC']).iloc[0].to_dict()

#get best run id (this notebook)
df = mlflow.search_runs(filter_string="metrics.val_f1_score < 1")
run = df.sort_values(by="metrics.val_f1_score", ascending=False).iloc[0].to_dict()

target_col = "abnormal_sensor"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

from mlflow.tracking import MlflowClient
import os
import uuid
import shutil
import pandas as pd

# Create temp directory to download input data from MLflow
input_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(input_temp_dir)

# Download the artifact and read it into a pandas DataFrame
input_client = MlflowClient()
input_data_path = input_client.download_artifacts(data_run["run_id"], "data", input_temp_dir)

df_loaded = pd.read_parquet(os.path.join(input_data_path, "training_data"))
# Delete the temp data
shutil.rmtree(input_temp_dir)

try:
    df_loaded = df_loaded.drop(['_automl_sample_weight_0000'], axis=1) #for demo only, to make it more stable across versions.
except:
    print('column weight not available - this might change depending on the automl version - can ignore')
# Preview data
df_loaded.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select supported columns
# MAGIC Select only the columns that are supported. This allows us to train a model that can predict on a dataset that has extra columns that are not used in training.
# MAGIC `["model"]` are dropped in the pipelines. See the Alerts tab of the AutoML Experiment page for details on why these columns are dropped.

# COMMAND ----------

from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
supported_cols = ["hourly_timestamp", "avg_energy", "std_sensor_A", "std_sensor_B", "std_sensor_C", "std_sensor_D", "std_sensor_E", "std_sensor_F", "location", "model", "state"]
col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Datetime Preprocessor
# MAGIC For each datetime column, extract relevant information from the date:
# MAGIC - Unix timestamp
# MAGIC - whether the date is a weekend
# MAGIC - whether the date is a holiday
# MAGIC
# MAGIC Additionally, extract extra information from columns with timestamps:
# MAGIC - hour of the day (one-hot encoded)
# MAGIC
# MAGIC For cyclic features, plot the values along a unit circle to encode temporal proximity:
# MAGIC - hour of the day
# MAGIC - hours since the beginning of the week
# MAGIC - hours since the beginning of the month
# MAGIC - hours since the beginning of the year

# COMMAND ----------

from pandas import Timestamp
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from databricks.automl_runtime.sklearn import DatetimeImputer
from databricks.automl_runtime.sklearn import TimestampTransformer
from sklearn.preprocessing import StandardScaler

imputers = {
  "hourly_timestamp": DatetimeImputer(),
}

datetime_transformers = []

for col in ["hourly_timestamp"]:
    ohe_transformer = ColumnTransformer(
        [("ohe", OneHotEncoder(sparse_output=False, handle_unknown="ignore"), [TimestampTransformer.HOUR_COLUMN_INDEX])],
        remainder="passthrough")
    timestamp_preprocessor = Pipeline([
        (f"impute_{col}", imputers[col]),
        (f"transform_{col}", TimestampTransformer()),
        (f"onehot_encode_{col}", ohe_transformer),
        (f"standardize_{col}", StandardScaler()),
    ])
    datetime_transformers.append((f"timestamp_{col}", timestamp_preprocessor, [col]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical columns
# MAGIC
# MAGIC Missing values for numerical columns are imputed with mean by default.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["avg_energy", "std_sensor_A", "std_sensor_B", "std_sensor_C", "std_sensor_D", "std_sensor_E", "std_sensor_F"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])
numerical_transformers = [("numerical", numerical_pipeline, ["std_sensor_B", "std_sensor_A", "avg_energy", "std_sensor_E", "std_sensor_C", "std_sensor_F", "std_sensor_D"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low-cardinality categoricals
# MAGIC Convert each low-cardinality categorical column into multiple binary columns through one-hot encoding.
# MAGIC For each input categorical column (string or numeric), the number of output columns is equal to the number of unique values in the input column.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

one_hot_imputers = []

one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="ignore")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["location", "state", "model"])]

# COMMAND ----------

from sklearn.compose import ColumnTransformer

transformers = datetime_transformers + numerical_transformers + categorical_one_hot_transformers 

preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC The input data is split by AutoML into 3 sets:
# MAGIC - Train (60% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (20% of the dataset used to report the true performance of the model on an unseen dataset)
# MAGIC
# MAGIC `_automl_split_col_3991` contains the information of which set a given row belongs to.
# MAGIC We use this column to split the dataset into the above 3 sets. 
# MAGIC The column should not be used for training so it is dropped after split is done.

# COMMAND ----------

# AutoML completed train - validation - test split internally and used _automl_split_col_xxxx to specify the set
split_col = [c for c in df_loaded.columns if c.startswith('_automl_split_col')][0]

split_train_df = df_loaded.loc[df_loaded[split_col] == "train"]
split_val_df = df_loaded.loc[df_loaded[split_col] == "val"]
split_test_df = df_loaded.loc[df_loaded[split_col] == "test"]

# Separate target column from features and drop split_col
X_train = split_train_df.drop([target_col, split_col], axis=1)
y_train = split_train_df[target_col]

X_val = split_val_df.drop([target_col, split_col], axis=1)
y_val = split_val_df[target_col]

X_test = split_test_df.drop([target_col, split_col], axis=1)
y_test = split_test_df[target_col]

if len(X_val) == 0: #hack for the demo to support all version - don't do that in production
    X_val = X_test
    y_val = y_test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train classification model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/4380395087402942)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

import lightgbm
from lightgbm import LGBMClassifier

help(LGBMClassifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the objective function
# MAGIC The objective function used to find optimal hyperparameters. By default, this notebook only runs
# MAGIC this function once (`max_evals=1` in the `hyperopt.fmin` invocation) with fixed hyperparameters, but
# MAGIC hyperparameters can be tuned by modifying `space`, defined below. `hyperopt.fmin` will then use this
# MAGIC function's return value to search the space to minimize the loss.

# COMMAND ----------

import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline
from unittest import mock

from hyperopt import hp, tpe, fmin, STATUS_OK, Trials

# Create a separate pipeline to transform the validation dataset. This is used for early stopping.
mlflow.sklearn.autolog(disable=True)
pipeline_val = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
])
pipeline_val.fit(X_train, y_train)
X_val_processed = pipeline_val.transform(X_val)
dataset = mlflow.data.from_pandas(X_train)

def objective(params):
  #Temporary pin python to 3.11.10
  with mlflow.start_run(experiment_id=run['experiment_id'], run_name="lightgbm") as mlflow_run, mock.patch("mlflow.utils.environment.PYTHON_VERSION", DBDemos.get_python_version_mlflow()):
    lgbmc_classifier = LGBMClassifier(**params)
    mlflow.log_input(dataset, context="training")
    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", lgbmc_classifier),
    ])

    # Enable automatic logging of input samples, metrics, parameters, and models
    mlflow.sklearn.autolog(
        log_input_examples=True,
        silent=True)

    model.fit(X_train, y_train, classifier__callbacks=[lightgbm.early_stopping(5), lightgbm.log_evaluation(0)], classifier__eval_set=[(X_val_processed,y_val)])

    
    # Log metrics for the training set
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    X_train[target_col] = y_train
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train,
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_"  }
    )
    lgbmc_training_metrics = training_eval_result.metrics
    # Log metrics for the validation set
    X_val[target_col] = y_val
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val,
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_"  }
    )
    lgbmc_val_metrics = val_eval_result.metrics
    # Log metrics for the test set
    X_test[target_col] = y_test
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test,
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_"  }
    )
    lgbmc_test_metrics = test_eval_result.metrics

    loss = lgbmc_val_metrics["val_f1_score"]

    # Truncate metric key names so they can be displayed together
    lgbmc_val_metrics = {k.replace("val_", ""): v for k, v in lgbmc_val_metrics.items()}
    lgbmc_test_metrics = {k.replace("test_", ""): v for k, v in lgbmc_test_metrics.items()}

    return {
      "loss": loss,
      "status": STATUS_OK,
      "val_metrics": lgbmc_val_metrics,
      "test_metrics": lgbmc_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure the hyperparameter search space
# MAGIC Configure the search space of parameters. Parameters below are all constant expressions but can be
# MAGIC modified to widen the search space. For example, when training a decision tree classifier, to allow
# MAGIC the maximum tree depth to be either 2 or 3, set the key of 'max_depth' to
# MAGIC `hp.choice('max_depth', [2, 3])`. Be sure to also increase `max_evals` in the `fmin` call below.
# MAGIC
# MAGIC See https://docs.databricks.com/applications/machine-learning/automl-hyperparam-tuning/index.html
# MAGIC for more information on hyperparameter tuning as well as
# MAGIC http://hyperopt.github.io/hyperopt/getting-started/search_spaces/ for documentation on supported
# MAGIC search expressions.
# MAGIC
# MAGIC For documentation on parameters used by the model in use, please see:
# MAGIC https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.LGBMClassifier.html
# MAGIC
# MAGIC NOTE: The above URL points to a stable version of the documentation corresponding to the last
# MAGIC released version of the package. The documentation may differ slightly for the package version
# MAGIC used by this notebook.

# COMMAND ----------

space = {
  "colsample_bytree": 0.5517855677922646,
  "lambda_l1": 40.596326947371445,
  "lambda_l2": 27.761059765184243,
  "learning_rate": 0.05279826333944268,
  "max_bin": 298,
  "max_depth": 10,
  "min_child_samples": 346,
  "n_estimators": 1817,
  "num_leaves": 271,
  "path_smooth": 38.751951722757724,
  "subsample": 0.7574791923479774,
  "random_state": 741748958,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run trials
# MAGIC When widening the search space and training multiple models, switch to `SparkTrials` to parallelize
# MAGIC training on Spark:
# MAGIC ```
# MAGIC from hyperopt import SparkTrials
# MAGIC trials = SparkTrials()
# MAGIC ```
# MAGIC
# MAGIC NOTE: While `Trials` starts an MLFlow run for each set of hyperparameters, `SparkTrials` only starts
# MAGIC one top-level run; it will start a subrun for each set of hyperparameters.
# MAGIC
# MAGIC See http://hyperopt.github.io/hyperopt/scaleout/spark/ for more info.

# COMMAND ----------

trials = Trials()
fmin(objective,
     space=space,
     algo=tpe.suggest,
     max_evals=1,  # Increase this when widening the hyperparameter search space.
     trials=trials)

best_result = trials.best_trial["result"]
model = best_result["model"]
mlflow_run = best_result["run"]

display(
  pd.DataFrame(
    [best_result["val_metrics"], best_result["test_metrics"]],
    index=["validation", "test"]))

set_config(display="diagram")
model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance
# MAGIC
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation, so to ensure that AutoML can run trials without
# MAGIC   running out of memory, we disable SHAP by default.<br />
# MAGIC   You can set the flag defined below to `shap_enabled = True` and re-run this notebook to see the SHAP plots.
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the validation set to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC - SHAP cannot explain models using data with nulls; if your dataset has any, both the background data and
# MAGIC   examples to explain will be imputed using the mode (most frequent values). This affects the computed
# MAGIC   SHAP values, as the imputed samples may not match the actual data distribution.
# MAGIC
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).
# MAGIC
# MAGIC > **NOTE:** SHAP run may take a long time with the datetime columns in the dataset.

# COMMAND ----------

#Note: shap doesn't handle well array column. To make it work extract the array as top-level column in your model instead.
shap_enabled = False
if shap_enabled:
    mlflow.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)
    from shap import KernelExplainer, summary_plot
    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train.sample(n=min(100, X_train.shape[0]), random_state=668269204)

    # Sample some rows from the validation set to explain. Increase the sample size for more thorough results.
    example = X_val.sample(n=min(100, X_val.shape[0]), random_state=668269204)

    # Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
    predict = lambda x: model.predict_proba(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="logit")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=500)
    summary_plot(shap_values, example, class_names=model.classes_)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confusion matrix for validation data
# MAGIC
# MAGIC We show the confusion matrix of the model on the validation data.
# MAGIC
# MAGIC For the plots evaluated on the training and the test data, check the artifacts on the MLflow run page.

# COMMAND ----------

# Paste the entire output (%md ...) to an empty cell, and click the link to see the MLflow run page
displayHTML(f"""<a href="#mlflow/experiments/{ run['experiment_id'] }/runs/{ mlflow_run.info.run_id }/artifactPath/model">Link to model run page</a>""")

# COMMAND ----------

import uuid
from IPython.display import Image

# Create temp directory to download MLflow model artifact
eval_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(eval_temp_dir, exist_ok=True)

# Download the artifact
eval_path = mlflow.artifacts.download_artifacts(run_id=mlflow_run.info.run_id, dst_path=eval_temp_dir)

# COMMAND ----------

eval_confusion_matrix_path = os.path.join(eval_path, "test_confusion_matrix.png")
display(Image(filename=eval_confusion_matrix_path))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### MLFlow tracked all our model information and the model is ready to be deployed in our registry!
# MAGIC We can do that manually or using MLFlow APIs directly:

# COMMAND ----------

# DBTITLE 1,Let's register a first model version as example
model_name = "dbdemos_turbine_maintenance"

#Use Databricks Unity Catalog to save our model
mlflow.set_registry_uri('databricks-uc')
latest_model = mlflow.register_model(f'runs:/{mlflow_run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
# Flag it as Production ready using UC Aliases
MlflowClient().set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=latest_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our model as production ready! [Open the dbdemos_turbine_maintenance model](#mlflow/models/dbdemos_turbine_maintenance) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### The model generated by AutoML is ready to be used in our DLT pipeline to detect Wind Turbine requiring potential maintenance.
# MAGIC
# MAGIC Our Data Engineer can now easily retrive the model `dbdemos_turbine_maintenance` from our Auto ML run and detect anomalies within our Delta Live Table Pipeline.<br>
# MAGIC Re-open the DLT pipeline to see how this is done.
# MAGIC
# MAGIC #### Adjust spare stock based on predictive maintenance result
# MAGIC
# MAGIC These predictions can be re-used in our dashboard to not only measure equipment failure probability, but also to take action to schedule maintenance and ajust spare part stock accordingly. 
# MAGIC
# MAGIC The pipeline created with the Data Intelligence Platform will offer a strong ROI: in the few hours that it took to set this pipeline up we are effectively saving our organization MILLIONS of dollars by month!
# MAGIC
# MAGIC <img width="800px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
# MAGIC
# MAGIC <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Open the Predictive Maintenance DBSQL dashboard</a> | [Go back to the introduction]($../00-IOT-wind-turbine-introduction-DI-platform)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Next: Deploying our model in production: batch or serverless inference 
# MAGIC
# MAGIC Let's now see how we can deploy this model and run inferences in production to provide Churn insight for our business
# MAGIC
# MAGIC Next: [04.3-running-inference-iot-turbine]($./04.3-running-inference-iot-turbine)
