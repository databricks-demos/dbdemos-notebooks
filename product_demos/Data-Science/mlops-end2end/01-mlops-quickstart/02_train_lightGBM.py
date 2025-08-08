# Databricks notebook source
# MAGIC %md
# MAGIC # Train a simple `LightGBM` model
# MAGIC
# MAGIC For this quickstart demo, we're going to train a base `LightGBM` model.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or disable the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=02_automl_best_run&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Install MLflow version for UC [for MLR < 15.2]
# MAGIC %pip install --quiet lightgbm mlflow
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC # LightGBM Classifier training

# COMMAND ----------

import mlflow
xp_path = f"/Users/{current_user}/dbdemos_mlops"


#Added for the demo purpose
xp = mlflow.search_experiments(filter_string=f"name LIKE '/Shared/dbdemos/experiments/mlops%'", order_by=["last_update_time DESC"])[0]

# Use MLflow to track experiments
mlflow.set_experiment(experiment_id=xp.experiment_id)

#Run containing the data analysis notebook to get the data from artifact
data_run = mlflow.search_runs(filter_string="tags.mlflow.source.name='Notebook: DataExploration'").iloc[0].to_dict()

#get best run id (this notebook)
df = mlflow.search_runs(filter_string="metrics.val_f1_score < 1")
run = df.sort_values(by="metrics.val_f1_score", ascending=False).iloc[0].to_dict()

target_col = "churn"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data lineage
# MAGIC Capturing upstream data lineage to a model allows data science teams to perform root cause analysis when issues are observed in the models' predictions. The lineage graph can be examined through Unity Catalog and queried from System Tables.
# MAGIC
# MAGIC MLflow provides APIs to capture this lineage. Lineage capture entails the following steps:
# MAGIC
# MAGIC - Load the object representing the training dataset from Unity Catalog
# MAGIC
# MAGIC   `src_dataset = mlflow.data.load_delta(table_name=f'{catalog}.{db}.mlops_churn_training', version=latest)`
# MAGIC
# MAGIC - Log the dataset object as part of the training run
# MAGIC
# MAGIC   <br>
# MAGIC
# MAGIC   ```
# MAGIC    mlflow.start_run():
# MAGIC      ...
# MAGIC      mlflow.log_input(src_dataset, context="training-input")
# MAGIC   ```

# COMMAND ----------

# Load the dataset object from Unity Catalog
latest_table_version = max(
    spark.sql(f"describe history {catalog}.{db}.mlops_churn_training").toPandas()["version"]
)

src_dataset = mlflow.data.load_delta(table_name=f"{catalog}.{db}.mlops_churn_training", version=str(latest_table_version))

# COMMAND ----------

import mlflow
import os
import uuid
import shutil
import pandas as pd


input_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(input_temp_dir)

# Download the artifact and read it into a Pandas DataFrame
input_data_path = mlflow.artifacts.download_artifacts(run_id=data_run['run_id'], artifact_path="data", dst_path=input_temp_dir)
df_loaded = pd.read_parquet(os.path.join(input_data_path, "training_data"))

# Delete the temp data
shutil.rmtree(input_temp_dir)

# Preview data
display(df_loaded.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean columns
# MAGIC For each column, impute missing values and then convert them into ones and zeros.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder


bool_imputers = []

bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer(bool_imputers, remainder="passthrough")),
    ("onehot", SklearnOneHotEncoder(handle_unknown="ignore", drop="first")),
])

bool_transformers = [("boolean", bool_pipeline, ["gender", "phone_service", "dependents", "senior_citizen", "paperless_billing", "partner"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical columns
# MAGIC
# MAGIC Missing values for numerical columns are imputed with mean by default.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler


num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["monthly_charges", "num_optional_services", "tenure", "total_charges"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors='coerce'))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["monthly_charges", "total_charges", "tenure", "num_optional_services"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low-cardinality categoricals
# MAGIC Convert each low-cardinality categorical column into multiple binary columns through one-hot encoding.
# MAGIC For each input categorical column (string or numeric), the number of output columns is equal to the number of unique values in the input column.

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder


one_hot_imputers = []
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="ignore")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bundle into single pipeline

# COMMAND ----------

transformers = bool_transformers + numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="drop", sparse_threshold=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC The input data is split by AutoML into 3 sets:
# MAGIC - Train (60% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (20% of the dataset used to report the true performance of the model on an unseen dataset)
# MAGIC
# MAGIC
# MAGIC We use this column to split the dataset into the above three sets.
# MAGIC The column should not be used for training, so it is dropped after the split is done.

# COMMAND ----------

# AutoML completed train - validation - test split internally and used _automl_split_col_xxxx to specify the set
split_col = [c for c in df_loaded.columns if c.startswith('_automl_split_col') or c == 'split'][0]

# AutoML completed train - validation - test split internally and used split to specify the set
split_train_df = df_loaded.loc[df_loaded[split_col] == "train"]
split_val_df = df_loaded.loc[df_loaded[split_col] == "validate"]
split_test_df = df_loaded.loc[df_loaded[split_col] == "test"]

# Separate target co# Separate target column from features and drop split
X_train = split_train_df.drop([target_col, "split", split_col], errors='ignore', axis=1)
y_train = split_train_df[target_col]

X_val = split_val_df.drop([target_col, "split", split_col], errors='ignore', axis=1)
y_val = split_val_df[target_col]

X_test = split_test_df.drop([target_col, "split", split_col], errors='ignore', axis=1)
y_test = split_test_df[target_col]

if len(X_val) == 0: #hack for the demo to support all version - don't do that in production
    X_val = X_test
    y_val = y_test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train classification model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under an experiment accessible from the "Experiment" view in your workspace's right-hand pane
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

import lightgbm
from lightgbm import LGBMClassifier


help(LGBMClassifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the training function

# COMMAND ----------

from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline


def train_fn(params):
  with mlflow.start_run(experiment_id=run['experiment_id'], run_name=params["run_name"]) as mlflow_run:
    lgbmc_classifier = LGBMClassifier(**params)

    model = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", lgbmc_classifier),
    ])

    # Enable automatic logging of input samples, metrics, parameters, and models
    mlflow.sklearn.autolog(log_models=False, silent=True)

    model.fit(X_train, y_train)
    signature = infer_signature(X_train, y_train)
    mlflow.sklearn.log_model(model, "sklearn_model", input_example=X_train.iloc[0].to_dict(), signature=signature)

    # Log training dataset object to capture upstream data lineage
    mlflow.log_input(src_dataset, context="training-input")

    # Log metrics for the training set
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(target_col):y_train}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "Yes" }
    )
    sklr_training_metrics = training_eval_result.metrics

    # Log metrics for the validation set
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(target_col):y_val}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "Yes" }
    )
    sklr_val_metrics = val_eval_result.metrics

    # Log metrics for the test set
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(target_col):y_test}),
        targets=target_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": "Yes" }
    )
    sklr_test_metrics = test_eval_result.metrics

    loss = -sklr_val_metrics["val_f1_score"]

    # Truncate metric key names so they can be displayed together
    sklr_val_metrics = {k.replace("val_", ""): v for k, v in sklr_val_metrics.items()}
    sklr_test_metrics = {k.replace("test_", ""): v for k, v in sklr_test_metrics.items()}

    return {
      "loss": loss,
      "val_metrics": sklr_val_metrics,
      "test_metrics": sklr_test_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure model's hyperparameter
# MAGIC
# MAGIC For documentation on parameters used by the model in use, please see:
# MAGIC https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.LGBMClassifier.html
# MAGIC
# MAGIC NOTE: The above URL points to a stable version of the documentation corresponding to the last
# MAGIC released version of the package. The documentation may differ slightly for the package version
# MAGIC used by this notebook.

# COMMAND ----------

params = {
  "run_name": "light_gbm_baseline",
  "colsample_bytree": 0.4120544919020157, 
  "lambda_l1": 2.6616074270114995,
  "lambda_l2": 514.9224373768443,
  "learning_rate": 0.0678497372371143,
  "max_bin": 229,
  "max_depth": 8,
  "min_child_samples": 66,
  "n_estimators": 250,
  "num_leaves": 100,
  "path_smooth": 61.06596877554017,
  "subsample": 0.6965257092078714,
  "random_state": 42,
}

# COMMAND ----------

training_results = train_fn(params)

# COMMAND ----------

loss = training_results["loss"]
model = training_results["model"]
print(f"Model loss: {loss}")

# COMMAND ----------

model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confusion matrix, ROC, and Precision-Recall curves for validation data
# MAGIC
# MAGIC We show the model's confusion matrix, RO,C and Precision-Recall curves on the validation data.
# MAGIC
# MAGIC For the plots evaluated on the training and the test data, check the artifacts on the MLflow run page.

# COMMAND ----------

mlflow_run = training_results["run"]

# COMMAND ----------

# Click the link to see the MLflow run page
displayHTML(f"<a href=#mlflow/experiments/{mlflow_run.info.experiment_id}/runs/{ mlflow_run.info.run_id }/artifactPath/model> Link to model run page </a>")

# COMMAND ----------

import uuid
from IPython.display import Image


# Create temp directory to download MLflow model artifact
eval_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(eval_temp_dir, exist_ok=True)

# Download the artifact
eval_path = mlflow.artifacts.download_artifacts(run_id=mlflow_run.info.run_id, dst_path=eval_temp_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion matrix for validation dataset

# COMMAND ----------

eval_confusion_matrix_path = os.path.join(eval_path, "val_confusion_matrix.png")
display(Image(filename=eval_confusion_matrix_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROC curve for validation dataset

# COMMAND ----------

eval_roc_curve_path = os.path.join(eval_path, "val_roc_curve_plot.png")
display(Image(filename=eval_roc_curve_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Precision-Recall curve for validation dataset

# COMMAND ----------

eval_pr_curve_path = os.path.join(eval_path, "val_precision_recall_curve_plot.png")
display(Image(filename=eval_pr_curve_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Automate model promotion validation
# MAGIC
# MAGIC Next step: [Search runs and trigger model promotion validation]($./03_from_notebook_to_models_in_uc)
