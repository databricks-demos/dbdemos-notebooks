# Databricks notebook source
# MAGIC %md
# MAGIC # Use the best Auto-ML generated notebook to bootstrap our ML Project
# MAGIC
# MAGIC We have selected the notebook from best run from the Auto ML experiment and reusing it to build our model.
# MAGIC
# MAGIC All the code below has been automatically generated. As Data Scientist, I can tune it based on the business knowledge I have if needed.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-2.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_auto_ml&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Auto-ML notebook",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["auto-ml"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,Install new feature engineering client for UC [for MLR < 13.2]
# MAGIC %pip install databricks-feature-engineering
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="dbdemos"

# COMMAND ----------

# MAGIC %md
# MAGIC # LightGBM Classifier training
# MAGIC - This is an auto-generated notebook.
# MAGIC - To reproduce these results, attach this notebook to a cluster with runtime version **14.3.x-cpu-ml-scala2.12**, and rerun it.
# MAGIC - Compare trials in the [MLflow experiment](#mlflow/experiments/xxx).
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.

# COMMAND ----------

import mlflow
import databricks.automl_runtime


label_col = "churn"

# COMMAND ----------

# Added for the demo purpose
run=dict()
xp_path = f"/Users/{current_user}/databricks_automl/{churn_experiment_name}"
run["experiment_path"] = xp_path
run["experiment_id"]   = mlflow.search_experiments(filter_string=f"name LIKE '{xp_path}%'",
                                                   order_by=["last_update_time DESC"])[0].experiment_id


_ = mlflow.set_experiment(experiment_id=run["experiment_id"])
print(f"Set experiment to: {run['experiment_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data
# MAGIC Load data directly from feature store
# MAGIC

# COMMAND ----------

feature_table_full_name = f"{catalog}.{dbName}.{feature_table_name}"

display(spark.table(feature_table_full_name))

# COMMAND ----------

# MAGIC %md We'll also use specific feature functions for `on-demand features`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION avg_price_increase

# COMMAND ----------

# MAGIC %md
# MAGIC Create features specifications

# COMMAND ----------

# DBTITLE 1,Define feature lookups
from databricks.feature_store import FeatureFunction, FeatureLookup


features = [
    FeatureLookup(
      table_name=f"{catalog}.{dbName}.{feature_table_name}",
      lookup_key=[primary_key],
      timestamp_lookup_key=timestamp_col
    ),
    FeatureFunction(
      udf_name=f"{catalog}.{dbName}.avg_price_increase",
      input_bindings={
        "monthly_charges_in" : "monthly_charges",
        "tenure_in" : "tenure",
        "total_charges_in" : "total_charges"
      },
      output_name="avg_price_increase"
    )
]

# COMMAND ----------

# DBTITLE 1,Pull labels to use for training/validating/testing

labels_df = spark.read.table(f"{catalog}.{dbName}.{labels_table_name}")

# COMMAND ----------

# DBTITLE 1,Create Training specifications
from databricks.feature_engineering import FeatureEngineeringClient


fe = FeatureEngineeringClient()

# Create Feature specifications object
training_set_specs = fe.create_training_set(
  df=labels_df, # DataFrame with lookup keys and label/target (+ any other input)
  label=label_col,
  feature_lookups=features
  # exclude_columns=[primary_key, timestamp_col] # Keeping them to create baseline table
)

# COMMAND ----------

df_loaded = training_set_specs.load_df().toPandas().set_index(keys=[primary_key, timestamp_col])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select supported columns
# MAGIC Select only the columns that are supported. This allows us to train a model that can predict on a dataset that has extra columns that are not used in training.
# MAGIC `[]` are dropped in the pipelines. See the Alerts tab of the AutoML Experiment page for details on why these columns are dropped.

# COMMAND ----------

from databricks.automl_runtime.sklearn.column_selector import ColumnSelector


supported_cols = ["online_backup", "internet_service", "payment_method", "multiple_lines", "paperless_billing", "partner", "tech_support", "tenure", "contract", "avg_price_increase", "phone_service", "streaming_movies", "dependents", "senior_citizen", "num_optional_services", "device_protection", "monthly_charges", "total_charges", "streaming_tv", "gender", "online_security"]

col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean columns
# MAGIC For each column, impute missing values and then convert into ones and zeros.

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

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler


num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["avg_price_increase", "monthly_charges", "num_optional_services", "tenure", "total_charges"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors='coerce'))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["monthly_charges", "total_charges", "avg_price_increase", "tenure", "num_optional_services"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low-cardinality categoricals
# MAGIC Convert each low-cardinality categorical column into multiple binary columns through one-hot encoding.
# MAGIC For each input categorical column (string or numeric), the number of output columns is equal to the number of unique values in the input column.

# COMMAND ----------

from databricks.automl_runtime.sklearn import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline


one_hot_imputers = []
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="indicator")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"])]

# COMMAND ----------

from sklearn.compose import ColumnTransformer


transformers = bool_transformers + numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC The input data is split by AutoML into 3 sets:
# MAGIC - Train (60% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (20% of the dataset used to report the true performance of the model on an unseen dataset)
# MAGIC
# MAGIC `_automl_split_col_xxxx` (or `_automl_split_col_0000` for MLR>=13.3LTS) contains the information of which set a given row belongs to.
# MAGIC We use this column to split the dataset into the above 3 sets.
# MAGIC The column should not be used for training so it is dropped after split is done.

# COMMAND ----------

# automl_split_col = list(filter(lambda x: "_automl_split_col_" in x, df_loaded.columns))[0]


# # AutoML completed train - validation - test split internally and used _automl_split_col_xxxx to specify the set
# split_train_df = df_loaded.loc[df_loaded[automl_split_col] == "train"]
# split_val_df = df_loaded.loc[df_loaded[automl_split_col] == "val"]
# split_test_df = df_loaded.loc[df_loaded[automl_split_col] == "test"]

# # Separate target column from features and drop _automl_split_col_xxxx
# X_train = split_train_df.drop([target_col, automl_split_col], axis=1)
# y_train = split_train_df[target_col]

# X_val = split_val_df.drop([target_col, automl_split_col], axis=1)
# y_val = split_val_df[target_col]

# X_test = split_test_df.drop([target_col, automl_split_col], axis=1)
# y_test = split_test_df[target_col]

# COMMAND ----------

X_train, X_eval, y_train, y_eval = train_test_split(df_loaded.drop(label_col, axis=1), df_loaded[label_col], test_size=0.4, stratify=df_loaded[label_col], random_state=42)

X_val, X_test, y_val, y_test = train_test_split(X_eval, y_eval, test_size=0.4, stratify=y_eval, random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train classification model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/3340404146807314) _(may be broken link)_
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

import lightgbm
from lightgbm import LGBMClassifier


help(LGBMClassifier)

# COMMAND ----------

# Record specific additional dependencies required by model serving (only required for MLR<13.3)
def pin_pandas_version(pandas_ver: str = model_serving_pandas_ver):
  """
  Custom pandas dependency to pin for deploying in model serving endpoints:
  :: pandas_ver : default version running in model serving containers
  """
  if pd.__version__ <= pandas_ver:
    return [f"pandas=={pandas_ver}"]
  else:
    return [f"pandas=={pd.__version__}"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### [TODO] Create custom pyfunc wrapper function to expose `predict_proba` method _(nice-to-have)_
# MAGIC To get AUC/ROC metrics in monitoring metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the objective function
# MAGIC The objective function used to find optimal hyperparameters. By default, this notebook only runs
# MAGIC this function once (`max_evals=1` in the `hyperopt.fmin` invocation) with fixed hyperparameters, but
# MAGIC hyperparameters can be tuned by modifying `space`, defined below. `hyperopt.fmin` will then use this
# MAGIC function's return value to search the space to minimize the loss.

# COMMAND ----------

from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline
from hyperopt import hp, tpe, fmin, STATUS_OK, Trials


# Create a separate pipeline to transform the validation dataset. This is used for early stopping.
mlflow.sklearn.autolog(disable=True)
pipeline_val = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
])
pipeline_val.fit(X_train, y_train)
X_val_processed = pipeline_val.transform(X_val)

def objective(params):
  with mlflow.start_run() as mlflow_run: # experiment_id=run['experiment_id']
    lgbmc_classifier = LGBMClassifier(**params)

    model = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ("classifier", lgbmc_classifier),
    ])

    # Enable automatic logging of input samples, metrics, parameters, and models
    mlflow.sklearn.autolog(
        log_input_examples=True,
        log_models=False,
        silent=True)

    model.fit(X_train, y_train, classifier__callbacks=[lightgbm.early_stopping(5), lightgbm.log_evaluation(0)], classifier__eval_set=[(X_val_processed,y_val)])

    import warnings
    from mlflow.types.utils import _infer_schema
    from mlflow.exceptions import MlflowException

    # Log the model

    # Infer output schema
    try:
      output_schema = _infer_schema(y_train)
    except Exception as e:
      warnings.warn(f"Could not infer model output schema: {e}")
      output_schema = None
    
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        extra_pip_requirements=pin_pandas_version(),
        training_set=training_set_specs,
        output_schema=output_schema,
        registered_model_name=model_name # Manual add to create "Champion" version
    )

    # Log metrics for the training set
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(label_col):y_train}),
        targets=label_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "Yes" }
    )
    lgbmc_training_metrics = training_eval_result.metrics

    # Log metrics for the validation set
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(label_col):y_val}),
        targets=label_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "Yes" }
    )
    lgbmc_val_metrics = val_eval_result.metrics

    # Log metrics for the test set
    test_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_test.assign(**{str(label_col):y_test}),
        targets=label_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "test_" , "pos_label": "Yes" }
    )
    lgbmc_test_metrics = test_eval_result.metrics

    loss = -lgbmc_val_metrics["val_f1_score"]

    # Truncate metric key names so they can be displayed together
    lgbmc_val_metrics = {k.replace("val_", ""): v for k, v in lgbmc_val_metrics.items()}
    lgbmc_test_metrics = {k.replace("test_", ""): v for k, v in lgbmc_test_metrics.items()}

    # Set as Champion model [Manual Add]
    client.set_registered_model_alias(model_name, "Champion", get_latest_model_version(model_name))

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

# COMMAND ----------

# Set this flag to True and re-run the notebook to see the SHAP plots
shap_enabled = True

# COMMAND ----------

if shap_enabled:
    mlflow.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)
    from shap import KernelExplainer, summary_plot
    # SHAP cannot explain models using data with nulls.
    # To enable SHAP to succeed, both the background data and examples to explain are imputed with the mode (most frequent values).
    mode = X_train.mode().iloc[0]

    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train.sample(n=min(100, X_train.shape[0]), random_state=790671489).fillna(mode)

    # Sample some rows from the validation set to explain. Increase the sample size for more thorough results.
    example = X_val.sample(n=min(100, X_val.shape[0]), random_state=790671489).fillna(mode)

    # Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
    predict = lambda x: model.predict_proba(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="logit")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=100)
    summary_plot(shap_values, example, class_names=model.classes_)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference
# MAGIC [The MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) is a collaborative hub where teams can share ML models, work together from experimentation to online testing and production, integrate with approval and governance workflows, and monitor ML deployments and their performance. The snippets below show how to add the model trained in this notebook to the model registry and to retrieve it later for inference.
# MAGIC
# MAGIC > **NOTE:** The `model_uri` for the model already trained in this notebook can be found in the cell below
# MAGIC
# MAGIC ### Register to Model Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC registered_model_version = mlflow.register_model(model_uri, model_name)
# MAGIC ```
# MAGIC
# MAGIC ### Load from Model Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC model_version = registered_model_version.version
# MAGIC
# MAGIC model_uri=f"models:/{model_name}/{model_version}"
# MAGIC from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC fe = FeatureEngineeringClient()
# MAGIC fe.score_batch(model_uri=model_uri, df=input_X) # specify `result_type` if it is not "double"
# MAGIC ```
# MAGIC
# MAGIC ### Load model without registering
# MAGIC ```
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC
# MAGIC from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC fe = FeatureEngineeringClient()
# MAGIC fe.score_batch(model_uri=model_uri, df=input_X) # specify `result_type` if it is not "double"
# MAGIC ```

# COMMAND ----------

# model_uri for the generated model
print(f"runs:/{ mlflow_run.info.run_id }/model")

# COMMAND ----------

mlflow_run.info.experiment_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create/Materialize baseline table (for ad-hoc model monitoring)

# COMMAND ----------

# Convert test/baseline pandas dataframe into pyspark dataframe
test_baseline_df = spark.createDataFrame(y_test.reset_index())

# COMMAND ----------

baseline_table_name = f"{catalog}.{dbName}.{inference_table_name}_baseline"
baseline_model_version = client.get_model_version_by_alias(name=model_name, alias="Baseline").version # Champion

baseline_predictions_df = fe.score_batch(
    df=test_baseline_df,
    model_uri=f"models:/{model_name}/{baseline_model_version}",
    result_type=test_baseline_df.schema[label_col].dataType
  ).withColumn("Model_Version", F.lit(baseline_model_version))

(
  baseline_predictions_df.drop(timestamp_col)
  .write
  .format("delta")
  .mode("overwrite") # "append" also works if baseline evolves
  .option("overwriteSchema",True)
  .option("delta.enableChangeDataFeed", "true")
  .saveAsTable(baseline_table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confusion matrix, ROC and Precision-Recall curves for validation data
# MAGIC
# MAGIC We show the confusion matrix, ROC and Precision-Recall curves of the model on the validation data.
# MAGIC
# MAGIC For the plots evaluated on the training and the test data, check the artifacts on the MLflow run page.

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
# MAGIC Next step: [Search runs and trigger model promotion validation]($./03_from_notebook_to_registry)
