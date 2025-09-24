# Databricks notebook source
# MAGIC %md
# MAGIC # HPO Model Training using Optuna & MLflow
# MAGIC
# MAGIC We'll run a couple of trainings to test different models
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-2-v2.png?raw=True" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_auto_ml&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Auto-ML notebook",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["auto-ml"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC databricks-feature-engineering==0.13.0a8
# MAGIC mlflow==3.3.2
# MAGIC optuna>=4.4.0
# MAGIC DBR/MLR16.4LTS (Recommended - Non-Autoscaling Cluster) OR Serverless v2
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering>=0.13.0a8 mlflow --upgrade lightgbm optuna
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Construct Training Set
# MAGIC Load data directly from feature store
# MAGIC

# COMMAND ----------

display(spark.table("advanced_churn_feature_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We'll also use specific feature functions for on-demand features.
# MAGIC
# MAGIC Recall that we have defined the `avg_price_increase` feature function in the [feature engineering notebook]($./01_feature_engineering)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION avg_price_increase

# COMMAND ----------

# MAGIC %md
# MAGIC Create feature specifications.
# MAGIC
# MAGIC The feature lookup definition specifies the tables to use as feature tables and the key to lookup feature values.
# MAGIC
# MAGIC The feature function definition specifies which columns from the feature table are bound to the function inputs.
# MAGIC
# MAGIC The Feature Engineering client will use these values to create a training specification that's used to assemble the training dataset from the labels table and the feature table.

# COMMAND ----------

# DBTITLE 1,Define feature lookups
from databricks.feature_store import FeatureFunction, FeatureLookup


feature_lookups_n_functions = [
    FeatureLookup(
      table_name=f"{catalog}.{db}.advanced_churn_feature_table",
      lookup_key=["customer_id"],
      timestamp_lookup_key="transaction_ts"
    ),
    FeatureFunction(
      udf_name=f"{catalog}.{db}.avg_price_increase",
      input_bindings={
        "monthly_charges_in" : "monthly_charges",
        "tenure_in" : "tenure",
        "total_charges_in" : "total_charges"
      },
      output_name="avg_price_increase"
    )
]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Read the label table.

# COMMAND ----------

# DBTITLE 1,Pull labels to use for training/validating/testing
labels_df = spark.read.table(f"advanced_churn_label_table")

# Set variable for label column. This will be used within the training code.
label_col = "churn"
pos_label = "Yes"

# COMMAND ----------

# DBTITLE 1,filter/ensure we take latest label for every customer
from pyspark.sql.functions import col, last, max


latest_customer_ids_df = labels_df \
    .groupBy("customer_id") \
    .agg(
        max("transaction_ts").alias("transaction_ts"),
        last(label_col).alias(label_col),
        last("split").alias("split")
    )

# display(latest_customer_ids_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define training set specifications
# MAGIC
# MAGIC This contains information on how the training set should be assembled from the label table, feature table, and feature function using the FE ([AWS](https://docs.databricks.com/aws/en/machine-learning/feature-store/python-api)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/python-api)|[GCP](https://docs.databricks.com/gcp/en/machine-learning/feature-store/python-api))`create_training_set()` method
# MAGIC
# MAGIC One can use [Point-In-Time](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series) feature/label joins.

# COMMAND ----------

# DBTITLE 1,Create Training specifications
from databricks.feature_engineering import FeatureEngineeringClient
from datetime import timedelta


fe = FeatureEngineeringClient()

# Create Feature specifications object
training_set_specs = fe.create_training_set(
  df=latest_customer_ids_df, # DataFrame with lookup keys and label/target (+ any other input)
  label="churn",
  feature_lookups=feature_lookups_n_functions,
  exclude_columns=["customer_id", "transaction_ts"],
  exclude_null_labels=True,
  # lookback_window=timedelta(days=1), # Set to None for no lookback window
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC With the training set specification, we can now build the training dataset.
# MAGIC
# MAGIC `training_set_specs.load_df()` returns a pySpark dataframe. We will convert it to a Pandas dataframe.

# COMMAND ----------

# DBTITLE 1,Load training set as Pandas dataframe
# Load as spark dataframes, filter for train/test and convert to pandas dataframes
training_pdf = training_set_specs.load_df().filter("split == 'train'").drop("split").toPandas()
test_pdf = training_set_specs.load_df().filter("split == 'test'").drop("split").toPandas()

# Training
X_train, Y_train = (training_pdf.drop(label_col, axis=1), training_pdf[label_col])

# Same held-out set for every training run for repro and ensuring fair comparison between runs
X_test, Y_test = (test_pdf.drop(label_col, axis=1), test_pdf[label_col])

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
# MAGIC Missing values for numerical columns are imputed with the mean by default.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler


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

one_hot_imputers = []
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", SklearnOneHotEncoder(handle_unknown="ignore")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"])]

# COMMAND ----------

from sklearn.compose import ColumnTransformer


transformers = bool_transformers + numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="drop", sparse_threshold=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scaling up Hyper-Parameter-Optimization (HPO) with Optuna, Spark and MLflow
# MAGIC
# MAGIC Optuna is an advanced hyperparameter optimization framework designed specifically for machine learning tasks. Here are the key ways Optuna conducts hyperparameter optimization primarly via:
# MAGIC
# MAGIC 1. Sampling Algorithms:
# MAGIC     1. **Tree-structured Parzen Estimator (Default)** - Bayesian optimization to efficiently search the hyperparameter space.
# MAGIC     2. Random Sampling - Randomly samples hyperparameter values from the search space.
# MAGIC     3. CMA-ES (Covariance Matrix Adaptation Evolution Strategy) - An evolutionary algorithm for difficult non-linear non-convex optimization problems.
# MAGIC     4. Grid Search - Exhaustively searches through a manually specified subset of the hyperparameter space.
# MAGIC     5. Quasi-Monte Carlo sampling - Uses low-discrepancy sequences to sample the search space more uniformly than pure random sampling.
# MAGIC     6. NSGA-II (Non-dominated Sorting Genetic Algorithm II) - A multi-objective optimization algorithm.
# MAGIC     7. Gaussian Process-based sampling (i.e. Kriging) - Uses Gaussian processes for Bayesian optimization.
# MAGIC     8. Optuna also allows implementing custom samplers by inheriting from the `BaseSampler` class.
# MAGIC 2. Pruning: Optuna implements pruning algorithms to early-stop unpromising trials
# MAGIC 3. Study Object: Users create a "study" object that manages the optimization process. The study.optimize() method is called to start the optimization, specifying the objective function and number of trials.
# MAGIC 4. Parallel Execution: Optuna scales to parallel execution on a single node and multi-node.
# MAGIC
# MAGIC
# MAGIC We will leverage the new `MLflowStorage` and `MlflowSparkStudy` objects.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define objective/loss function and search space to optimize
# MAGIC The search space here is defined by calling functions such as `suggest_categorical`, `suggest_float`, `suggest_int` for the Trial object that is passed to the objective function. Optuna allows to define the search space dynamically.
# MAGIC
# MAGIC Refer to the documentation for:
# MAGIC * [optuna.samplers](https://optuna.readthedocs.io/en/stable/reference/samplers/index.html) for the choice of samplers
# MAGIC * [optuna.pruners](https://optuna.readthedocs.io/en/stable/reference/pruners.html) for the choice of pruners
# MAGIC * [optuna.trial.Trial](https://optuna.readthedocs.io/en/stable/reference/generated/optuna.trial.Trial.html) for a full list of functions supported to define a hyperparameter search space.

# COMMAND ----------

import optuna
from lightgbm import LGBMClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split


class ObjectiveOptuna(object):
  """
  a callable class for implementing the objective function. It takes the training dataset by a constructor's argument
  instead of loading it in each trial execution. This will speed up the execution of each trial
  """
  def __init__(self, X_train_in:pd.DataFrame, Y_train_in:pd.Series, preprocessor_in: ColumnTransformer, rng_seed:int=2025, pos_label_in:str=pos_label):
    """
    X_train_in: features
    Y_train_in: label
    """

    # Set pre-processing pipeline
    self.preprocessor = preprocessor_in
    self.rng_seed = rng_seed
    self.pos_label = pos_label_in

    # Split into training and validation set
    X_train, X_val, Y_train, Y_val = train_test_split(X_train_in, Y_train_in, test_size=0.1, random_state=rng_seed)

    self.X_train = X_train
    self.Y_train = Y_train
    self.X_val = X_val
    self.Y_val = Y_val
    
  def __call__(self, trial):
    """
    Wrapper call containing data processing pipeline, training and hyperparameter tuning code.
    The function returns the weighted F1 accuracy metric to maximize in this case.
    """

    # Define list of classifiers to test
    classifier_name = trial.suggest_categorical("classifier", ["LogisticRegression", "RandomForest", "LightGBM"])
    
    if classifier_name == "LogisticRegression":
      # Optimize tolerance and C hyperparameters
      lr_C = trial.suggest_float("C", 1e-2, 1, log=True)
      lr_tol = trial.suggest_float('tol' , 1e-6 , 1e-3, step=1e-6)
      classifier_obj = LogisticRegression(C=lr_C, tol=lr_tol, random_state=self.rng_seed)

    elif classifier_name == "RandomForest":
      # Optimize number of trees, tree depth, min_sample split and leaf hyperparameters
      n_estimators = trial.suggest_int("n_estimators", 10, 200, log=True)
      max_depth = trial.suggest_int("max_depth", 3, 10)
      min_samples_split = trial.suggest_int("min_samples_split", 2, 10)
      min_samples_leaf = trial.suggest_int("min_samples_leaf", 1, 10)
      classifier_obj = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth,
                                              min_samples_split=min_samples_split, min_samples_leaf=min_samples_leaf, random_state=self.rng_seed)

    elif classifier_name == "LightGBM":
      # Optimize number of trees, tree depth, learning rate and maximum number of bins hyperparameters
      n_estimators = trial.suggest_int("n_estimators", 10, 200, log=True)
      max_depth = trial.suggest_int("max_depth", 3, 10)
      learning_rate = trial.suggest_float("learning_rate", 1e-2, 0.9)
      max_bin = trial.suggest_int("max_bin", 2, 256)
      num_leaves = trial.suggest_int("num_leaves", 2, 256),
      classifier_obj = LGBMClassifier(force_row_wise=True, verbose=-1,
                                      n_estimators=n_estimators, max_depth=max_depth, learning_rate=learning_rate, max_bin=max_bin, num_leaves=num_leaves, random_state=self.rng_seed)
      
    # Assemble the pipeline
    this_model = Pipeline(steps=[("preprocessor", self.preprocessor), ("classifier", classifier_obj)])

    # Fit the model
    mlflow.sklearn.autolog(disable=True) # Disable mlflow autologging to avoid logging artifacts for every run
    
    this_model.fit(self.X_train, self.Y_train)


    # Predict on validation set
    y_val_pred = this_model.predict(self.X_val)

    # Calculate and return F1-Score
    f1_score_binary= f1_score(self.Y_val, y_val_pred, average="binary", pos_label=self.pos_label)

    return f1_score_binary

# COMMAND ----------

# DBTITLE 1,Define the Sampler
optuna_sampler = optuna.samplers.TPESampler(
  seed=2025 # Random Number Generator seed
)

# COMMAND ----------

# DBTITLE 1,Define the Pruner
from optuna.pruners import BasePruner


class NoneValuePruner(BasePruner):
  """Custom Pruner to ignore failed trials with None value."""

  def prune(self, study, trial):
    # If the trial's value is None, prune it
    if trial.value is None:
      return True
      
    else:
      return False

# COMMAND ----------

# MAGIC %md
# MAGIC #### Quick test/debug locally

# COMMAND ----------

objective_fn = ObjectiveOptuna(X_train, Y_train, preprocessor, pos_label_in=pos_label)

# COMMAND ----------

# DBTITLE 1,without mlflow
study_debug = optuna.create_study(direction="maximize", study_name="test_debug", sampler=optuna_sampler, pruner= NoneValuePruner())
study_debug.optimize(objective_fn, n_trials=4, n_jobs=-1)

# COMMAND ----------

print("Best trial:")
best_trial = study_debug.best_trial
print(f"  F1_score: {best_trial.value}")
print("  Params: ")
for key, value in best_trial.params.items():
    print(f"    {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using native `MLFlow.spark.optuna` flavor: Create a shared storage for distributed optimization
# MAGIC With `MlflowStorage`, the MLflow Tracking Server can serve as the storage backend. See documentation for [AWS](https://docs.databricks.com/aws/en/machine-learning/automl-hyperparam-tuning/optuna)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/automl-hyperparam-tuning/optuna)|[GCP](https://docs.databricks.com/gcp/en/machine-learning/automl-hyperparam-tuning/optuna).
# MAGIC
# MAGIC **TO-DO:** Update function to rename objective loss metric to `val_f1_score` once this will be supported (now loss metric is logged as `value`)

# COMMAND ----------

# DBTITLE 1,Point/Set experiment
experiment_name = f"{xp_path}/{xp_name}" # Point to given experiment (Staging/Prod)
# experiment_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # Point to local/notebook experiment (Dev)

try:
  print(f"Loading experiment: {experiment_name}")
  experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id

except Exception as e:
  print(f"Creating experiment: {experiment_name}")
  experiment_id = mlflow.create_experiment(name=experiment_name, tags={"dbdemos":"advanced"})

# COMMAND ----------

import mlflow
from mlflow.optuna.storage import MlflowStorage
from mlflow.pyspark.optuna.study import MlflowSparkStudy


mlflow.set_experiment(f"{xp_path}/{xp_name}")
print(f"Set experiment to: {xp_name}")

mlflow_storage = MlflowStorage(experiment_id=experiment_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick test/debug locally
# MAGIC with MLFlow

# COMMAND ----------

mlflow_optuna_study_debug = MlflowSparkStudy(
  pruner= NoneValuePruner(),
  sampler=optuna_sampler,
  study_name="mlflow-smoke-test",
  storage=mlflow_storage,
)
mlflow_optuna_study_debug._directions = ["maximize"]

mlflow_optuna_study_debug.optimize(objective_fn, n_trials=8, n_jobs=4)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Main training function

# COMMAND ----------

display(X_train)

# COMMAND ----------

import warnings
import pandas as pd
from mlflow.types.utils import _infer_schema
from mlflow.exceptions import MlflowException
from mlflow.models import Model
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc


def optuna_hpo_fn(n_trials: int, X_train: pd.DataFrame, Y_train: pd.Series, X_test: pd.DataFrame, Y_test: pd.Series, training_set_specs_in, preprocessor_in: ColumnTransformer, experiment_id: str, pos_label_in: str = pos_label, rng_seed_in: int = 2025, run_name:str = "spark-mlflow-tuning", optuna_sampler_in: optuna.samplers.TPESampler = optuna_sampler, optuna_pruner_in: optuna.pruners.BasePruner = None, n_jobs: int = 2) -> optuna.study.study.Study:
    """
    Increasing `n_jobs` may cause experiment to fail due to failed trials which return None and can't be pruned/caught in parallel mode
    """

    # Kick distributed HPO as nested runs
    objective_fn = ObjectiveOptuna(X_train, Y_train, preprocessor_in, rng_seed_in, pos_label_in)
    mlflow_optuna_study = MlflowSparkStudy(
        pruner=optuna_pruner_in,
        sampler=optuna_sampler_in,
        study_name=run_name,
        storage=mlflow_storage,
    )
    mlflow_optuna_study._directions = ["maximize"]
    mlflow_optuna_study.optimize(objective_fn, n_trials=n_trials, n_jobs=n_jobs)

    # Extract best trial info
    best_model_params = mlflow_optuna_study.best_params
    best_model_params["random_state"] = rng_seed_in
    classifier_type = best_model_params.pop('classifier')

    # Reproduce best classifier
    if classifier_type  == "LogisticRegression":
        best_model = LogisticRegression(**best_model_params)
    elif classifier_type == "RandomForest":
        best_model = RandomForestClassifier(**best_model_params)
    elif classifier_type == "LightGBM":
        best_model = LGBMClassifier(force_row_wise=True, verbose=-1, **best_model_params)

    # Enable automatic logging of input samples, metrics, parameters, and models
    mlflow.sklearn.autolog(log_input_examples=True, log_models=False, silent=True)

    active_run = mlflow.active_run()
    if not active_run:
        active_run = client.search_runs(
            experiment_ids=[experiment_id],
            filter_string=f"tags.mlflow.runName = '{run_name}'",
            order_by=["start_time DESC"],
            max_results=1)[0]
    
    run_id = active_run.info.run_id

    with mlflow.start_run(run_id=run_id, experiment_id=experiment_id) as run:
        # Fit best model and log using FE client in parent run
        model_pipeline = Pipeline(steps=[("preprocessor", objective_fn.preprocessor), ("classifier", best_model)])
        model_pipeline.fit(X_train, Y_train)

        # Evaluate model and log into experiment
        mlflow_model = Model()
        pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
        pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model_pipeline)

        # Log metrics for the training set
        training_eval_result = mlflow.evaluate(
            model=pyfunc_model,
            data=X_train.assign(**{str(label_col):Y_train}),
            targets=label_col,
            model_type="classifier",
            evaluator_config = {"log_model_explainability": False,
                                "metric_prefix": "training_" , "pos_label": pos_label_in }
        )

        # Log metrics for the test set
        test_eval_result = mlflow.evaluate(
            model=pyfunc_model,
            data=X_test.assign(**{str(label_col):Y_test}),
            targets=label_col,
            model_type="classifier",
            evaluator_config = {"log_model_explainability": True,
                                "metric_prefix": "test_" , "pos_label": pos_label_in }
        )

        # Create/Log model artifacts with embedded feature lookups
        fe.log_model(
            model=model_pipeline,
            artifact_path="model",
            flavor=mlflow.sklearn,
            training_set=training_set_specs_in,
            # env_manager="uv"
        )
        mlflow.end_run()

    return mlflow_optuna_study

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute training runs

# COMMAND ----------

# Invoke training function which will be distributed accross workers/nodes
distributed_study = optuna_hpo_fn(
  n_trials=32, # Decrease this for live demo purposes
  X_train=X_train,
  X_test=X_test,
  Y_train=Y_train,
  Y_test=Y_test,
  training_set_specs_in=training_set_specs,
  preprocessor_in=preprocessor,
  experiment_id=experiment_id,
  pos_label_in=pos_label,
  rng_seed_in=2025,
  run_name="mlops-hpo-best-run", # "smoke-test"
  optuna_sampler_in=optuna_sampler,
  optuna_pruner_in=NoneValuePruner(),
  # n_jobs = 2, # Increase this to number for more parallel trials
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Automate model promotion validation
# MAGIC
# MAGIC Next steps: 
# MAGIC - [Create Model Deployment Job]($./03a_create_deployment_job)
# MAGIC - [Search runs and trigger model promotion validation]($./03b_from_notebook_to_models_in_uc)
