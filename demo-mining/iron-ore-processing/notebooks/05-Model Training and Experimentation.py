# Databricks notebook source
# MAGIC %run ../demo_setup/00.Initial_library_install

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Model Training and Experimentation

# COMMAND ----------

import lightgbm as lgb
import pandas as pd
import numpy as np
import mlflow
import shap
import matplotlib.pyplot as plt
import os

import mlflow.lightgbm
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK
from bayes_opt import BayesianOptimization
from mlflow.models import infer_signature
from databricks.feature_store import FeatureStoreClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient

mlflow.set_registry_uri("databricks-uc")
mlflow.autolog(disable=True)

fs = FeatureStoreClient()
fe = FeatureEngineeringClient()
client = mlflow.MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC We use `mlflow.create_experiment()` to explicitly define and organize our ML experiment, ensuring that all runs related to this use case (e.g., iron ore processing predictions) are logged under a dedicated experiment in MLflow.
# MAGIC
# MAGIC This allows us to:
# MAGIC - 🗂️ Group related runs together for easier comparison and tracking
# MAGIC - 📊 Log and visualize key metrics, parameters, models, and artifacts in a central place
# MAGIC - 🔁 Revisit and reproduce results consistently over time
# MAGIC - 📎 Maintain a clear audit trail of the model development process

# COMMAND ----------

EXP_NAME = f"/Shared/iron_ore_precessing_demo/process_control_demo_experiments"

if mlflow.get_experiment_by_name(EXP_NAME) is None:
    mlflow.create_experiment(name=EXP_NAME)
experiment = mlflow.set_experiment(EXP_NAME)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 📂 Load Dataset & Split into Train, Test, and Out-of-Time Sets
# MAGIC
# MAGIC In this step, we load the prepared dataset and split it into training, testing, and out-of-time (OOT) sets. This allows us to train the model, evaluate its performance, and validate generalisation on unseen data that simulates future conditions.

# COMMAND ----------

def save_feature_table(table_name, data):
    # Drop the fs table if it was already existing to cleanup the demo state
    drop_fs_table(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table_name}")

    df = spark.createDataFrame(data)
    try:
        spark.sql(f"""
        ALTER TABLE {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table_name} ALTER COLUMN date SET NOT NULL
        """)
        
        spark.sql(f"""
            ALTER TABLE {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table_name}
            ADD CONSTRAINT pk PRIMARY KEY(date)
        """)
    except:
        pass

    # Create feature table using the feature store client
    fs.create_table(
        name=f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table_name}",
        primary_keys=["date"],
        df=df,
        description=""
    )

# COMMAND ----------

# Load dataset
df_master = fs.read_table(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.fs_gold_iop_features")

# Convert data into pandas dataframe
pandas_df = df_master.toPandas()

# Split dataset into train, test and out-of-time validation sets for counterfatual evaluation
train_and_test_ds = df_master[df_master.date < '2017-08-25'].toPandas()
oot_ds = df_master[df_master.date >= '2017-08-25']
oot_ds = oot_ds.orderBy(oot_ds.date.asc())
print((train_and_test_ds.count(), len(train_and_test_ds.columns)), (oot_ds.count(), len(oot_ds.columns)))

save_feature_table('ml_train_test_data', train_and_test_ds)
save_feature_table('ml_oot_data', oot_ds.toPandas())

# COMMAND ----------

# list of features to train model
model_features = [
  'Percent_Iron_Feed',
  'Percent_Silica_Feed',
  'Starch_Flow',
  'Amina_Flow', 
  'Ore_Pulp_Flow', 
  'Ore_Pulp_pH', 
  'Ore_Pulp_Density',
  'Flotation_Column_01_Air_Flow',
  'Flotation_Column_02_Air_Flow',
  'Flotation_Column_03_Air_Flow',
  'Flotation_Column_04_Air_Flow',
  'Flotation_Column_05_Air_Flow',
  'Flotation_Column_06_Air_Flow',
  'Flotation_Column_07_Air_Flow',
  'Flotation_Column_01_Level',
  'Flotation_Column_02_Level',
  'Flotation_Column_03_Level',
  'Flotation_Column_04_Level',
  'Flotation_Column_05_Level',
  'Flotation_Column_06_Level',
  'Flotation_Column_07_Level',
  ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Model Hyperparameter Tuning 
# MAGIC
# MAGIC In this section, we demonstrate how to use Hyperopt for automated hyperparameter tuning and how to leverage **MLflow** to track experiments, log performance metrics, and capture model explainability outputs.
# MAGIC
# MAGIC This example:
# MAGIC - Splits the dataset into training and testing sets
# MAGIC - Defines a hyperparameter search space for a LightGBM model
# MAGIC - Uses Hyperopt to find the best combination of parameters that minimises Mean Absolute Percentage Error (MAPE)
# MAGIC - Logs all relevant information to MLflow, including:
# MAGIC   - Input hyperparameters
# MAGIC   - Train/test metrics for each run 
# MAGIC   - The final trained model (versioned in MLflow)
# MAGIC   - SHAP-based feature importance plots and summary visualisations for explainability
# MAGIC
# MAGIC This workflow not only shows how to automate model selection, but also how to capture rich experiment metadata to support reproducibility, comparison, and collaboration across the team.

# COMMAND ----------

# Run Hyperopt tuning
def run_tuning(target, run_name):
    X_train, X_test, y_train, y_test = train_test_split(
      train_and_test_ds[model_features], train_and_test_ds[target], test_size=0.2, random_state=42)
    
    space = {
      "run_name": run_name,
      "n_estimators": hp.choice('n_estimators', [100, 150, 200, 500]),
      "learning_rate": hp.choice('learning_rate', [0.1, 0.25, 0.5]),
      "num_leaves": hp.choice('num_leaves', [5, 10, 50, 100]),
      "max_depth": hp.choice('max_depth', [5, 10, 15, 30]),
      "random_state": 42
    }

    # Objective function for Hyperopt
    def objective(params):
        with mlflow.start_run(run_name=params["run_name"], nested=True):
            model = lgb.LGBMRegressor(**params)
            model.fit(X_train, y_train)

            train_preds = model.predict(X_train)
            test_preds = model.predict(X_test)

            train_mape = mean_absolute_percentage_error(y_train, train_preds)
            test_mape = mean_absolute_percentage_error(y_test, test_preds)

            # Log metrics and parameters
            mlflow.log_params(params)
            mlflow.log_metric("train_mape", train_mape)
            mlflow.log_metric("test_mape", test_mape)

            return {'loss': test_mape, 'status': STATUS_OK}

    with mlflow.start_run(run_name=space["run_name"], experiment_id=experiment.experiment_id):
        trials = Trials()
        best_params = fmin(
            fn=objective, space=space, algo=tpe.suggest, max_evals=20, trials=trials
        )

        signature = infer_signature(X_train, y_train)

        # Map index-based params back to actual values
        best_params["num_leaves"] = int([5, 10, 50, 100][best_params["num_leaves"]])
        best_params["max_depth"] = int([5, 10, 15, 30][best_params["max_depth"]])
        best_params["n_estimators"] = int(
            [100, 150, 200, 500][best_params["n_estimators"]]
        )
        best_params["learning_rate"] = [0.1, 0.25, 0.5][best_params["learning_rate"]]

        # Train final model
        final_model = lgb.LGBMRegressor(**best_params)
        final_model.fit(X_train, y_train)

        train_preds = final_model.predict(X_train)
        test_preds = final_model.predict(X_test)

        train_mape = mean_absolute_percentage_error(y_train, train_preds)
        test_mape = mean_absolute_percentage_error(y_test, test_preds)

        # Log final model and performance
        mlflow.log_params(best_params)
        mlflow.log_metric("final_train_mape", train_mape)
        mlflow.log_metric("final_test_mape", test_mape)
        mlflow.lightgbm.log_model(
            final_model, f"lightgbm_{run_name}_model", signature=signature
        )

        # Model explainability with SHAP
        explainer = shap.Explainer(final_model, X_train)
        shap_values = explainer(X_test, check_additivity=False)

        # Plot summary of SHAP values
        shap.summary_plot(shap_values, X_test, show=False)
        plot_path = f"../outputs/shap_summary_plot_{run_name}.png"
        plt.tight_layout()
        plt.savefig(plot_path, bbox_inches="tight")
        plt.close()
        mlflow.log_artifact(plot_path, artifact_path="shap_explainability")

        # Feature importance bar plot
        plt.figure()
        shap.plots.bar(shap_values, show=False)
        bar_path = f"../outputs/shap_bar_{run_name}.png"
        plt.tight_layout()
        plt.savefig(bar_path, bbox_inches="tight")
        plt.close()
        mlflow.log_artifact(bar_path, artifact_path="shap_explainability")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2.1 Fe Concentrate Prediction 

# COMMAND ----------

target = 'Percent_Iron_Concentrate'
run_name = 'iron_ore_quality_fe_concentrate'
run_tuning(target, run_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2.2 Si Concentrate Prediction 

# COMMAND ----------

target = 'Percent_Silica_Concentrate'
run_name = 'iron_ore_quality_si_concentrate'

run_tuning(target, run_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Identify Best Model and Register to Unity Catalog

# COMMAND ----------

best_model_si = mlflow.search_runs(
  experiment_ids=experiment.experiment_id,
  order_by=["metrics.final_test_mape"],
  max_results=1,
  filter_string="status = 'FINISHED' and run_name='iron_ore_quality_si_concentrate'"
)

best_model_fe = mlflow.search_runs(
  experiment_ids=experiment.experiment_id,
  order_by=["metrics.final_test_mape"],
  max_results=1,
  filter_string="status = 'FINISHED' and run_name='iron_ore_quality_fe_concentrate'" 
)

# COMMAND ----------

# SI
mv = mlflow.register_model(
    f"runs:/{best_model_si.iloc[0]['run_id']}/lightgbm_iron_ore_quality_si_concentrate_model", f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.si_model"
)

alias = "Champion" if mv.version == '1' else "Challenger"
client.set_registered_model_alias(
  f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.si_model", alias, mv.version
)

# FE
mv = mlflow.register_model(
  f"runs:/{best_model_fe.iloc[0]['run_id']}/lightgbm_iron_ore_quality_fe_concentrate_model", 
  f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.fe_model"
)
alias = "Champion" if mv.version == '1' else "Challenger"
client.set_registered_model_alias(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.fe_model", alias, mv.version)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Model Inference
# MAGIC
# MAGIC In this section, we showcase how champion models are seamlessly loaded from Unity Catalog for model inference.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4.1 Load Champion Models & Model Features from UC

# COMMAND ----------

# Si Concentrate Model:
si_model = mlflow.pyfunc.load_model('models:/mining_iron_ore_processing_demo_catalog.iop_schema.si_model@Champion')
si_input_schema = si_model.metadata.get_input_schema()
si_input_columns = [col.name for col in si_input_schema]
si_label = [t.name for t in si_model.metadata.get_output_schema()][0]

# Fe Concentrate Model:
fe_model = mlflow.pyfunc.load_model('models:/mining_iron_ore_processing_demo_catalog.iop_schema.fe_model@Champion')
fe_input_schema = fe_model.metadata.get_input_schema()
fe_input_columns = [col.name for col in fe_input_schema]
fe_label = [t.name for t in fe_model.metadata.get_output_schema()][0]


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3.4.2 Predict Si and Fe and Save Results to a Table

# COMMAND ----------

oot_ds = oot_ds.toPandas()
si_predictions = si_model.predict(oot_ds[si_input_columns])
fe_predictions = fe_model.predict(oot_ds[fe_input_columns])

# COMMAND ----------

from sklearn.metrics import mean_absolute_percentage_error

# Calculate MAPE for Si predictions
si_mape = mean_absolute_percentage_error(oot_ds[si_label], si_predictions)

# Calculate MAPE for Fe predictions
fe_mape = mean_absolute_percentage_error(oot_ds[fe_label], fe_predictions)

si_mape, fe_mape

# COMMAND ----------

oot_ds["si_prediction"] = si_predictions
oot_ds["fe_prediction"] = fe_predictions

display(oot_ds)

table_name = "gold_iron_ore_predictions"
spark_df = spark.createDataFrame(oot_ds)
spark_df.write.format("delta").mode("overwrite").saveAsTable(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Evaluate Model Performance with MLFLow Evaluate
# MAGIC
# MAGIC MLFLow has inbuilt evaluation capabilties that allows you to generate a comprehensive evaluation against a given dataset, for a suite of metrics. This triggers a MLFLow evaluation that captures the evaluation and all of the assiociated artifacts. The evaluation can be accessed to compare across models or to use the results in downstream processes.

# COMMAND ----------

EXP_NAME = f"/Shared/iron_ore_precessing_demo/process_control_demo_experiments_eval"

if mlflow.get_experiment_by_name(EXP_NAME) is None:
    mlflow.create_experiment(name=EXP_NAME)
experiment = mlflow.set_experiment(EXP_NAME)

with mlflow.start_run(run_name="iron_ore_quality_eval", experiment_id=experiment.experiment_id):
    si_eval = mlflow.models.evaluate(
        model=si_model,
        data=oot_ds,
        targets="Percent_Silica_Concentrate",
        model_type="regressor",
        evaluators=["default"],
    )

    fe_eval = mlflow.models.evaluate(
        model=fe_model,
        data=oot_ds,
        targets="Percent_Iron_Concentrate",
        model_type="regressor",
        evaluators=["default"],
    )

# COMMAND ----------

print("All Metrics:")
for metric_name, value in fe_eval.metrics.items():
    print(f"  {metric_name}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 AutoML
# MAGIC
# MAGIC **Databricks AutoML** simplifies the model development process by automatically running a series of experiments to identify the best-performing model for your dataset. You can easily select the training data, specify which features to include or exclude, and configure key model parameters — all while Databricks handles model selection, tuning, and tracking behind the scenes.
# MAGIC
# MAGIC ### 🧠 Steps to Run AutoML in Databricks
# MAGIC
# MAGIC - Launch AutoML
# MAGIC   - From the Databricks workspace UI, go to the "Machine Learning" section.
# MAGIC   - Click "Create AutoML experiment".
# MAGIC - Configure the experiment
# MAGIC   - Select the task type: Classification, Regression, or Forecasting.
# MAGIC   - Choose your training dataset and target column.
# MAGIC   - Optionally configure:
# MAGIC     - Columns to include/exclude
# MAGIC     - Primary metric (e.g., accuracy, MAPE, AUC)
# MAGIC     - Runtime limit and experiment name
# MAGIC - Run the AutoML experiment
# MAGIC   - Databricks AutoML will:
# MAGIC     - Profile your dataset
# MAGIC     - Automatically preprocess data (e.g., encoding, imputation)
# MAGIC     - Train and evaluate multiple models using different algorithms and hyperparameters
# MAGIC     - Track results in MLflow and log the full pipeline in a generated notebook
# MAGIC - Review the results
# MAGIC   - Explore the leaderboard of model runs, ranked by your selected metric
# MAGIC   - Open the generated notebook to inspect preprocessing, model training code, and evaluation
# MAGIC   - Review feature importance and other insights
# MAGIC - Register or deploy the best model
# MAGIC   - Register the top-performing model in the MLflow Model Registry
# MAGIC   - Optionally deploy the model for batch or real-time inference
# MAGIC
# MAGIC ![](/Workspace/Shared/iron_ore_precessing_demo/demo_setup/images/automl-data-selection.png)
# MAGIC ![](/Workspace/Shared/iron_ore_precessing_demo/demo_setup/images/automl-setup.png)
# MAGIC
# MAGIC Once the training is complete, you can see the resulting runs, as well as the notebook that was used to generate the best model.
# MAGIC
# MAGIC ![](/Workspace/Shared/iron_ore_precessing_demo/demo_setup/images/automl-complete.png)