# Databricks notebook source
# MAGIC %md
# MAGIC # 5. Optimisation
# MAGIC
# MAGIC  We'll walk through an optimisation use case focused on maximizing iron ore concentrate yield, while maintaining stable levels of silicon (Si) concentrate. This demonstrates how predictive models can be integrated into operational workflows to drive smarter, constraint-aware decisions in mineral processing.

# COMMAND ----------

# MAGIC %run ../demo_setup/00.Initial_library_install

# COMMAND ----------

import pandas as pd
import numpy as np
import mlflow
import lightgbm as lgb
from bayes_opt import BayesianOptimization
from sklearn.metrics import mean_absolute_percentage_error
from databricks.feature_store import FeatureStoreClient
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient

mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()
fs = FeatureStoreClient()

train_and_test_ds = fs.read_table(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.ml_train_test_data").toPandas()
oot_ds = fs.read_table(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.ml_oot_data")
oot_ds = oot_ds.toPandas()

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
# MAGIC ## 5.1 Set Optimisation Parameters and Objective function

# COMMAND ----------

# Define bounds for the 3 features (customize as needed)
pbounds = {
    'Starch_Flow': (train_and_test_ds['Starch_Flow'].min(), train_and_test_ds['Starch_Flow'].max()),
    'Amina_Flow': (train_and_test_ds['Amina_Flow'].min(), train_and_test_ds['Amina_Flow'].max()),
    'Ore_Pulp_Flow': (train_and_test_ds['Ore_Pulp_Flow'].min(), train_and_test_ds['Ore_Pulp_Flow'].max()),
}

# COMMAND ----------

def run_custom_timestamp(date):
    # IDENTIFY PREVIOUS ROW
    row_id = oot_ds.loc[oot_ds['date'] == date].index.tolist()[0]

    # GET ROW VALUES
    oot_ds_row = pd.DataFrame(oot_ds.loc[row_id].values.reshape(1, -1), columns=oot_ds.columns)
    
    si_baseline = si_model.predict(oot_ds_row[si_input_columns])[0]
    fe_baseline = fe_model.predict(oot_ds_row[fe_input_columns])[0]
    penalty_value = -1e6  # large negative number to penalize constraint violation

    # Define the objective function 
    def constrained_objective(Starch_Flow, Amina_Flow, Ore_Pulp_Flow):
        input_vector = oot_ds_row.copy()
        input_vector["Starch_Flow"] = Starch_Flow
        input_vector["Amina_Flow"] = Amina_Flow
        input_vector["Ore_Pulp_Flow"] = Ore_Pulp_Flow

        y1 = fe_model.predict(input_vector[fe_input_columns])[0]  # objective
        y2 = si_model.predict(input_vector[si_input_columns])[0]  # constraint
        
        if y2 > si_baseline:
            return penalty_value  # penalize invalid solutions
        return y1

    # Initialize the optimizer
    optimizer = BayesianOptimization(
        f=constrained_objective,
        pbounds=pbounds,
        random_state=42,
        verbose=2
    )

    # Run optimization
    optimizer.maximize(
        init_points=5,
        n_iter=30,
    )

    # Best result
    print("Current parameters and best parameters found:")
    starch_flow_current = oot_ds_row['Starch_Flow'].values[0]
    amina_flow_current = oot_ds_row['Amina_Flow'].values[0]
    ore_pulp_flow_current = oot_ds_row['Ore_Pulp_Flow'].values[0]

    starch_flow_optimised = optimizer.max['params']['Starch_Flow']
    amina_flow_optimised = optimizer.max['params']['Amina_Flow']
    ore_pulp_flow_optimised = optimizer.max['params']['Ore_Pulp_Flow']
    
    print(f"Starch Flow Sepoint (current, optimised): {starch_flow_current:.4f}, {starch_flow_optimised:.4f}")
    print(f"Amina Flow Sepoint (current, optimised): {amina_flow_current:.4f}, {amina_flow_optimised:.4f}")
    print(f"Ore Pulp Flow Sepoint (current, optimised): {ore_pulp_flow_current:.4f}, {ore_pulp_flow_optimised:.4f}")
    
    input_vector = oot_ds_row.copy()
    input_vector["Starch_Flow"] = optimizer.max['params']['Starch_Flow']
    input_vector["Amina_Flow"] = optimizer.max['params']['Amina_Flow']
    input_vector["Ore_Pulp_Flow"] = optimizer.max['params']['Ore_Pulp_Flow']
    si_optimised = si_model.predict(input_vector[si_input_columns])[0]
    fe_optimised = fe_model.predict(input_vector[fe_input_columns])[0]
    
    print(f"Maximum Model Si prediction (current, optimised): {si_baseline:.4f}, {si_optimised:.4f}")
    print(f"Maximum Model Fe prediction (current, optimised): {fe_baseline:.4f}, {fe_optimised:.4f}")

    return([date, 
            starch_flow_current, 
            starch_flow_optimised, 
            amina_flow_current, 
            amina_flow_optimised, 
            ore_pulp_flow_current,
            ore_pulp_flow_optimised,
            si_baseline,
            si_optimised,
            fe_baseline,
            fe_optimised,
            ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Run Optimisation for a Selected Hour

# COMMAND ----------

# MANUAL INPUTS 
date = '2017-08-25 01:00:00'
run_custom_timestamp(date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Run Counterfactual for all OOT hours

# COMMAND ----------

column_names = [
    "date", 
    "Starch_Flow", 
    "Starch_Flow_optimised", 
    "Amina_Flow", 
    "Amina_Flow_optimised", 
    "Ore_Pulp_Flow",
    "Ore_Pulp_Flow_optimised",
    "Si_baseline",
    "Si_optimised",
    "Fe_baseline",
    "Fe_optimised"
]

for idx, date in enumerate(oot_ds['date'][0:100]):
    print(date)
    results = run_custom_timestamp(date)
    if idx == 0:
        results_df = pd.DataFrame([results], columns=column_names)
    else:
        results_df = pd.concat([results_df, pd.DataFrame([results], columns=column_names)], ignore_index=True)

results_df['Si_Change'] = results_df['Si_optimised'] - results_df['Si_baseline']
results_df['Fe_Uplift'] = results_df['Fe_optimised'] - results_df['Fe_baseline']
display(results_df)

# COMMAND ----------

# Save the table
table_name = "gold_iron_ore_optimisations"
spark_results_df = spark.createDataFrame(results_df)
spark_results_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{table_name}")
