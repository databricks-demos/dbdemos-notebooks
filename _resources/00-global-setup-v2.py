# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Technical Setup notebook. Hide this cell results
# MAGIC Initialize dataset to the current user and cleanup data when reset_all_data is set to true
# MAGIC
# MAGIC Do not edit

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("min_dbr_version", "12.2", "Min required DBR version")

# COMMAND ----------

import requests
import collections
import os

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEMPORARY MLflow serverless shim (ES-2047313)
# MAGIC `mlflow.pyfunc.spark_udf` / `score_batch` currently crash on Serverless with
# MAGIC `InvalidVersion: '18.x-...-photon-scala2'` because MLflow parses the sandbox
# MAGIC runtime version assuming a `<major>.<minor>` format. This shim normalizes the
# MAGIC runtime version at runtime so `spark_udf` works today.
# MAGIC
# MAGIC **This is a stopgap. Remove it once the fixed MLflow build lands (next MLflow version).**

# COMMAND ----------

import re, sys
_UNCUT_MINOR = 999

def _normalize_runtime_version(raw):
    if raw is None:
        return raw
    parts = str(raw).split(".")
    if not parts or not parts[0].isdigit():
        return raw
    major = int(parts[0]); minor = _UNCUT_MINOR
    if len(parts) > 1:
        m = re.match(r"(\d+)", parts[1]); minor = int(m.group(1)) if m else _UNCUT_MINOR
    return f"{major}.{minor}"

def _apply_mlflow_serverless_shim():
    # Stopgap for ES-2047313 - safe no-op if MLflow internals aren't present.
    # Remove once the fixed MLflow build is available on serverless.
    try:
        import mlflow.utils.databricks_utils as du
    except Exception as e:
        print(f"mlflow serverless shim skipped (mlflow not available): {e}")
        return
    orig = getattr(du, "_es2047313_orig", None) or du.get_dbconnect_udf_sandbox_info
    du._es2047313_orig = orig
    def patched(spark):
        info = orig(spark)
        try:
            fixed = _normalize_runtime_version(info.runtime_version)
            if fixed != info.runtime_version:
                info = info.__class__(**{**info.__dict__, "runtime_version": fixed})
        except Exception:
            pass
        return info
    du.get_dbconnect_udf_sandbox_info = patched
    for mod in list(sys.modules.values()):
        if getattr(mod, "get_dbconnect_udf_sandbox_info", None) is orig:
            mod.get_dbconnect_udf_sandbox_info = patched
    try:
        DRV = du.DatabricksRuntimeVersion
    except Exception:
        return
    orig_parse = getattr(DRV, "_es2047313_orig_parse", None) or DRV.parse.__func__
    DRV._es2047313_orig_parse = orig_parse
    def patched_parse(cls, databricks_runtime=None):
        try:
            return orig_parse(cls, databricks_runtime)
        except Exception:
            dbr = databricks_runtime or du.get_databricks_runtime_version()
            is_gpu = dbr.endswith("-gpu")
            if is_gpu: dbr = dbr[:-4]
            sp = dbr.split(".", maxsplit=2)
            if sp[0] == "client":
                major = int(re.match(r"(\d+)", sp[1]).group(1))
                minor = int(re.match(r"(\d+)", sp[2]).group(1)) if len(sp) > 2 and re.match(r"(\d+)", sp[2]) else 0
            else:
                major = int(re.match(r"(\d+)", sp[0]).group(1))
                minor = int(re.match(r"(\d+)", sp[1]).group(1)) if len(sp) > 1 and re.match(r"(\d+)", sp[1]) else _UNCUT_MINOR
            return cls(sp[0] == "client", major, minor, is_gpu)
    DRV.parse = classmethod(patched_parse)

try:
    _apply_mlflow_serverless_shim()
    print("mlflow serverless shim applied (ES-2047313 - remove once mlflow is fixed)")
except Exception as e:
    print(f"mlflow serverless shim not applied: {e}")

# COMMAND ----------


class DBDemos():
  @staticmethod
  def setup_schema(catalog, db, reset_all_data, volume_name = None):
    if reset_all_data:
      print(f'clearing up volume named `{catalog}`.`{db}`.`{volume_name}`')
      try:
        spark.sql(f"DROP VOLUME IF EXISTS `{catalog}`.`{db}`.`{volume_name}`")
        spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{db}` CASCADE")
      except Exception as e:
        print(f'catalog `{catalog}` or schema `{db}` do not exist.  Skipping data reset')

    def use_and_create_db(catalog, dbName, cloud_storage_path = None):
      print(f"USE CATALOG `{catalog}`")
      spark.sql(f"USE CATALOG `{catalog}`")
      spark.sql(f"""create database if not exists `{dbName}` """)

    assert catalog not in ['hive_metastore', 'spark_catalog'], "This demo only support Unity. Please change your catalog name."
    #If the catalog is defined, we force it to the given value and throw exception if not.
    current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
    if current_catalog != catalog:
      catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
      if catalog not in catalogs:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        if catalog == 'dbdemos':
          spark.sql(f"ALTER CATALOG `{catalog}` OWNER TO `account users`")
    use_and_create_db(catalog, db)

    if catalog == 'dbdemos':
      try:
        spark.sql(f"GRANT CREATE, USAGE on DATABASE `{catalog}`.`{db}` TO `account users`")
        spark.sql(f"ALTER SCHEMA `{catalog}`.`{db}` OWNER TO `account users`")
        for t in spark.sql(f'SHOW TABLES in {catalog}.{db}').collect():
          try:
            spark.sql(f'GRANT ALL PRIVILEGES ON TABLE {catalog}.{db}.{t["tableName"]} TO `account users`')
            spark.sql(f'ALTER TABLE {catalog}.{db}.{t["tableName"]} OWNER TO `account users`')
          except Exception as e:
            if "NOT_IMPLEMENTED.TRANSFER_MATERIALIZED_VIEW_OWNERSHIP" not in str(e) and "STREAMING_TABLE_OPERATION_NOT_ALLOWED.UNSUPPORTED_OPERATION" not in str(e) :
              print(f'WARN: Couldn t set table {catalog}.{db}.{t["tableName"]} owner to account users, error: {e}')
      except Exception as e:
        print("Couldn't grant access to the schema to all users:"+str(e))    

    print(f"using catalog.database `{catalog}`.`{db}`")
    spark.sql(f"""USE `{catalog}`.`{db}`""")    

    if volume_name:
      spark.sql(f'CREATE VOLUME IF NOT EXISTS {volume_name};')

                     
  #Return true if the folder is empty or does not exists
  @staticmethod
  def is_folder_empty(folder):
    try:
      return len(dbutils.fs.ls(folder)) == 0
    except:
      return True
    
  @staticmethod
  def is_any_folder_empty(folders):
    return any([DBDemos.is_folder_empty(f) for f in folders])

  @staticmethod
  def set_model_permission(model_name, permission, principal):
    import databricks.sdk.service.catalog as c
    sdk_client = databricks.sdk.WorkspaceClient()
    return sdk_client.grants.update(c.SecurableType.FUNCTION, model_name, changes=[
                              c.PermissionsChange(add=[c.Privilege[permission]], principal=principal)])

  @staticmethod
  def set_model_endpoint_permission(endpoint_name, permission, group_name):
    import databricks.sdk.service.serving as s
    sdk_client = databricks.sdk.WorkspaceClient()
    ep = sdk_client.serving_endpoints.get(endpoint_name)
    return sdk_client.serving_endpoints.set_permissions(serving_endpoint_id=ep.id, access_control_list=[s.ServingEndpointAccessControlRequest(permission_level=s.ServingEndpointPermissionLevel[permission], group_name=group_name)])

  @staticmethod
  def set_index_permission(index_name, permission, principal):
      import databricks.sdk.service.catalog as c
      sdk_client = databricks.sdk.WorkspaceClient()
      return sdk_client.grants.update(c.SecurableType.TABLE, index_name, changes=[
                              c.PermissionsChange(add=[c.Privilege[permission]], principal=principal)])
    

  @staticmethod
  def download_file_from_git(dest, owner, repo, path):
    def download_file(url, destination):
      local_filename = url.split('/')[-1]
      # NOTE the stream=True parameter below
      with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
          for chunk in r.iter_content(chunk_size=8192): 
            # If you have chunk encoded response uncomment if
            # and set chunk_size parameter to None.
            #if chunk: 
            f.write(chunk)
      return local_filename

    if not os.path.exists(dest):
      os.makedirs(dest)
    from concurrent.futures import ThreadPoolExecutor
    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents{path}').json()
    files = [f['download_url'] for f in files if 'NOTICE' not in f['name']]
    def download_to_dest(url):
      try:
        #Temporary fix to avoid hitting github limits - Swap github to our S3 bucket to download files
        s3url = url.replace("https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/", "https://dbdemos-dataset.s3.amazonaws.com/")
        download_file(s3url, dest)
      except:
        download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
      collections.deque(executor.map(download_to_dest, files))
         

  #force the experiment to the field demos one. Required to launch as a batch
  @staticmethod
  def init_experiment_for_batch(demo_name, experiment_name):
    import mlflow
    #You can programmatically get a PAT token with the following
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    xp_root_path = f"/Shared/dbdemos/experiments/{demo_name}"
    try:
      r = w.workspace.mkdirs(path=xp_root_path)
    except Exception as e:
      print(f"ERROR: couldn't create a folder for the experiment under {xp_root_path} - please create the folder manually or  skip this init (used for job only: {e})")
      raise e
    xp = f"{xp_root_path}/{experiment_name}"
    print(f"Using common experiment under {xp}")
    mlflow.set_experiment(xp)
    DBDemos.set_experiment_permission(xp)
    return mlflow.get_experiment_by_name(xp)

  @staticmethod
  def set_experiment_permission(experiment_path):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import iam
    w = WorkspaceClient()
    try:
      status = w.workspace.get_status(experiment_path)
      w.permissions.set("experiments", request_object_id=status.object_id,  access_control_list=[
                            iam.AccessControlRequest(group_name="users", permission_level=iam.PermissionLevel.CAN_MANAGE)])    
    except Exception as e:
      print(f"error setting up shared experiment {experiment_path} permission: {e}")

    print(f"Experiment on {experiment_path} was set public")


  @staticmethod
  def get_active_streams(start_with = ""):
    return [s for s in spark.streams.active if len(start_with) == 0 or (s.name is not None and s.name.startswith(start_with))]

  @staticmethod
  def stop_all_streams_asynch(start_with = "", sleep_time=0):
    import threading
    def stop_streams():
        DBDemos.stop_all_streams(start_with=start_with, sleep_time=sleep_time)

    thread = threading.Thread(target=stop_streams)
    thread.start()

  @staticmethod
  def stop_all_streams(start_with = "", sleep_time=0):
    import time
    time.sleep(sleep_time)
    streams = DBDemos.get_active_streams(start_with)
    if len(streams) > 0:
      print(f"Stopping {len(streams)} streams")
      for s in streams:
          try:
              s.stop()
          except:
              pass
      print(f"All stream stopped {'' if len(start_with) == 0 else f'(starting with: {start_with}.)'}")

  @staticmethod
  def wait_for_all_stream(start = ""):
    import time
    actives = DBDemos.get_active_streams(start)
    if len(actives) > 0:
      print(f"{len(actives)} streams still active, waiting... ({[s.name for s in actives]})")
    while len(actives) > 0:
      spark.streams.awaitAnyTermination()
      time.sleep(1)
      actives = DBDemos.get_active_streams(start)
    print("All streams completed.")

  @staticmethod
  def get_last_experiment(demo_name, experiment_path = "/Shared/dbdemos/experiments/"):
    import requests
    import re
    from datetime import datetime
    #TODO: waiting for https://github.com/databricks/databricks-sdk-py/issues/509 to use the python sdk instead
    base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.get(base_url+"/api/2.0/workspace/list", params={'path': f"{experiment_path}/{demo_name}"}, headers=headers).json()
    if 'objects' not in r:
      raise Exception(f"No experiment available for this demo. Please re-run the previous notebook with the AutoML run. - {r}")
    xps = [f for f in r['objects'] if f['object_type'] == 'MLFLOW_EXPERIMENT' and 'automl' in f['path']]
    xps = [x for x in xps if re.search(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2})', x['path'])]
    sorted_xp = sorted(xps, key=lambda f: f['path'], reverse = True)
    if len(sorted_xp) == 0:
      raise Exception(f"No experiment available for this demo. Please re-run the previous notebook with the AutoML run. - {r}")

    last_xp = sorted_xp[0]

    # Search for the date pattern in the input string
    match = re.search(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2})', last_xp['path'])

    if match:
        date_str = match.group(1)  # Extract the matched date string
        date = datetime.strptime(date_str, '%Y-%m-%d_%H:%M:%S')  # Convert to a datetime object
        # Calculate the difference in days from the current date
        days_difference = (datetime.now() - date).days
        if days_difference > 30:
            raise Exception(f"It looks like the last experiment {last_xp} is too old ({days_difference} days). Please re-run the previous notebook to make sure you have the latest version. Delete the experiment folder if needed to clear history.")
    else:
        raise Exception(f"Invalid experiment format or no experiment available. Please re-run the previous notebook. {last_xp['path']}")
    return last_xp
  

  @staticmethod
  def wait_for_table(table_name, timeout_duration=120):
    import time
    i = 0
    while not spark.catalog.tableExists(table_name) or spark.table(table_name).count() == 0:
      time.sleep(1)
      if i > timeout_duration:
        raise Exception(f"couldn't find table {table_name} or table is empty. Do you have data being generated to be consumed?")
      i += 1

  @staticmethod
  def get_python_version_mlflow():
    import sys
    # Determine target version
    major, minor, micro = sys.version_info[:3]

    if major == 3 and minor == 11 and micro > 10:
        return "3.11.10"
    elif major == 3 and minor == 12 and micro > 3:
        return "3.12.3"
    else:
        return f"{major}.{minor}.{micro}"

  # Workaround for dbdemos to support automl the time being, creates a mock run simulating automl results
  @staticmethod
  def create_mockup_automl_run(full_xp_path, df, model_name=None, target_col=None):
    import mlflow
    import os
    print("AutoML doesn't seem to be available, creating a mockup automl run instead - automl serverless will be added soon...")
    # Spark Connect's toPandas() populates df.attrs with non-JSON-serializable
    # PlanMetrics protos, which crashes pandas to_parquet below.
    df.attrs = {}
    from databricks.sdk import WorkspaceClient
    # Initialize the WorkspaceClient
    w = WorkspaceClient()
    w.workspace.mkdirs(path=os.path.dirname(full_xp_path))
    xp = mlflow.create_experiment(full_xp_path)
    mlflow.set_experiment(experiment_id=xp)
    with mlflow.start_run(run_name="DBDemos automl mock autoML run", experiment_id=xp) as run:
        mlflow.set_tag('mlflow.source.name', 'Notebook: DataExploration')
        mlflow.log_metric('val_f1_score', 0.81)
        
        split_choices = ['train', 'val', 'test']
        split_probabilities = [0.7, 0.2, 0.1]  # 70% train, 20% val, 10% test
        # Add a new column with random assignments
        import numpy as np
        df['_automl_split_col'] = np.random.choice(split_choices, size=len(df), p=split_probabilities)
        import uuid
        import os
        random_path = f"/tmp/{uuid.uuid4().hex}/dataset.parquet"
        os.makedirs(os.path.dirname(random_path), exist_ok=True)
        df.to_parquet(random_path, index=False)
        mlflow.log_artifact(random_path, artifact_path='data/training_data')
        model = None
        if model_name is not None and target_col is not None:
          from sklearn.ensemble import RandomForestClassifier
          from sklearn.metrics import f1_score
          import pandas as pd

          class SafeRandomForestClassifier(RandomForestClassifier):
            def _sanitize(self, X):
                if isinstance(X, pd.DataFrame):
                    # Drop datetime columns
                    X = X.drop(columns=X.select_dtypes(include=['datetime']).columns)

                    # Encode object/string columns as categorical codes
                    for col in X.select_dtypes(include=['object', 'category']).columns:
                        X[col] = X[col].astype('category').cat.codes

                    # Fill missing values
                    X = X.fillna(0)
                return X

            def fit(self, X, y=None, sample_weight=None):
                X = self._sanitize(X)
                return super().fit(X, y, sample_weight)

            def predict(self, X):
                X = self._sanitize(X)
                return super().predict(X)
                
          # Split the data based on _automl_split_col
          train_df = df[df['_automl_split_col'] == 'train']
          val_df = df[df['_automl_split_col'] == 'val']
          
          # Prepare training and validation datasets
          X_train = train_df.drop(columns=['_automl_split_col', target_col], errors='ignore')
          y_train = train_df[target_col]
          X_val = val_df.drop(columns=['_automl_split_col', target_col], errors='ignore')
          y_val = val_df[target_col]
          
          # Train RandomForest model
          model = SafeRandomForestClassifier(random_state=42)
          model.fit(X_train, y_train)
          y_pred = model.predict(X_val)
          val_f1 = f1_score(y_val, y_pred, average='weighted')
          
          # Log model and metric to MLflow
          mlflow.log_metric('val_f1_score', val_f1)
          # Force cloudpickle: newer mlflow / serverless runtimes default sklearn logging to
          # the skops format, which rejects the notebook-defined SafeRandomForestClassifier as
          # an "untrusted type". cloudpickle serializes the custom class without that check.
          mlflow.sklearn.log_model(model, artifact_path="model", input_example=X_train.iloc[[0]], serialization_format="cloudpickle")

        class BestTrialMock:
            def __init__(self, mlflow_run_id, model):
                self.mlflow_run_id = mlflow_run_id
                self.model = model
                run_data = mlflow.get_run(mlflow_run_id).data
                self.metrics = run_data.metrics
                self.params = run_data.params
            def load_model(self):
                return self.model
        
        class XPMock:
            def __init__(self, experiment_id):
                self.experiment_id = experiment_id
        
        class AutoMLRun:
            def __init__(self, best_trial_mlflow_run_id, model, xp):
                self.best_trial = BestTrialMock(best_trial_mlflow_run_id, model)
                self.experiment = XPMock(xp)
        
        return AutoMLRun(run.info.run_id, model, xp)

# COMMAND ----------

#Let's skip some warnings for cleaner output
import warnings
warnings.filterwarnings("ignore")
