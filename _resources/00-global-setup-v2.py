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


class DBDemos():
  @staticmethod
  def setup_schema(catalog, db, reset_all_data, volume_name = None):
    if reset_all_data:
      print(f'clearing up volume named `{catalog}`.`{db}`.`{volume_name}`')
      spark.sql(f"DROP VOLUME IF EXISTS `{catalog}`.`{db}`.`{volume_name}`")
      spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{db}` CASCADE")

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
        s3url = url.replace("https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/", "https://notebooks.databricks.com/demos/dbdemos-dataset/")
        download_file(s3url, dest)
      except:
        download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
      collections.deque(executor.map(download_to_dest, files))
         

  #force the experiment to the field demos one. Required to launch as a batch
  @staticmethod
  def init_experiment_for_batch(demo_name, experiment_name):
    import mlflow
    #You can programatically get a PAT token with the following
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
            raise Exception(f"It looks like the last experiment {last_xp} is too old ({days} days). Please re-run the previous notebook to make sure you have the latest version.")
    else:
        raise Exception(f"Invalid experiment format or no experiment available. Please re-run the previous notebook. {last_xp['path']}")
    return last_xp

# COMMAND ----------

#Let's skip some warnings for cleaner output
import warnings
warnings.filterwarnings("ignore")
