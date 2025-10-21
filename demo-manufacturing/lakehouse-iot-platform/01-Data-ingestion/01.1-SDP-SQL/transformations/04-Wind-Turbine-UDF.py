# ----------------------------------------
# Registering python UDF to a SQL function
# ----------------------------------------

# This is a companion notebook to load the wind turbine prediction model as a spark udf and save it as a SQL function
# Make sure you add this fine in your Spark Declarative Pipelines job to have access to the `get_turbine_status` SQL function.

import mlflow

mlflow.set_registry_uri('databricks-uc')     
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/main_build.dbdemos_iot_platform.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)