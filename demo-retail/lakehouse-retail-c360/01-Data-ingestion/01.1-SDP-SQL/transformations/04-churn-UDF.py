# ----------------------------------------
# Registering python UDF to a SQL function
# ----------------------------------------
# This notebook loads the predict_churn model from MLflow registry
# and registers it as a SQL function for use in the SDP pipeline.
#
# While this code could be embedded in the SQL notebook, it won't be executed
# by the SDP engine (since SQL notebooks only process SQL cells).
# Therefore, this companion Python notebook must be included in your SDP libraries.

import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                                                     Stage/version
#                                                                                   Model name               |
#                                                                                       |                    |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/main__build.dbdemos_retail_c360.dbdemos_customer_churn@prod", "long", env_manager='virtualenv')
spark.udf.register("predict_churn", predict_churn_udf)
