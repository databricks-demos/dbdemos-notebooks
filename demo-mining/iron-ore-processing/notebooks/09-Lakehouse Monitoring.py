# Databricks notebook source
# MAGIC %md
# MAGIC # 7. Lakehouse Monitoring
# MAGIC
# MAGIC This section showcases Databricks Lakehouse Monitoring, which provides unified visibility across data pipelines, models, and infrastructure—helping teams quickly detect issues, ensure data quality, and maintain reliable AI and analytics workflows.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](../demo_setup/images/DataQuality.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 🛡️ Data Quality and Anomaly Detection
# MAGIC We can leverage automated Data Quality and Anomaly detection to periodically scan our tables and generate alerts and incidents as Data Quality changes over time. It can monitor for freshness and values out of range etc.
# MAGIC
# MAGIC [Individual Table Quality Monitoring](https://e2-demo-field-eng.cloud.databricks.com/explore/data/mining_iron_ore_processing_demo_catalog/iop_schema/fe_model_endpoint_payload?o=1444828305810485&activeTab=quality)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Anomaly Detection Dashboard
# MAGIC
# MAGIC [Data Quality Dashboard](https://e2-demo-field-eng.cloud.databricks.com/dashboardsv3/01f0433235a31c38bd1e5b1be0d92a51/published/pages/quality-overview?o=1444828305810485&f_quality-overview%7Equality-overview-logging-table-name=mining_iron_ore_processing_demo_catalog.iop_schema._quality_monitoring_summary&f_table-quality-details%7Etable-quality-details-logging-table-name=mining_iron_ore_processing_demo_catalog.iop_schema._quality_monitoring_summary)
# MAGIC
# MAGIC ![](https://docs.databricks.com/aws/en/assets/images/anomaly-detection-dashboard-quality-overview-schema-summary-d8ef475ba3191b575cf43d1583491fc8.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Data Profiling (aka Lakehouse Monitoring)
# MAGIC
# MAGIC Lakehouse monitoring allows you to automatically profile data in a table or monitor the drift in inference of a model. Lakehouse monitoring compares the data or inferences over time and detects how the it is changing. All of the statistics are recorded in a metrics and profiling tables and comes with a built in dashboard to show this
# MAGIC
# MAGIC example
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/sql/dashboardsv3/01f04b92317816e6b3b79d59009cba33?o=1444828305810485
# MAGIC
# MAGIC # ![](../demo_setup/images/LakehouseMonitoring.png)