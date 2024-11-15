# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-04-system-tables",
  "category": "governance",
  "title": "System Tables: Billing Forecast, Usage and Audit",
  "custom_schema_supported": True,
  "default_schema": "dbdemos_billing_forecast",
  "default_catalog": "main",
  "custom_message": "System tables need the be enabled first on UC for this demo to work. See <a href=\"https://notebooks.databricks.com/demos/uc-04-system-tables/index.html#\">_enable_system_tables notebook</a> for more details (also installed as part of the demo).",
  "description": "Track and analysis usage, billing & access with UC System tables.",
  "fullDescription": "Databricks Unity Catalog is the world's first AI-powered governance solution for the lakehouse. It empowers enterprises to seamlessly govern their structured and unstructured data, ML models, notebooks, dashboards, and files on any cloud or platform. <br/>Through Delta Sharing, Databricks Unity Catalog offers direct access to many of the lakehouse activity logs exposed in Delta as System Tables. System Tables are the cornerstone of lakehouse observability and enable at-scale operational intelligence on numerous key business questions. <br/>In this demo, we'll show how Unity Catalog System Tables can be used to: <ul><li>Monitor your consumption and leverage the lakehouse AI capabilities to forecast your future usage, triggering alerts when billing goes above your criterias</li><li>Monitor accesses to your data assets</li><li>Monitor and understand your platform usage</li></ul>",
    "usecase": "Data Governance",
  "products": ["Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Unity Catalog: Fine grained Governance", "url": "https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html"}],
  "recommended_items": ["uc-01-acl", "uc-02-external-location", "uc-03-data-lineage"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"uc": "Unity Catalog"}],
  "notebooks": [
     {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "00-intro-system-tables", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Introduction to system tables", 
      "description": "Start here to review the main system tables available."
    },
    {
      "path": "01-billing-tables/01-billing-tables-overview", 
      "pre_run": False,
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Billing table overview", 
      "description": "Explore the billing table to track your usage."
    },
    {
      "path": "01-billing-tables/02-forecast-billing-tables", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Forecast your consumption", 
      "description": "Leverage the Lakehouse AI capabilities to forecast your usage"
    },
    {
      "path": "02-audit-logs-tables/02-audit-log", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Audit Log system table", 
      "description": "Track lakehouse operation, including data access"
    },
    {
      "path": "03-lineage-tables/03-lineage", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Data lineage", 
      "description": "Track data lineage on all assets (tables, workflow, dashboards, AI...)"
    },
    {
      "path": "_enable_system_tables", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Enable system tables", 
      "description": "Helper to turn on the system tables - read before you run."
    }
  ],
  "init_job": {
    "settings": {
        "name": "dbdemos_billing_forecast_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/01-billing-tables/02-forecast-billing-tables",
                    "source": "WORKSPACE",
                    "base_parameters": {"catalog": "main", "schema": "billing_forecast"}
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}            
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "spark_version": "13.1.x-cpu-ml-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
    }
  },
  "cluster": {
    "num_workers": 4,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER",
    "spark_version": "15.4.x-scala2.12"
  },
  "dashboards": [{"name": "[dbdemos] System Tables - account usage overview",       "id": "account-usage"},
                 {"name": "[dbdemos] System Tables - Cost forecast from ML models", "id": "cost-forecasting"},
                 {"name": "[dbdemos] System Tables - Databricks Model Serving Endpoint Cost Attribution", "id": "model-serving-cost"},
                 {"name": "[dbdemos] System Tables - Warehouse Monitoring and Cost Attribution", "id": "warehouse-serverless-cost"},
                 {"name": "[dbdemos] System Tables - Workflow and Job Cost Attribution", "id": "worklow-analysis"}
                 ],
  "genie_rooms":[
    {
        "id": "system-tables",
        "display_name": "Databricks System Table Genie Space",
        "description": "Ask questions on your Databricks Usage leveraging built-in system tables",
        "table_identifiers": [
            "system.billing.usage",
            "system.access.audit",
            "system.billing.list_prices",
            "system.compute.clusters",
            "system.compute.warehouse_events",
            "system.compute.node_types",
            "system.lakeflow.job_run_timeline",
            "system.lakeflow.job_task_run_timeline",
            "system.lakeflow.job_tasks",
            "system.lakeflow.jobs"
        ],
        "sql_instructions": [
            {
                "title": "What are all of the available SKUs, the current list price for each, and when did the price take effect?",
                "content": "select\n  sku_name,\n  pricing.default,\n  price_start_time,\n  usage_unit\nfrom\n  system.billing.list_prices\nwhere\n  price_end_time is null -- current price for a sku\norder by\n  sku_name;"
            },
            {
                "title": "What are all of the photon SKUs and their list prices over time?",
                "content": "select\n  sku_name,\n  pricing.default,\n  price_start_time,\n  price_end_time,\n  usage_unit\nfrom\n  system.billing.list_prices\nwhere\n  upper(sku_name) like '%PHOTON%'\norder by\n  sku_name,\n  price_start_time;"
            },
            {
                "title": "What SKUs have ever had a price change?",
                "content": "with skus as (\n  select\n    distinct sku_name -- distinct because it might have had multiple price changes\n  from\n    system.billing.list_prices\n  where\n    price_end_time is not null -- sku has had at least one price change\n)\nselect\n  lp.sku_name,\n  pricing.default,\n  price_start_time,\n  price_end_time,\n  usage_unit\nfrom\n  system.billing.list_prices lp\n  inner join skus s on s.sku_name = lp.sku_name\norder by\n  sku_name,\n  price_start_time;"
            },
            {
                "title": "What are the total DBUs used by date and by SKUs for DBSQL for the account (all workspaces)?",
                "content": "select\n  u.usage_date,\n  u.sku_name,\n  sum(u.usage_quantity) as total_dbus\nfrom\n  system.billing.usage u\nwhere\n  upper(u.sku_name) like '%SQL%'\ngroup by\n  all\norder by\n  total_dbus desc"
            },
            {
                "title": "What is the DBU spend for each workspace by month for DBSQL Serverless?",
                "content": "select\n  u.workspace_id,\n  date_trunc('month', u.usage_date) as usage_month,\n  sum(u.usage_quantity) as total_dbus\nfrom\n  system.billing.usage u\nwhere\n  upper(u.sku_name) like '%SERVERLESS%'\n  and upper(u.billing_origin_product) like '%SQL%'\ngroup by\n  all\norder by\n  1,\n  2"
            },
            {
                "title": "What are the total DBUs and total dollars (list price) used by date and by SKU for DBSQL for the account (all workspaces)?",
                "content": "select\n  u.usage_date,\n  u.sku_name,\n  sum(u.usage_quantity) as total_dbus,\n  sum(lp.pricing.default * u.usage_quantity) as list_cost\nfrom\n  system.billing.usage u\n  inner join system.billing.list_prices lp on u.cloud = lp.cloud\n  and u.sku_name = lp.sku_name\n  and u.usage_start_time >= lp.price_start_time\n  and (\n    u.usage_end_time <= lp.price_end_time\n    or lp.price_end_time is null\n  )\nwhere\n  upper(u.billing_origin_product) like '%SQL%'\ngroup by\n  all\norder by\n  total_dbus desc"
            },
            {
                "title": "Group DBUs and total dollars (list price) spent into origin products by date",
                "content": "SELECT\n  date_trunc ('month', usage_date) as usage_month,\n  billing_origin_product,\n  SUM(usage_quantity) AS total_dbus\nFROM\n  system.billing.usage\nGROUP BY\n  1,\n  2\nORDER BY\n  1,\n  2"
            },
            {
                "title": "Are there any node types that have more than one GPU?",
                "content": "select\n  node_type,\n  memory_mb,\n  core_count,\n  gpu_count\nfrom\n  system.compute.node_types\nwhere\n  gpu_count >= 2\norder by\n  gpu_count desc,\n  core_count desc"
            },
            {
                "title": "What node type has both the fewest cores and the least amount of memory?",
                "content": "with min_count as (\n  select\n    min(core_count) as min_cores,\n    min(memory_mb) as min_mb\n  from\n    system.compute.node_types\n)\nselect\n  node_type,\n  m.*\nfrom\n  system.compute.node_types n\n  inner join min_count m on n.memory_mb = m.min_mb\n  and n.core_count = m.min_cores"
            },
            {
                "title": "What are the current value records for all of the clusters in the region?",
                "content": "SELECT\n  *,\n  ROW_NUMBER() OVER (\n    PARTITION BY cluster_id\n    ORDER BY\n      change_time DESC\n  ) AS row_num\nFROM\n  system.compute.clusters QUALIFY row_num = 1"
            },
            {
                "title": "What clusters currently have autoscaling turned on with the max workers set to a value higher than 50?",
                "content": "with clusters_current as (\n  SELECT\n    *,\n    ROW_NUMBER() OVER (\n      PARTITION BY cluster_id\n      ORDER BY\n        change_time DESC\n    ) AS row_num\n  FROM\n    system.compute.clusters QUALIFY row_num = 1\n)\nselect\n  *\nfrom\n  clusters_current\nwhere\n  max_autoscale_workers >= 50\n  and delete_time is null"
            },
            {
                "title": "What clusters currently don\u2019t have auto-termination enabled or their auto-termination timeout longer than two hours?",
                "content": "with clusters_current as (\n  SELECT\n    *,\n    ROW_NUMBER() OVER (\n      PARTITION BY cluster_id\n      ORDER BY\n        change_time DESC\n    ) AS row_num\n  FROM\n    system.compute.clusters QUALIFY row_num = 1\n)\nSELECT\n  *\nFROM\n  clusters_current\nwhere\n  (\n    auto_termination_minutes is null\n    or auto_termination_minutes > 120\n  )\n  and cluster_name not like 'job-%' -- don't want job clusters\n  and delete_time is null"
            },
            {
                "title": "Give a full history of changes made to cluster currently name \u2018xyz\u2019",
                "content": "select c.*\nfrom system.compute.clusters c\ninner join system_reporting.compute.clusters_current curr using(cluster_id)\nwhere curr.cluster_name = 'xyz'\norder by c.change_date desc, c.change_time desc"
            },
            {
                "title": "what are the current names of all warehouses?",
                "content": "with data as ( -- get all of the successful creates and edits of warehouses and endpoints\nselect event_time, request_params.name as warehouse_name, from_json(response ['result'], 'Map<STRING, STRING>') [\"id\"] as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('createWarehouse', 'createEndpoint')\nand response.status_code = '200'\nunion\nselect event_time, request_params.name as warehouse_name, request_params.id as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('editWarehouse', 'editEndpoint')\nand response.status_code = '200'\n),\ncurrent_data as ( -- get the most recent create or edit of each warehouse or endpoint\n    select *,\n  ROW_NUMBER() OVER (\n    PARTITION BY warehouse_id\n    ORDER BY\n      event_time DESC\n  ) AS row_num\n    from data\n    qualify row_num = 1\n)\nselect * from current_data"
            },
            {
                "title": "Usage by cluster/warehouse name",
                "content": "with clusters_current as (\n  SELECT\n    *,\n    ROW_NUMBER() OVER (\n      PARTITION BY cluster_id\n      ORDER BY\n        change_time DESC\n    ) AS row_num\n  FROM\n    system.compute.clusters QUALIFY row_num = 1\n),\nwarehouse_data as ( -- get all of the successful creates and edits of warehouses and endpoints\nselect event_time, request_params.name as warehouse_name, from_json(response ['result'], 'Map<STRING, STRING>') [\"id\"] as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('createWarehouse', 'createEndpoint')\nand response.status_code = '200'\nunion\nselect event_time, request_params.name as warehouse_name, request_params.id as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('editWarehouse', 'editEndpoint')\nand response.status_code = '200'\n),\nwarehouses as ( -- get the most recent create or edit of each warehouse or endpoint\n    select *,\n  ROW_NUMBER() OVER (\n    PARTITION BY warehouse_id\n    ORDER BY\n      event_time DESC\n  ) AS row_num\n    from warehouse_data\n    qualify row_num = 1\n)\nselect\n  'JOBS' as work_type,\n  u.usage_date,\n  cc.cluster_id as cluster_or_warehouse_id,\n  cc.cluster_name as cluster_or_warehouse_name,\n  sum(u.usage_quantity) as total_dbus,\n  sum(lp.pricing.default * u.usage_quantity) as list_cost\nfrom\n  system.billing.usage u\n  inner join system.billing.list_prices lp on u.cloud = lp.cloud\n  and u.sku_name = lp.sku_name\n  and u.usage_start_time >= lp.price_start_time\n  and (\n    u.usage_end_time <= lp.price_end_time\n    or lp.price_end_time is null\n  )\n  inner join clusters_current cc \n        on u.usage_metadata.cluster_id = cc.cluster_id\nwhere\n  usage_metadata.job_id is not Null\ngroup by\n  all\nunion all\nselect\n  'ALL PURPOSE' as work_type,\n  u.usage_date,\n  cc.cluster_id as cluster_or_warehouse_id,\n  cc.cluster_name as cluster_or_warehouse_name,  \n  sum(u.usage_quantity) as total_dbus,\n  sum(lp.pricing.default * u.usage_quantity) as list_cost\nfrom\n  system.billing.usage u\n  inner join system.billing.list_prices lp on u.cloud = lp.cloud\n  and u.sku_name = lp.sku_name\n  and u.usage_start_time >= lp.price_start_time\n  and (\n    u.usage_end_time <= lp.price_end_time\n    or lp.price_end_time is null\n  )\n  inner join clusters_current cc \n        on u.usage_metadata.cluster_id = cc.cluster_id\nwhere\n  usage_metadata.job_id is Null\n  and usage_metadata.cluster_id is not null\ngroup by\n  all\nunion all\nselect\n  'SQL' as work_type,\n  u.usage_date,\n  w.warehouse_id as cluster_or_warehouse_id,\n  w.warehouse_name as cluster_or_warehouse_name,  \n  sum(u.usage_quantity) as total_dbus,\n  sum(lp.pricing.default * u.usage_quantity) as list_cost\nfrom\n  system.billing.usage u\n  inner join system.billing.list_prices lp on u.cloud = lp.cloud\n  and u.sku_name = lp.sku_name\n  and u.usage_start_time >= lp.price_start_time\n  and (\n    u.usage_end_time <= lp.price_end_time\n    or lp.price_end_time is null\n  )\n  inner join warehouses w \n        on u.usage_metadata.warehouse_id = w.warehouse_id\nwhere\n  usage_metadata.job_id is Null\n  and usage_metadata.cluster_id is null\n  and usage_metadata.warehouse_id is not null\ngroup by\n  all\n"
            },
            {
                "title": "what are the current instance pool settings?",
                "content": "with data as (\nselect event_time, request_params.instance_pool_name as instance_pool_name, from_json(response ['result'], 'Map<STRING, STRING>') [\"instance_pool_id\"] as instance_pool_id, request_params.max_capacity as max_capacity\nfrom system.access.audit\nwhere service_name = 'instancePools'\nand action_name = 'create'\nunion\nselect event_time, request_params.instance_pool_name as instance_pool_name, request_params.instance_pool_id as instance_pool_id, request_params.max_capacity as max_capacity\nfrom system.access.audit\nwhere service_name = 'instancePools'\nand action_name = 'edit'\n),\ncurrent_data as (\n    select *,\n  ROW_NUMBER() OVER (\n    PARTITION BY instance_pool_id\n    ORDER BY\n      event_time DESC\n  ) AS row_num\n    from data\n    qualify row_num = 1\n)\nselect * except(event_time,row_num) from current_data"
            },
            {
                "title": "what was the result of jobs, how much did they cost and how many dbus did the use?",
                "content": "with data as (\n  SELECT\n    u.workspace_id,\n    u.usage_metadata.job_id as job_id,\n    usage_metadata.job_run_id as run_id,\n    sum(lp.pricing.default * u.usage_quantity) as list_cost,\n    sum(u.usage_quantity) as dbus\n  FROM\n    system.billing.usage u\n    inner join system.billing.list_prices lp on u.cloud = lp.cloud\n    and u.sku_name = lp.sku_name\n    and u.usage_start_time >= lp.price_start_time\n    and (\n      u.usage_end_time <= lp.price_end_time\n      or lp.price_end_time is null\n    )\n    where u.sku_name like '%JOB%'\n    and usage_metadata.job_id is not null\n    group by all\n),\njob_runs as (\n    select date_trunc('DAY', period_end_time) as run_date, job_id, run_id, result_state \n    from system.lakeflow.job_run_timeline \n    where result_state is not null\n),\nrun_times as (\n    select job_id, run_id, min(period_start_time) as start_time, max(period_end_time) as end_time\n    from system.lakeflow.job_run_timeline\n    group by all\n),\njob_runs_with_times as (\n    select * \n    from job_runs\n    inner join run_times using (job_id, run_id)\n)\nselect  \n    workspace_id, \n    job_id,\n    run_id,\n    result_state,\n    start_time,\n    end_time,\n    list_cost,\n    dbus\nfrom data\ninner join job_runs_with_times using (job_id,run_id)"
            },
            {
                "title": "Get the current job names by job_id",
                "content": "\nSELECT\n  *,\n  ROW_NUMBER() OVER (\n    PARTITION BY job_id\n    ORDER BY\n      change_time DESC\n  ) AS row_num\nFROM\n  system.lakeflow.jobs QUALIFY row_num = 1"
            },
            {
                "title": "what are the valid job result states?",
                "content": "select distinct result_state\nfrom system.lakeflow.job_run_timeline\nwhere result_state is not null"
            },
            {
                "title": "what is the current name of a job?",
                "content": "with raw_data as (\nselect \njob_id,\nname,\ndelete_time,\nrow_number() over (partition by job_id order by change_time desc) as row\nfrom system.lakeflow.jobs\nqualify row = 1\n)\nselect job_id, name\nfrom raw_data\nwhere delete_time is null"
            },
            {
                "title": "get user names and user ids",
                "content": "with data as (\nselect \nrequest_params.targetUserId as user_id,\nrequest_params.targetUserName as user_name,\nevent_time as change_time\nfrom system.access.audit\nwhere request_params.targetUserId is not null\n)\nselect user_id, \nuser_name,\nrow_number() over (partition by user_id order by change_time desc) as row\nfrom data\nqualify row = 1"
            },
            {
                "title": "What is monthly spend and usage by billing origina product separating serverless from non-serverless? Concatenate the billing origin product with SERVERLESS/NON-SERVERLESS from the sku name.",
                "content": "SELECT\n  DATE_TRUNC('month', u.usage_date) AS month,\n  CONCAT(\n    u.billing_origin_product,\n    ' ',\n    CASE\n      WHEN UPPER(u.sku_name) LIKE '%SERVERLESS%' THEN 'SERVERLESS'\n      ELSE 'NON-SERVERLESS'\n    END\n  ) AS work_type,\n  ROUND(SUM(u.usage_quantity), 2) AS total_dbus,\n  ROUND(SUM(lp.pricing.default * u.usage_quantity), 2) AS total_spend\nFROM\n  system.billing.usage u\n  INNER JOIN system.billing.list_prices lp ON u.cloud = lp.cloud\n  AND u.sku_name = lp.sku_name\n  AND u.usage_start_time >= lp.price_start_time\n  AND (\n    u.usage_end_time <= lp.price_end_time\n    OR lp.price_end_time IS NULL\n  )\nGROUP BY\n  1,\n  2\nORDER BY\n  1,\n  2"
            },
            {
                "title": "What was the maximum number of clusters for each of my warehouses this year? Show the warehouse id and warehouse name.",
                "content": "with warehouse_data as ( -- get all of the successful creates and edits of warehouses and endpoints\nselect event_time, request_params.name as warehouse_name, from_json(response ['result'], 'Map<STRING, STRING>') [\"id\"] as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('createWarehouse', 'createEndpoint')\nand response.status_code = '200'\nunion\nselect event_time, request_params.name as warehouse_name, request_params.id as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('editWarehouse', 'editEndpoint')\nand response.status_code = '200'\n),\nwarehouses as ( -- get the most recent create or edit of each warehouse or endpoint\n    select *,\n  ROW_NUMBER() OVER (\n    PARTITION BY warehouse_id\n    ORDER BY\n      event_time DESC\n  ) AS row_num\n    from warehouse_data\n    qualify row_num = 1\n)\nSELECT\n  w.warehouse_id,\n  w.warehouse_name,\n  MAX(we.cluster_count) AS max_clusters\nFROM\n  system.compute.warehouse_events we\n  INNER JOIN warehouses w ON we.warehouse_id = w.warehouse_id\nWHERE\n  YEAR(we.event_time) = 2024\nGROUP BY\n  w.warehouse_id,\n  w.warehouse_name\nORDER BY\n  max_clusters DESC"
            },
            {
                "title": "Calculate job run final (non null) result state percentages of total runs by day for August 2024",
                "content": "WITH daily_totals AS (\n  SELECT\n    DATE_TRUNC('day', period_end_time) AS run_day,\n    COUNT(*) AS total_runs\n  FROM\n    system.lakeflow.job_run_timeline\n  WHERE\n    period_end_time BETWEEN '2024-08-01'\n    AND '2024-08-31'\n    AND result_state IS NOT NULL\n  GROUP BY\n    DATE_TRUNC('day', period_end_time)\n),\nstate_counts AS (\n  SELECT\n    DATE_TRUNC('day', period_end_time) AS run_day,\n    result_state,\n    COUNT(*) AS state_count\n  FROM\n    system.lakeflow.job_run_timeline\n  WHERE\n    period_end_time BETWEEN '2024-08-01'\n    AND '2024-08-31'\n    AND result_state IS NOT NULL\n  GROUP BY\n    DATE_TRUNC('day', period_end_time),\n    result_state\n)\nSELECT\n  sc.run_day,\n  sc.result_state,\n  sc.state_count,\n  ROUND(\n    (sc.state_count :: FLOAT / dt.total_runs :: FLOAT) * 100,\n    2\n  ) AS percentage_of_total_runs\nFROM\n  state_counts sc\n  INNER JOIN daily_totals dt ON sc.run_day = dt.run_day\nORDER BY\n  sc.run_day,\n  sc.result_state"
            },
            {
                "title": "What are the current details for warehouses in the region?",
                "content": "with warehouses as ( -- get all of the successful creates and edits of warehouses and endpoints\nselect event_time, request_params.name as warehouse_name, from_json(response ['result'], 'Map<STRING, STRING>') [\"id\"] as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('createWarehouse', 'createEndpoint')\nand response.status_code = '200'\nunion\nselect event_time, request_params.name as warehouse_name, request_params.id as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('editWarehouse', 'editEndpoint')\nand response.status_code = '200'\n),\nwarehouses_current as ( -- get the most recent create or edit of each warehouse or endpoint\n    select *,\n  ROW_NUMBER() OVER (\n    PARTITION BY warehouse_id\n    ORDER BY\n      event_time DESC\n  ) AS row_num\n    from warehouses\n    qualify row_num = 1\n)\nselect * from warehouses_current"
            },
            {
                "title": "What are the top 15 most expensive sql warehouses, including warehouse name, workspace, and owner. order by DBU and cost descending?",
                "content": "with warehouse_data as ( -- get all of the successful creates and edits of warehouses and endpoints\nselect event_time, request_params.name as warehouse_name, from_json(response ['result'], 'Map<STRING, STRING>') [\"id\"] as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('createWarehouse', 'createEndpoint')\nand response.status_code = '200'\nunion\nselect event_time, request_params.name as warehouse_name, request_params.id as warehouse_id, user_identity.email as owner, request_params.warehouse_type as warehouse_type, request_params.cluster_size as cluster_size\nfrom system.access.audit\nwhere service_name = 'databrickssql'\nand action_name in ('editWarehouse', 'editEndpoint')\nand response.status_code = '200'\n),\nwarehouses as ( -- get the most recent create or edit of each warehouse or endpoint\n    select *,\n  ROW_NUMBER() OVER (\n    PARTITION BY warehouse_id\n    ORDER BY\n      event_time DESC\n  ) AS row_num\n    from warehouse_data\n    qualify row_num = 1\n)\nSELECT\n  w.warehouse_id,\n  w.warehouse_name,\n  u.workspace_id,\n  w.owner,\n  SUM(u.usage_quantity) AS total_dbus,\n  SUM(lp.pricing.default * u.usage_quantity) AS total_cost\nFROM\n  system.billing.usage u\n  INNER JOIN system.billing.list_prices lp ON u.cloud = lp.cloud\n  AND u.sku_name = lp.sku_name\n  AND u.usage_start_time >= lp.price_start_time\n  AND (\n    u.usage_end_time <= lp.price_end_time\n    OR lp.price_end_time IS NULL\n  )\n  INNER JOIN warehouses w ON u.usage_metadata.warehouse_id = w.warehouse_id\nWHERE\n  UPPER(u.billing_origin_product) LIKE '%SQL%'\nGROUP BY all\nORDER BY\n  total_dbus DESC,\n  total_cost DESC\nLIMIT\n  15"
            },
            {
                "title": "Please identify sources (source name, source id, sku) of DBU usage in the month of August without a Cost_Center or WBS tags order by most usage descending",
                "content": "WITH clusters_current AS (\n  SELECT\n    cluster_id,\n    cluster_name,\n    ROW_NUMBER() OVER (\n      PARTITION BY cluster_id\n      ORDER BY\n        change_time DESC\n    ) AS row_num\n  FROM\n    system.compute.clusters QUALIFY row_num = 1\n),\nwarehouse_data AS (\n  SELECT\n    event_time,\n    request_params.name AS warehouse_name,\n    from_json(response ['result'], 'Map<STRING, STRING>') ['id'] AS warehouse_id,\n    user_identity.email AS owner,\n    request_params.warehouse_type AS warehouse_type,\n    request_params.cluster_size AS cluster_size\n  FROM\n    system.access.audit\n  WHERE\n    service_name = 'databrickssql'\n    AND action_name IN ('createWarehouse', 'createEndpoint')\n    AND response.status_code = '200'\n  UNION\n  SELECT\n    event_time,\n    request_params.name AS warehouse_name,\n    request_params.id AS warehouse_id,\n    user_identity.email AS owner,\n    request_params.warehouse_type AS warehouse_type,\n    request_params.cluster_size AS cluster_size\n  FROM\n    system.access.audit\n  WHERE\n    service_name = 'databrickssql'\n    AND action_name IN ('editWarehouse', 'editEndpoint')\n    AND response.status_code = '200'\n),\nwarehouses_current AS (\n  SELECT\n    *,\n    ROW_NUMBER() OVER (\n      PARTITION BY warehouse_id\n      ORDER BY\n        event_time DESC\n    ) AS row_num\n  FROM\n    warehouse_data QUALIFY row_num = 1\n)\nSELECT\n  u.workspace_id,\n  u.sku_name,\n  CASE\n  WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL AND u.usage_metadata.cluster_id IS NULL then u.usage_metadata.dlt_pipeline_id\n  WHEN cc.cluster_id IS NOT NULL THEN cc.cluster_id\n  WHEN u.usage_metadata.cluster_id IS NOT NULL THEN u.usage_metadata.cluster_id\n  WHEN wc.warehouse_id IS NOT NULL THEN wc.warehouse_id\n  WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN u.usage_metadata.warehouse_id\n  WHEN u.usage_metadata.endpoint_id IS NOT NULL THEN u.usage_metadata.endpoint_id \n  WHEN contains(u.sku_name, 'JOBS_SERVERLESS') and u.usage_metadata.job_run_id IS NOT NULL THEN u.usage_metadata.job_id\n  WHEN contains(u.sku_name, 'ALL_PURPOSE_SERVERLESS') and u.usage_metadata.notebook_id IS NOT NULL THEN u.usage_metadata.notebook_id\n  END as source_id,\n  CASE\n  WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL then concat('dlt-execution-',u.usage_metadata.dlt_pipeline_id)\n  WHEN cc.cluster_id IS NOT NULL THEN cc.cluster_name\n  WHEN u.usage_metadata.cluster_id IS NOT NULL THEN u.usage_metadata.cluster_id\n  WHEN wc.warehouse_id IS NOT NULL THEN wc.warehouse_name\n  WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN u.usage_metadata.warehouse_id\n  WHEN u.usage_metadata.endpoint_id IS NOT NULL THEN u.usage_metadata.endpoint_name \n  WHEN contains(u.sku_name, 'JOBS_SERVERLESS') and u.usage_metadata.job_run_id IS NOT NULL THEN u.usage_metadata.job_run_id\n  WHEN contains(u.sku_name, 'ALL_PURPOSE_SERVERLESS') and u.usage_metadata.notebook_id IS NOT NULL THEN u.usage_metadata.notebook_path\n  END as source_name,\n  u.billing_origin_product,\n  sum(u.usage_quantity) as total_dbus,\n  sum(lp.pricing.default * u.usage_quantity) as list_cost  \nFROM\n  system.billing.usage u\n  LEFT JOIN clusters_current cc ON u.usage_metadata.cluster_id = cc.cluster_id\n  LEFT JOIN warehouses_current wc ON u.usage_metadata.warehouse_id = wc.warehouse_id\n  inner join system.billing.list_prices lp on u.cloud = lp.cloud\n  and u.sku_name = lp.sku_name\n  and u.usage_start_time >= lp.price_start_time\n  and (\n    u.usage_end_time <= lp.price_end_time\n    or lp.price_end_time is null\n  )  \nWHERE\n  AND u.usage_date >= '2024-08-01'\n  AND u.usage_date <= '2024-08-31'\n  AND u.custom_tags ['Cost_Center'] IS NULL\n  AND u.custom_tags ['WBS'] IS NULL\nGROUP BY all\nORDER BY\n  total_dbus DESC"
            },
            {
                "title": "What is the cost by endpoint for vector search?",
                "content": "select\n  u.usage_date,\n  u.usage_metadata.endpoint_name,\n  u.usage_metadata.endpoint_id,\n  sum(u.usage_quantity) as total_dbus,\n  sum(lp.pricing.default * u.usage_quantity) as list_cost\nfrom\n  system.billing.usage u\n  inner join system.billing.list_prices lp on u.cloud = lp.cloud\n  and u.sku_name = lp.sku_name\n  and u.usage_start_time >= lp.price_start_time\n  and (\n    u.usage_end_time <= lp.price_end_time\n    or lp.price_end_time is null\n  )\nwhere\n  upper(u.billing_origin_product) like '%VECTOR_SEARCH%'\ngroup by\n  all\norder by\n  total_dbus desc"
            },
            {
                "title": "How much did I spend on GPU Model Serving, CPU Model Serving, and Foundation Model last month?",
                "content": "WITH last_month_gpu_usage AS (\n  SELECT\n    u.usage_date,\n    u.product_features.serving_type, --MODEL is CPU Model Serving, GPU_MODEL is GPU Model Serving, and FOUNDATION_MODEL is Foundation Model Serving\n    u.usage_quantity,\n    lp.pricing.default AS unit_price\n  FROM\n    `system`.`billing`.`usage` u\n    INNER JOIN `system`.`billing`.`list_prices` lp ON u.cloud = lp.cloud\n    AND u.sku_name = lp.sku_name\n    AND u.usage_start_time >= lp.price_start_time\n    AND (\n      u.usage_end_time <= lp.price_end_time\n      OR lp.price_end_time IS NULL\n    )\n  WHERE\n    u.sku_name ILIKE '%REAL_TIME_INFERENCE%'\n    AND u.billing_origin_product = 'MODEL_SERVING'\n    AND u.product_features.serving_type IN ('MODEL', 'GPU_MODEL', 'FOUNDATION_MODEL')\n    AND u.usage_date BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)\n    AND LAST_DAY(CURRENT_DATE - INTERVAL '1' MONTH)\n)\nSELECT\n  serving_type,\n  SUM(usage_quantity * unit_price) AS total_spend\nFROM\n  last_month_gpu_usage\nGROUP BY serving_type"
            },
            {
                "title": "Can you give me cost by sku for the last 30 days? Round to the nearest dollar."
                "content:" "WITH daily_costs AS (\n  SELECT\n    u.sku_name,\n    SUM(lp.pricing.default * u.usage_quantity) AS total_cost\n  FROM\n    `system`.`billing`.`usage` u\n    INNER JOIN `system`.`billing`.`list_prices` lp ON u.cloud = lp.cloud\n    AND u.sku_name = lp.sku_name\n    AND u.usage_start_time >= lp.price_start_time\n    AND (\n      u.usage_end_time <= lp.price_end_time\n      OR lp.price_end_time IS NULL\n    )\n  WHERE\n    u.usage_date BETWEEN DATE_SUB(CURRENT_DATE, 30)\n    AND CURRENT_DATE\n  GROUP BY\n    u.sku_name\n)\nSELECT\n  sku_name,\n  ROUND(total_cost) AS total_cost\nFROM\n  daily_costs\nORDER BY\n  total_cost DESC"
            }
        ],
        "instructions": "If a customer asks for a forecast, leverage the SQL function `ai_forecast`.\nWhenever including tags in a query use the lower() function to match based on case insensitivity.\nAn all purpose cluster doesn't have 'job' in the name.\nA job cluster has 'job' in the name.\nA warehouse isn't in the clusters table.\nAny time a user asks for job information include the job id and job owner.\nowner_id is a user id.\nround numeric values to 2 places unless otherwise specified.\nany request for 'usage' should refer to dbus.\nWhen asked about cost or usage always use current details for clusters, that is there should be a qualify row = 1 in the query for clusters.\nWhen asked about cost or usage for warehouses always use current details for warehouses, that is there should be a qualify row = 1 in the query for warehouses.\nWhen asked about cost or usage for jobs always only get the current name of each job.\nWhen asked about job owner it is the same as the creator id in the job table.\nAn owner is a type of user.\nWhen querying system.access.audit for warehouse information, action_names createEndpoint,createWarehouse have to be in separate queries from action_names editEndpoint, editWarehouse.\nWarehouse_id is not a field in system.access.audit.\nAlways use common table expressions and do not use nested selects in queries.\nWhen providing a warehouse_id or cluster_id and the query includes the usage table use usage.usage_metadata.warehouse_id or usage.usage_metadata.cluster_id.\nWhen looking for tags include them in the where clause.\nsystem.compute.warehouse_events does not have a warehouse_name column.\nDo not use UNNEST function.\nIf asked for an endpoint name add a filter to only get rows that endpoint_name is not null.\nIf asked about model serving endpoint or model serving usage, use sku_name LIKE '%REAL_TIME_INFERENCE%' and billing_origin_product = 'MODEL_SERVING'; filter by product_features.serving_type only when specified GPU, CPU, or Foundation Model.",
        "curated_questions": [
            "What is monthly spend and DBUs by serverless and non-serverless work types by workspace?",
            "What is the most expensive job run this month?",
            "What are all the current names for all of the clusters and warehouses in the region, ignoring job clusters? Include the cluster owner's name.",
            "What clusters currently don't have auto-termination enabled or longer than 1 hour?",
            "How much did spend increase month over month in 2024 by origin product?",
            "Calculate job run final result state percentages of total runs by day for <month>",
            "What are the total DBUs used by date and by SKUs for SQL for the account?",
            "What is the DBU spend for each workspace by month for SQL?",
            "What are the total DBUs and total dollars (list price) used by date and by SKU for Serverless SQL for all workspaces?",
            "What are DBUs and dollars grouped by origin product by date?",
            "What node types have a GPU?",
            "What are all of the available SKUs, the current list price for each, and when did the price take effect?",
            "What are all of the photon SKUs and their list prices over time?",
            "Are there any node types that have more than one GPU?",
            "What node type has both the most cores and the most amount of memory?",
            "What are the current names for all of the clusters in the region?"
        ]
    }
  ]
}
