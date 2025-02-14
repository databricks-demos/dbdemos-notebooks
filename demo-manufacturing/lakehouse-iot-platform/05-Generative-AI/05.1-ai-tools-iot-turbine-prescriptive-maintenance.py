# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Generative AI with Databricks
# MAGIC
# MAGIC ## From Predictive to Prescriptive Maintenance
# MAGIC Manufacturers face labor shortages, supply chain disruptions, and rising costs, making efficient maintenance essential. Despite investments in maintenance programs, many struggle to boost asset productivity due to technician shortages and poor knowledge-sharing systems. This leads to knowledge loss and operational inefficiencies.
# MAGIC
# MAGIC <div style="font-family: 'DM Sans';">
# MAGIC   <div style="width: 400px; color: #1b3139; margin-left: 50px; margin-right: 50px; float: left;">
# MAGIC     <div style="color: #ff5f46; font-size:50px;">73%</div>
# MAGIC     <div style="font-size:25px; margin-top: -20px; line-height: 30px;">
# MAGIC       of manufacturers struggle to recruit maintenance technicians — McKinsey (2023)
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:50px;">55%</div>
# MAGIC     <div style="font-size:25px; margin-top: -20px; line-height: 30px;">
# MAGIC       of manufacturers lack formal knowledge-sharing systems — McKinsey (2023)
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC Generative AI can transform maintenance by reducing downtime and improving productivity. While predictive maintenance anticipates failures, Generative AI enables prescriptive maintenance. Using historical data, AI systems can identify issues, generate solutions, and assist technicians, allowing junior staff to perform effectively and freeing experts for complex tasks.
# MAGIC <br><br>
# MAGIC
# MAGIC ### From Models to Agent Systems
# MAGIC Generative AI is moving from standalone models to modular agent systems ([Zaharia et al., 2024](https://bair.berkeley.edu/blog/2024/02/18/compound-ai-systems/)). These systems integrate retrievers, models, prompts, and tools to handle complex tasks. Their modular design allows seamless upgrades (e.g., integrating a new LLM) and adaptation to changing needs.
# MAGIC
# MAGIC <br><br>
# MAGIC <img style="float: right; margin-top: 10px;" width="700px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/team_flow_liza.png" />
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC <div style="font-size: 19px; margin-left: 0px; clear: left; padding-top: 10px; ">
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/liza.png" style="float: left;" width="80px"> 
# MAGIC <h3 style="padding: 10px 0px 0px 5px;">Liza, a Generative AI engineer, uses the Databricks Intelligence Platform to:</h3>
# MAGIC <ul style="list-style: none; padding: 0; margin-left: 05%;">
# MAGIC   <li style="margin-bottom: 10px; display: flex; align-items: center;">
# MAGIC     <div class="badge" style="height: 30px; width: 30px; border-radius: 50%; background: #fcba33; color: white; text-align: center; line-height: 30px; font-weight: bold; margin-right: 10px;">1</div>
# MAGIC     Build real-time data pipelines
# MAGIC   </li>
# MAGIC   <li style="margin-bottom: 10px; display: flex; align-items: center;">
# MAGIC     <div class="badge" style="height: 30px; width: 30px; border-radius: 50%; background: #fcba33; color: white; text-align: center; line-height: 30px; font-weight: bold; margin-right: 10px;">2</div>
# MAGIC     Retrieve vectors & features
# MAGIC   </li>
# MAGIC   <li style="margin-bottom: 10px; display: flex; align-items: center;">
# MAGIC     <div class="badge" style="height: 30px; width: 30px; border-radius: 50%; background: #fcba33; color: white; text-align: center; line-height: 30px; font-weight: bold; margin-right: 10px;">3</div>
# MAGIC     Create AI agent tools
# MAGIC   </li>
# MAGIC   <li style="margin-bottom: 10px; display: flex; align-items: center;">
# MAGIC     <div class="badge" style="height: 30px; width: 30px; border-radius: 50%; background: #fcba33; color: white; text-align: center; line-height: 30px; font-weight: bold; margin-right: 10px;">4</div>
# MAGIC     Build & deploy agents
# MAGIC   </li>
# MAGIC   <li style="margin-bottom: 10px; display: flex; align-items: center;">
# MAGIC     <div class="badge" style="height: 30px; width: 30px; border-radius: 50%; background: #fcba33; color: white; text-align: center; line-height: 30px; font-weight: bold; margin-right: 10px;">5</div>
# MAGIC     Operate in batch or real-time
# MAGIC   </li>
# MAGIC   <li style="display: flex; align-items: center;">
# MAGIC     <div class="badge" style="height: 30px; width: 30px; border-radius: 50%; background: #fcba33; color: white; text-align: center; line-height: 30px; font-weight: bold; margin-right: 10px;">6</div>
# MAGIC     Evaluate agent performance
# MAGIC   </li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC
# MAGIC **Databricks empowers Liza with a Data + AI platform for Prescriptive Maintenance.** Let’s explore how to deploy this in production.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Building Agent Systems with Databricks Mosaic AI agent framework
# MAGIC
# MAGIC We will build an Agent System designed to generate prescriptive work orders for wind turbine maintenance technicians. This system integrates multiple interacting components to ensure proactive and efficient maintenance, thereby optimizing the overall equipment effectiveness.
# MAGIC
# MAGIC Databricks simplifies this by providing a built-in service to:
# MAGIC
# MAGIC - Create and store your AI tools leveraging UC functions
# MAGIC - Execute the AI tools in a safe way
# MAGIC - Use agents to reason about the tools you selected and chain them together to properly answer your question. 
# MAGIC
# MAGIC At a high level, here is the agent system we will implement in this demo:
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/agent_graph_7.png" style="margin-left: 05%"  width="1000px;">
# MAGIC
# MAGIC This notebook creates the three Mosaic AI tools and associated Mosaic AI endpoints, which will be composed together into a agent in notebook [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-agent-framework-iot-turbine-prescriptive-maintenance).
# MAGIC 1. **Turbine predictor** which uses a Model Serving endpoint to predict turbines at risk of failure.
# MAGIC 2. **Turbine maintenance report retriever**  which uses a Vector Search endpoint to retrieve historical maintenance reports with similar sensor readings.
# MAGIC 3. **Turbine specifications retriever** which uses a Feature Serving endpoint to retrueve turbine specifications based on id.

# COMMAND ----------

# DBTITLE 1,Install required external libraries
# MAGIC %pip install mlflow==2.20.1 databricks-vectorsearch==0.40 databricks-feature-engineering==0.8.0 databricks-sdk==0.40.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Initializing the Application
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 1: Create the Turbine Predictor as a tool to predict turbine failure
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/agent_graph_2.png" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC To enable our Agent System to predict turbine failtures based on industrial IoT sensor readings, we will rely on the model we deployed previously in the  [04.1-automl-iot-turbine-predictive-maintenance]($./04.3-running-inference-iot-turbine) notebook. 
# MAGIC
# MAGIC **Make sure you run this notebook to create the model serving endpoint!**
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using the Model Serving as tool to predict faulty turbines
# MAGIC Let's define the turbine predictor tool function our LLM agent will be able to execute. 
# MAGIC
# MAGIC AI agents use [AI Agent Tools](https://docs.databricks.com/en/generative-ai/create-log-agent.html#create-ai-agent-tools) to perform actions besides language generation, for example to retrieve structured or unstructured data, execute code, or talk to remote services (e.g. send an email or Slack message). 
# MAGIC
# MAGIC These functions can contain any logic, from simple SQL to advanced python. Below we wrap the model serving endpoint in a SQL function using '[ai_query function](https://docs.databricks.com/en/sql/language-manual/functions/ai_query.html)'.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS turbine_maintenance_predictor;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION turbine_maintenance_predictor(hourly_timestamp TIMESTAMP, avg_energy DOUBLE, std_sensor_A DOUBLE, std_sensor_B DOUBLE, std_sensor_C DOUBLE, std_sensor_D DOUBLE, std_sensor_E DOUBLE, std_sensor_F DOUBLE, location STRING, model STRING, state STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This tool predicts whether or not a turbine is faulty to facilitate proactive maintenance'
# MAGIC RETURN
# MAGIC (
# MAGIC     SELECT ai_query(
# MAGIC         'dbdemos_iot_turbine_prediction_endpoint', 
# MAGIC         named_struct(
# MAGIC             'hourly_timestamp', hourly_timestamp,
# MAGIC             'avg_energy', avg_energy,
# MAGIC             'std_sensor_A', std_sensor_A,
# MAGIC             'std_sensor_B', std_sensor_B,
# MAGIC             'std_sensor_C', std_sensor_C,
# MAGIC             'std_sensor_D', std_sensor_D,
# MAGIC             'std_sensor_E', std_sensor_E,
# MAGIC             'std_sensor_F', std_sensor_F,
# MAGIC             'location', location,
# MAGIC             'model', model,
# MAGIC             'state', state        ),
# MAGIC         'STRING'
# MAGIC     )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can test out our function below:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_maintenance_predictor(
# MAGIC     TIMESTAMP '2025-01-14T16:00:00.000+00:00',    -- hourly_timestamp
# MAGIC     0.9000803742589635,                           -- avg_energy
# MAGIC     2.2081154200781867,                           -- std_sensor_A
# MAGIC     2.6012126574143823,                           -- std_sensor_B
# MAGIC     2.1075958066966423,                           -- std_sensor_C
# MAGIC     2.2081154200781867,                           -- std_sensor_D
# MAGIC     2.6012126574143823,                           -- std_sensor_E
# MAGIC     2.1075958066966423,                           -- std_sensor_F
# MAGIC     'Lexington',                                  -- location
# MAGIC     'EpicWind',                                   -- model
# MAGIC     'America/New_York'                           -- state
# MAGIC ) AS prediction

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 2: Create the Maintenance Report Retriever as a tool to retrieve maintenance reports
# MAGIC
# MAGIC To enable our Agent System to retrieve relevant historical maintenance reports for turbines predicted to be at risk of failure, we have to index historical maintenance reports into a Vector Search Index. Since our `Turbine Predictor as a tool` requires sensor readings as input, we will use an embedding vector of the sensor readings to retrieve maintenance reports with similar sensor readings. 
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-06-01%20at%2012.58.23.png?raw=true" style="float: right" width="700px">
# MAGIC
# MAGIC Databricks provides multiple types of vector search indexes:
# MAGIC
# MAGIC - **Managed embeddings**: you provide a text column and endpoint name and Databricks synchronizes the index with your Delta table. 
# MAGIC - **Self Managed embeddings**: you compute the embeddings and save them as a field of your Delta Table, Databricks will then synchronize the index.
# MAGIC - **Direct index**: when you want to use and update the index without having a Delta Table. **(what we'll use in this demo)**
# MAGIC
# MAGIC Since we are working with numerical sensor readings, which are non-text inputs, we have to use Vector Search Direct Index. In this section we:
# MAGIC 1. Create a `Vector Search endpoint`
# MAGIC 2. Create a `Vector Search Direct Index` 
# MAGIC 3. Create a `Vector Search as tool` 
# MAGIC 4. Query a `Vector Search as tool` 

# COMMAND ----------

# MAGIC %md Let's start by loading the `turbine training dataset`, which we will use to index the historical maintenance reports:

# COMMAND ----------

# Read our dataset into a DataFrame
(spark.read.table("turbine_training_dataset").dropDuplicates(["turbine_id"])
  .select("composite_key", "sensor_vector", "maintenance_report", "abnormal_sensor").filter("maintenance_report is not null")
  .write.mode('overwrite').saveAsTable("turbine_sensor_reports"))
spark.sql('ALTER TABLE turbine_sensor_reports SET TBLPROPERTIES (delta.enableChangeDataFeed = true)')

display(spark.table("turbine_sensor_reports"))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Creating the Vector Search endpoint
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/agent_graph_4.png" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC Direct Vector Access Index supports direct read and write of vectors and metadata. The user is responsible for updating this table using the REST API or the Python SDK. This type of index cannot be created using the UI. You must use the REST API or the SDK.
# MAGIC
# MAGIC A vector search index uses a **Vector search endpoint** to serve the embeddings (you can think about it as your Vector Search API endpoint).
# MAGIC
# MAGIC Multiple Indexes can use the same endpoint. 
# MAGIC
# MAGIC Let's now create one.

# COMMAND ----------

# DBTITLE 1,Creating the Vector Search endpoint
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if not endpoint_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME):
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/index_creation.gif?raw=true" width="600px" style="float: right">
# MAGIC
# MAGIC You can view your endpoint on the [Vector Search Endpoints UI](#/setting/clusters/vector-search). Click on the endpoint name to see all indexes that are served by the endpoint.
# MAGIC
# MAGIC
# MAGIC ### Creating the Vector Search Index
# MAGIC
# MAGIC All we now have to do is to as Databricks to create the index on top of our table. The Delta Table will automatically be synched with the index.

# COMMAND ----------

import databricks.sdk.service.catalog as c

# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.turbine_sensor_reports_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  index = vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    source_table_name=f"{catalog}.{db}.turbine_sensor_reports",
    index_name=vs_index_fullname,
    pipeline_type="TRIGGERED",
    primary_key='composite_key',
    embedding_dimension=6,
    embedding_vector_column="sensor_vector"
  )
else:
  print(f"Grabbing existing index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  index = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Define Maintenance Report Retriever function
# MAGIC Below, we utilize the _VECTOR\_SEARCH_ SQL function from Databricks to easily set up our maintenance reports retriever function. Our agent will utilize this function in the subsequent steps!

# COMMAND ----------

spark.sql("DROP FUNCTION IF EXISTS turbine_maintenance_reports_retriever")
spark.sql(f"""
CREATE OR REPLACE FUNCTION turbine_maintenance_reports_retriever(sensor_reading_array ARRAY<DOUBLE>)
RETURNS ARRAY<STRING>
LANGUAGE SQL
RETURN (
  SELECT collect_list(maintenance_report) FROM VECTOR_SEARCH(index => '{catalog}.{schema}.turbine_sensor_reports_vs_index', query_vector => sensor_reading_array, num_results => 3) ) """)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using the Maintenance Report Retriever as tool to retrieve maintenance reports
# MAGIC Now we can now use our function in SQL, and it'll be available to our tool

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT turbine_maintenance_reports_retriever(
# MAGIC   ARRAY(0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638)
# MAGIC   ) AS reports

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 3: Create the Turbine Specifications Retriever as a tool to retrieve turbine specifications
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/agent_graph_3.png" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC To enable our Agent System to retrieve turbine specifications for turbines predicted to be faulty, we need to serve the DLT `turbine_current_features` feature table through a feature serving endpoint.
# MAGIC
# MAGIC Databricks Feature Serving offers a unified interface for serving pre-materialized and on-demand features to models or applications deployed outside Databricks. These endpoints automatically scale to handle real-time traffic, ensuring high availability and low latency.
# MAGIC
# MAGIC This part illustrates how to:
# MAGIC 1. Create a `FeatureSpec`. A `FeatureSpec` defines a set of features (prematerialized and on-demand) that are served together. 
# MAGIC 2. Create an `Online Table` from a Delta Table.
# MAGIC 3. Serve the features. To serve features, you create a Feature Serving endpoint with the `FeatureSpec`.
# MAGIC 4. Create a `Feature Serving as tool` using UC tool functions.
# MAGIC 5. Query a `Feature Serving as tool` using SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC ###  This demo requires a secret to work:
# MAGIC Your Tool will need a secret to authenticate against the online table we create (see [Documentation](https://docs.databricks.com/en/security/secrets/secrets.html)).  <br/>
# MAGIC **Note: if you are using a shared demo workspace and you see that the secret is setup, please don't run these steps and do not override its value**<br/>
# MAGIC
# MAGIC - You'll need to [setup the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) on your laptop or using this cluster terminal: <br/>
# MAGIC `pip install databricks-cli` <br/>
# MAGIC - Configure the CLI. You'll need your workspace URL and a PAT token from your profile page<br>
# MAGIC `databricks configure`
# MAGIC - Create the dbdemos scope:<br/>
# MAGIC `databricks secrets create-scope --scope dbdemos`
# MAGIC - Save your service principal secret. It will be used by the Model Endpoint to autenticate. If this is a demo/test, you can use one of your [PAT token](https://docs.databricks.com/en/dev-tools/auth/pat.html).<br>
# MAGIC `databricks secrets put-secret <SCOPE_GOES_HERE> <KEY_GOES_HERE> --string-value 
# MAGIC <SECRET_GOES_HERE>'`
# MAGIC
# MAGIC *Note: Make sure your service principal has access to the Vector Search index:*
# MAGIC
# MAGIC ```
# MAGIC spark.sql('GRANT USAGE ON CATALOG <catalog> TO `<YOUR_SP>`');
# MAGIC spark.sql('GRANT USAGE ON DATABASE <catalog>.<db> TO `<YOUR_SP>`');
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC import databricks.sdk.service.catalog as c
# MAGIC WorkspaceClient().grants.update(c.SecurableType.TABLE, <index_name>, 
# MAGIC                                 changes=[c.PermissionsChange(add=[c.Privilege["SELECT"]], principal="<YOUR_SP>")])
# MAGIC WorkspaceClient().secrets.put_acl(scope=dbdemos, principal="<YOUR_SP>", permission=workspace.AclPermission.READ)
# MAGIC   ```

# COMMAND ----------

# MAGIC %md ### Set up a Feature Table
# MAGIC
# MAGIC We'll use the table `turbine_current_features` we created in our DLT pipeline as our feature store.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### What's required for our Feature Serving endpoint
# MAGIC
# MAGIC To deploy a Feature Serving endpoint, you need to create a **FeatureSpec**.
# MAGIC
# MAGIC FeatureSpecs are stored in and mananged by Unity Catalog and appear in the Catalog Explorer.
# MAGIC
# MAGIC Tables specified in a FeatureSpec must be published to an **online store or an online table** for Online Serving.
# MAGIC
# MAGIC This demo shows how to setup a **Feature Spec** with a **Feature Function**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up a Databricks Online Table
# MAGIC To access the feature table from Feature Serving, you must create an Online Table from the feature table. You can create an online table from the Catalog Explorer UI, Databricks SDK or Rest API. The steps to use Databricks python SDK are described below. For more details, see the Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#create)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#create)). For information about required permissions, see Permissions ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#user-permissions)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#user-permissions)).

# COMMAND ----------

# DBTITLE 1,Databricks Online Table Setup
from pprint import pprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
import mlflow

w = WorkspaceClient()
online_table_name = f"{catalog}.{db}.turbine_current_features_online"

spec = OnlineTableSpec(
    primary_key_columns=["turbine_id"],
    source_table_full_name=f"{catalog}.{db}.turbine_current_features",
    run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({'triggered': 'true'}),
    perform_full_copy=True
)

try:
    online_table_pipeline = w.online_tables.create(table=OnlineTable(name=online_table_name, spec=spec))
except Exception as e:
    if "already exists" in str(e):
        pass
    else:
        raise e

pprint(w.online_tables.get(online_table_name))

# COMMAND ----------

# DBTITLE 1,Catalog Turbine Specifications
from databricks.feature_engineering import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

features = [FeatureLookup(
    table_name=f"{catalog}.{db}.turbine_current_features",
    lookup_key=["turbine_id"]
  )]

# Create a `FeatureSpec` in Unity Catalog
try:
  fe.create_feature_spec(name=f"{catalog}.{db}.turbine_features_spec", features=features)
except Exception as e:
  if "already exists" in str(e):
    print(f"FeatureSpec {catalog}.{db}.turbine_features_spec already exists. Skipping execution")
  else:
    print(f"An error occurred: {e}")
    raise e

# COMMAND ----------

# MAGIC %md ### Create a Feature Serving endpoint
# MAGIC
# MAGIC Let's create Feature Serving endpoint using the Databricks Python SDK: 

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

try:
 status = w.serving_endpoints.create_and_wait(
   name=FEATURE_SERVING_ENDPOINT_NAME,
   config = EndpointCoreConfigInput(
     served_entities=[
       ServedEntityInput(
         entity_name=f"{catalog}.{db}.turbine_features_spec",
         scale_to_zero_enabled=True,
         workload_size="Small"
       )
     ]
   ) 
 )

except Exception as e:
  if "already exists" in str(e):
    print(f"Serving endpoint {FEATURE_SERVING_ENDPOINT_NAME} already exists. Skipping execution")
  else:
    raise e

# COMMAND ----------

# MAGIC %md You can now view the status of the Feature Serving Endpoint in the table on the **Serving endpoints** page. Click **Serving** in the sidebar to display the page.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the Turbine Specifications Retriever as tool to retrieve turbine specifications
# MAGIC Next, we define the turbine specifications retriever tool function our LLM agent will be able to execute. To do so, we will wrap the Feature Serving endpoint in a UC tool function.

# COMMAND ----------

host = WorkspaceClient().config.host

spark.sql(f"""DROP FUNCTION IF EXISTS {catalog}.{db}.turbine_specifications_retriever_with_secret;""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{db}.turbine_specifications_retriever_with_secret(
  turbine_id STRING, databricks_token STRING)
RETURNS STRUCT<turbine_id STRING, hourly_timestamp STRING, avg_energy DOUBLE, std_sensor_A DOUBLE, std_sensor_B DOUBLE, std_sensor_C DOUBLE, std_sensor_D DOUBLE, std_sensor_E DOUBLE, std_sensor_F DOUBLE, country STRING, lat STRING, location STRING, long STRING, model STRING, state STRING>
LANGUAGE PYTHON
AS
$$
  try:
    import requests
    headers = {{"Authorization": "Bearer " + databricks_token}}
    #Call our vector search endpoint via simple SQL statement
    response = requests.post("{host}/serving-endpoints/dbdemos_iot_turbine_feature_endpoint/invocations", json = {{"dataframe_records": [{{"turbine_id": turbine_id}}]}}, headers=headers)
    return response.json().get('outputs')[0]
  except Exception as e:
    raise e
$$;""")

# COMMAND ----------

# DBTITLE 1,Add the wrapper
# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS turbine_specifications_retriever;
# MAGIC CREATE OR REPLACE FUNCTION turbine_specifications_retriever (turbine_id STRING)
# MAGIC   RETURNS STRUCT<turbine_id STRING, hourly_timestamp STRING, avg_energy DOUBLE, std_sensor_A DOUBLE, std_sensor_B DOUBLE, std_sensor_C DOUBLE, std_sensor_D DOUBLE, std_sensor_E DOUBLE, std_sensor_F DOUBLE, country STRING, lat STRING, location STRING, long STRING, model STRING, state STRING>
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This tool returns turbine specifications based on the turbine_id.'
# MAGIC   RETURN SELECT turbine_specifications_retriever_with_secret(turbine_id, secret('dbdemos', 'ai_agent_sp_token'));

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can query our turbine specifcations retriever tool function:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_specifications_retriever('25b2116a-ae6c-ff55-ce0c-3f08e12656f1') AS turbine_specifications

# COMMAND ----------

# MAGIC %md ## Exploring Mosaic AI Tools in Unity Catalog
# MAGIC
# MAGIC You can now view the UC function tools in Catalog Explorer. Click **Catalog** in the sidebar. In the Catalog Explorer, navigate to your catalog and schema. The UC function tools appears under **Functions**. 
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-18%20at%2016.24.24.png?raw=true"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## What’s next:
# MAGIC Now that we create the Mosaic AI Tools, We will compose them into an agent system that generates maintenance work orders using the Mosaic AI agent framework.
# MAGIC
# MAGIC Open the [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-build-agent-iot-turbine-prescriptive-maintenance) notebook to create and deploy the system.
