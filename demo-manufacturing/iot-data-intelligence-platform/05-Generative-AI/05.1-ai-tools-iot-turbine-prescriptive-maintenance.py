# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Generative AI with Databricks
# MAGIC
# MAGIC ## Generative AI enables a shift from Predictive to Prescriptive Maintenance
# MAGIC Today manufacturers are operating in a challenging environment, including labor shortages, supply chain disruptions and rising raw material prices. To increase asset productivity and reduce costs, the performance of the maintenance function is vital for long-term success. However, according to a recent [McKinsey survey](https://www.mckinsey.com/industries/electric-power-and-natural-gas/our-insights/maintenance-and-operations-is-asset-productivity-broken), many manufacturers are struggling to increase asset productivity, even though investing in maintenance transformation programs. These challenges arise due to labor shortage of maintenance technicians, which is amplified by a lack of knowledge sharing systems between technicians. Therefore, it's hard to learn from past maintenance operations with a significant risk of knowledge loss.
# MAGIC
# MAGIC
# MAGIC <div style="font-family: 'DM Sans'">
# MAGIC   <div style="width: 500px; color: #1b3139; margin-left: 50px; float: left">
# MAGIC     <div style="color: #ff5f46; font-size:80px">73%</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC       of manufacturers have difficulty to recruit new maintenance technicians — Mckinsey survey (2023) 
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px">55%</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC        of manufacturers have formal systems to ensure knowledge is shared between technicians - Mckinsey survey (2023)
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC Generative AI has the potential to revolutionize the maintenance function addressing these challenges by minimizing equipment downtime and boosting asset productivity. Traditionally, maintenance operations have focused on predictive maintenance, which anticipates equipment failures based on historical trends. Generative AI offers an opportunity to advance from predictive to prescriptive maintenance. Agent Systems can support maintenance technicians by identifying faulty equipment and generating prescriptive work orders that outline potential issues and their solutions, based on historical maintenance reports and equipment specifications. This streamlined access to knowledge enhances productivity, enabling less experienced technicians to perform effectively while allowing seasoned professionals to concentrate on more complex problems.
# MAGIC
# MAGIC ### The Shift from Models to Agent Systems
# MAGIC The rise of Generative AI is driving a shift from standalone models to agent systems, as noted by [Zaharia et al. (2024)](https://bair.berkeley.edu/blog/2024/02/18/compound-ai-systems/). Unlike traditional monolithic models, agent systems integrate multiple interacting components — retrievers, models, prompts, chains, and external tools — to handle complex AI tasks.  This approach increases control and trust by incorporating functionalities such as output filtering, dynamic routing, and real-time information retrieval. Furthermore, Agent Systems are more adaptable to evolving industry trends and organizational needs due to their modular design. Each component in an agent system can operate independently, allowing developers to update or replace individual components without disrupting the entire system. For instance, when a new advanced LLM is released, it can be seamlessly integrated without altering the overall architecture. Similarly, components can be added or removed as organizational requirements evolve.
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px;height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC
# MAGIC
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-10-01%20at%2014.43.40.png?raw=true" />
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/cdo.png?raw=true" style="float: left;" width="80px"> 
# MAGIC <h3 style="padding: 10px 0px 0px 5px">Liza, as a Generative AI engineer, needs a data + AI platform accelerating all the GenAI steps:</h3>
# MAGIC <br>
# MAGIC <div style="font-size: 19px; margin-left: 73px; clear: left">
# MAGIC <div class="badge_b"><div class="badge">1</div> Build Data Pipeline supporting real time </div>
# MAGIC <div class="badge_b"><div class="badge">2</div> Retrieve vectors & features</div>
# MAGIC <div class="badge_b"><div class="badge">3</div> Create AI agent tools</div>
# MAGIC <div class="badge_b"><div class="badge">4</div> Build & deploy agent</div>
# MAGIC <div class="badge_b"><div class="badge">5</div> Deploy agent (batch or realtime)</div>
# MAGIC <div class="badge_b"><div class="badge">6</div> Evaluate agent </div>
# MAGIC </div>
# MAGIC
# MAGIC **Liza needs a Data Intelligence Platform**. Let's see how we can deploy a Prescriptive Maintenance agent in production with Databricks.

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
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Prescriptive%20Maintenance%20Demo%20Overview%20(15).png?raw=true" style="margin-left: 10px"  width="1000px;">
# MAGIC
# MAGIC This notebook creates the three Mosaic AI tools and associated Mosaic AI endpoints, which will be composed together into a agent in notebook [05.2-agent-framework-iot-turbine-prescriptive-maintenance]($./05.2-agent-framework-iot-turbine-prescriptive-maintenance).
# MAGIC 1. **Turbine predictor** which uses a Model Serving endpoint to predict turbines at risk of failure.
# MAGIC 2. **Turbine maintenance report retriever**  which uses a Vector Search endpoint to retrieve historical maintenance reports with similar sensor readings.
# MAGIC 3. **Turbine specifications retriever** which uses a Feature Serving endpoint to retrueve turbine specifications based on id.

# COMMAND ----------

# DBTITLE 1,Install required external libraries
# MAGIC %pip install mlflow==2.17.2 databricks-vectorsearch==0.40 databricks-feature-engineering==0.7.0 databricks-sdk==0.34.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Initializing the Application
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 1: Create the Turbine Predictor as a tool to predict turbine failure
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/model.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC To enable our Agent System to predict turbine failtures based on industrial IoT sensor readings, we have to deploy the predictive mainteance model created in [04.1-automl-iot-turbine-predictive-maintenance]($./04.1-automl-iot-turbine-predictive-maintenance) notebook. To so, you can create a Model Serving endpoint from the Catalog Explorer UI, Databricks SDK or Rest API. 
# MAGIC
# MAGIC In this section we:
# MAGIC 1. Create a `Model Serving Endpoint` using the MLflow Deployments SDK - an API for create, update and deletion tasks.
# MAGIC 2. Create a `Model Serving as tool` using a UC functions tools.
# MAGIC 3. Query the `Model Serving as tool` to test it.
# MAGIC

# COMMAND ----------

# MAGIC %md Let's start by creating a Model Serving endpoint using the MLflow Deployments SDK: 

# COMMAND ----------

# DBTITLE 1,deploy_endpoint_iot_turbine_model
from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
endpoint = client.create_endpoint(
    name=MODEL_SERVING_ENDPOINT_NAME,
    config={
        "served_entities": [
            {
                "name": "ads-entity",
                "entity_name": f"{catalog}.{db}.{model_name}",
                "entity_version": get_last_model_version(f"{catalog}.{db}.{model_name}"),
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }
        ]
        }
)

# COMMAND ----------

# MAGIC %md You can now view the status of the Feature Serving Endpoint in the table on the **Serving endpoints** page. Click **Serving** in the sidebar to display the page.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using the Model Serving as tool to predict faulty turbines
# MAGIC Next, let's define the turbine predictor tool function our LLM agent will be able to execute. AI agents use [AI Agent Tools](https://docs.databricks.com/en/generative-ai/create-log-agent.html#create-ai-agent-tools) to perform actions besides language generation, for example to retrieve structured or unstructured data, execute code, or talk to remote services (e.g. send an email or Slack message). These functions can contain any logic, from simple SQL to advanced python. Below we wrap the model serving endpoint in a SQL function using '[ai_query function](https://docs.databricks.com/en/sql/language-manual/functions/ai_query.html)'.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS turbine_maintenance_predictor; 
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION turbine_maintenance_predictor(
# MAGIC     std_sensor_A FLOAT,
# MAGIC     std_sensor_B FLOAT,
# MAGIC     std_sensor_C FLOAT,
# MAGIC     std_sensor_D FLOAT,
# MAGIC     std_sensor_E FLOAT,
# MAGIC     std_sensor_F FLOAT
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This tool predicts whether or not a turbine is faulty to facilitate proactive maintenance'
# MAGIC RETURN
# MAGIC (
# MAGIC     SELECT ai_query(
# MAGIC         'dbdemos_turbine_maintenance_endpoint', 
# MAGIC         named_struct(
# MAGIC             "std_sensor_A", std_sensor_A,
# MAGIC             "std_sensor_B", std_sensor_B,
# MAGIC             "std_sensor_C", std_sensor_C,
# MAGIC             "std_sensor_D", std_sensor_D,
# MAGIC             "std_sensor_E", std_sensor_E,
# MAGIC             "std_sensor_F", std_sensor_F
# MAGIC         ),
# MAGIC         'STRING'
# MAGIC     )
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can query our turbine predictor tool function:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_maintenance_predictor(0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638) AS prediction

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
df = spark.read.table("turbine_training_dataset").dropDuplicates(["turbine_id"]).select("composite_key", "sensor_vector", "maintenance_report")
display(df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Creating the Vector Search endpoint
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/vector.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
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
# MAGIC ### Creating the Direct Vector Search Index
# MAGIC
# MAGIC All we now have to do is to as Databricks to create the index. 
# MAGIC The following example creates a Direct Vector Access Index.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Creating Index for Turbine Training Dataset
import databricks.sdk.service.catalog as c

# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.turbine_maintenance_reports_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  index = vsc.create_direct_access_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    index_name=vs_index_fullname,
    primary_key='composite_key',
    embedding_dimension=6,
    embedding_vector_column="sensor_vector",
    schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
  )

# COMMAND ----------

# MAGIC %md 
# MAGIC You can use the Python SDK or the REST API to insert, update, or delete data from a Direct Vector Access Index. The Direct Vector Index expects a JSON structure compatible as input.

# COMMAND ----------

# DBTITLE 1,Upserting a Dictionary List
# Only insert records that have maintenance reports
df = df.filter("maintenance_report != 'N/A'").limit(200)
index.upsert([row.asDict() for row in df.collect()])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using the Maintenance Report Retriever as tool to retrieve maintenance reports
# MAGIC Now we can define the maintencance report retriever tool function our LLM agent will be able to execute. Delta Sync Indexes in Databricks can be queried from SQL using the [vector_search function](https://docs.databricks.com/en/sql/language-manual/functions/vector_search.html). Since, this is not (yet) supported for Direct Vector Access Indexes, we will wrap the Vector Search API in a UC tool function.

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC ###  This demo requires a secret to work:
# MAGIC Your Model Serving Endpoint needs a secret to authenticate against your Vector Search Index (see [Documentation](https://docs.databricks.com/en/security/secrets/secrets.html)).  <br/>
# MAGIC **Note: if you are using a shared demo workspace and you see that the secret is setup, please don't run these steps and do not override its value**<br/>
# MAGIC
# MAGIC - You'll need to [setup the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) on your laptop or using this cluster terminal: <br/>
# MAGIC `pip install databricks-cli` <br/>
# MAGIC - Configure the CLI. You'll need your workspace URL and a PAT token from your profile page<br>
# MAGIC `databricks configure`
# MAGIC - Create the dbdemos scope:<br/>
# MAGIC `databricks secrets create-scope dbdemos`
# MAGIC - Save your service principal secret. It will be used by the Model Endpoint to autenticate. If this is a demo/test, you can use one of your [PAT token](https://docs.databricks.com/en/dev-tools/auth/pat.html).<br>
# MAGIC `databricks secrets put-secret --json '{
# MAGIC   "scope": "dbdemos",
# MAGIC   "key": "ai_agent_sp_token",
# MAGIC   "string_value": "<secret>"
# MAGIC }'`
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

host = w.config.host
spark.sql(f"""DROP FUNCTION IF EXISTS {catalog}.{db}.turbine_maintenance_reports_retriever_with_secret;""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{db}.turbine_maintenance_reports_retriever_with_secret(search_vector ARRAY<DOUBLE>, databricks_token STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
AS
$$
  try:
    import requests
    headers = {{"Authorization": "Bearer "+databricks_token}}
    #Call our vector search endpoint via simple SQL statement
    response = requests.post("{host}/api/2.0/vector-search/indexes/{catalog}.{db}.turbine_maintenance_reports_vs_index/query", json ={{"columns":["maintenance_report"],"query_vector":list(search_vector), "num_results":3}}, headers=headers)
    
    formatted_response = [item[0] for item in response.json().get('result', {{}}).get('data_array', [])]
      
    return formatted_response
  except Exception as e:
    raise
$$;""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_maintenance_reports_retriever_with_secret(
# MAGIC     ARRAY(0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638), 
# MAGIC   secret('dbdemos', 'ai_agent_sp_token')
# MAGIC ) AS reports

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS turbine_maintenance_reports_retriever;
# MAGIC CREATE OR REPLACE FUNCTION turbine_maintenance_reports_retriever (search_vector ARRAY<DOUBLE>)
# MAGIC RETURNS ARRAY<STRING>
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This tool returns historical maintenance reports from turbines that had similar sensor readings as input sensor readings.'
# MAGIC RETURN SELECT turbine_maintenance_reports_retriever_with_secret(search_vector, secret('dbdemos', 'ai_agent_sp_token'));

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can query our turbine maintenance reports retriever tool function:

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT turbine_maintenance_reports_retriever(
# MAGIC   ARRAY(0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638)
# MAGIC   ) AS reports

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 3: Create the Turbine Specifications Retriever as a tool to retrieve turbine specifications
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/feature.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC To enable our Agent System to retrieve turbine specifications for turbines predicted t obe faulty, we need to create and serve a feature table through a feature serving endpoint.
# MAGIC
# MAGIC Databricks Feature Serving offers a unified interface for serving pre-materialized and on-demand features to models or applications deployed outside Databricks. These endpoints automatically scale to handle real-time traffic, ensuring high availability and low latency.
# MAGIC
# MAGIC This part illustrates how to:
# MAGIC 1. Create a `Feature Table` in Unity Catalog 
# MAGIC 2. Create a `FeatureSpec`. A `FeatureSpec` defines a set of features (prematerialized and on-demand) that are served together. 
# MAGIC 3. Create an `Online Table` from a Delta Table.
# MAGIC 4. Serve the features. To serve features, you create a Feature Serving endpoint with the `FeatureSpec`.
# MAGIC 5. Create a `Feature Serving as tool` using UC tool functions.
# MAGIC 6. Query a `Feature Serving as tool` using SQL.

# COMMAND ----------

# MAGIC %md ### Set up a Feature Table
# MAGIC The first step to create a Feature Serving endpoint is to create a feature table in Unity Catalog using the FeatureEngineeringClient. You can use `fe.create_table` without providing a dataframe, and then later populate the feature table using `fe.write_table`.

# COMMAND ----------

# DBTITLE 1,Creating Wind Turbine Specifications Feature Table
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

df = spark.table("turbine").filter("turbine_id IS NOT NULL")

# Create a new feature table for storing wind turbine specifications
feature_table = fe.create_table(
  name=f"{catalog}.{db}.turbine_specifications",
  primary_keys=['turbine_id'],
  schema=df.schema,
  description='wind turbine specification features')

# Write the synthetic wind turbine data into the feature table
fe.write_table(
  name=f"{catalog}.{db}.turbine_specifications",
  df = df,
  mode = 'merge'
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### What's required for our Feature Serving endpoint
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-05-30%20at%2018.43.46.png?raw=true" style="float: right" width="800px">
# MAGIC
# MAGIC To deploy a Feature Serving endpoint, you need to create a **FeatureSpec**: a chain of user-defined:
# MAGIC - Feature Retrievals (known as **FeatureLookUps**)
# MAGIC - Feature Transformations (known as **FeatureFunctions**)
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

workspace = WorkspaceClient()

online_table_name = f"{catalog}.{db}.turbine_specifications_online"

spec = OnlineTableSpec(
  primary_key_columns = ["turbine_id"],
  source_table_full_name = f"{catalog}.{db}.turbine_specifications",
  run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({'triggered': 'true'}),
  perform_full_copy=True)

try:
  online_table_pipeline = workspace.online_tables.create(name=online_table_name, spec=spec)
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e

pprint(workspace.online_tables.get(online_table_name))

# COMMAND ----------

# DBTITLE 1,Catalog Turbine Specifications
from databricks.feature_engineering import FeatureLookup

features = [FeatureLookup(
    table_name=f"{catalog}.{db}.turbine_specifications",
    lookup_key=["turbine_id"]
  )]

# Create a `FeatureSpec` in Unity Catalog
fe.create_feature_spec(
  name=f"{catalog}.{db}.turbine_feature_spec",
  features=features,
)

# COMMAND ----------

# MAGIC %md ### Create a Feature Serving endpoint
# MAGIC
# MAGIC Let's create Feature Serving endpoint using the Databricks Python SDK: 

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

try:
 status = workspace.serving_endpoints.create_and_wait(
   name=FEATURE_SERVING_ENDPOINT_NAME,
   config = EndpointCoreConfigInput(
     served_entities=[
       ServedEntityInput(
         entity_name=f"{catalog}.{db}.turbine_feature_spec",
         scale_to_zero_enabled=True,
         workload_size="Small"
       )
     ]
   )
 )

except Exception as e:
  raise

# COMMAND ----------

# MAGIC %md You can now view the status of the Feature Serving Endpoint in the table on the **Serving endpoints** page. Click **Serving** in the sidebar to display the page.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the Turbine Specifications Retriever as tool to retrieve turbine specifications
# MAGIC Next, we define the turbine specifications retriever tool function our LLM agent will be able to execute. To do so, we will wrap the Feature Serving endpoint in a UC tool function.

# COMMAND ----------

spark.sql(f"""DROP FUNCTION IF EXISTS {catalog}.{db}.turbine_specifications_retriever_with_secret;""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{db}.turbine_specifications_retriever_with_secret(
  turbine_id STRING, databricks_token STRING)
RETURNS STRUCT<
country STRING, 
    lat STRING, 
    location STRING, 
    long STRING, 
    model STRING, 
    state STRING, 
    turbine_id STRING, 
    _rescued_data STRING
>
LANGUAGE PYTHON
AS
$$
  try:
    import requests
    headers = {{"Authorization": "Bearer " + databricks_token}}
    #Call our vector search endpoint via simple SQL statement
    response = requests.post("{host}/serving-endpoints/turbineSpecEndpoint/invocations", json = {{"dataframe_records": [{{"turbine_id": turbine_id}}]}}, headers=headers)

    return response.json().get('outputs')[0]
  except Exception as e:
    raise e
$$;""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_specifications_retriever_with_secret('25b2116a-ae6c-ff55-ce0c-3f08e12656f1',  secret('dbdemos', 'ai_agent_sp_token')
# MAGIC ) AS turbine_specifications

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS turbine_specifications_retriever;
# MAGIC CREATE OR REPLACE FUNCTION turbine_specifications_retriever (turbine_id STRING)
# MAGIC RETURNS STRUCT<
# MAGIC     country STRING, 
# MAGIC         lat STRING, 
# MAGIC         location STRING, 
# MAGIC         long STRING, 
# MAGIC         model STRING, 
# MAGIC         state STRING, 
# MAGIC         turbine_id STRING, 
# MAGIC         _rescued_data STRING
# MAGIC     >
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This tool returns turbine specifications based on the turbine_id.'
# MAGIC RETURN SELECT turbine_specifications_retriever_with_secret(turbine_id,  secret('dbdemos', 'ai_agent_sp_token'));

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
