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
# MAGIC
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05.1-ai-tools-iot-turbine-prescriptive-maintenance&demo_name=lakehouse-iot-platform&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Building Agent Systems with Databricks Mosaic AI agent framework
# MAGIC
# MAGIC We will build an Agent System designed to generate prescriptive work orders for wind turbine maintenance technicians. This system integrates multiple interacting components to ensure proactive and efficient maintenance, thereby optimizing the overall equipment effectiveness.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/iot_agent_graph_v2_0.png" style="margin-left: 5px; float: right"  width="1000px;">
# MAGIC
# MAGIC Databricks simplifies this by providing a built-in service to:
# MAGIC
# MAGIC - Create and store your AI tools leveraging UC functions
# MAGIC - Execute the AI tools in a safe way
# MAGIC - Use agents to reason about the tools you selected and chain them together to properly answer your question. 
# MAGIC
# MAGIC
# MAGIC This notebook creates the three Mosaic AI tools and associated Mosaic AI endpoints, which will be composed together into a agent in notebook [05.2-agent-creation-guide]($./05.2-agent-creation-guide).
# MAGIC 1. **Turbine predictor** which uses a Model Serving endpoint to predict turbines at risk of failure.
# MAGIC 2. **Turbine specifications retriever** which retrieve the turbine specifications based on its id.
# MAGIC 3. **Turbine maintenance guide**  which uses a Vector Search endpoint to retrieve maintenance guide based on the turbines and issues being adressed.

# COMMAND ----------

# DBTITLE 1,Install required external libraries
# MAGIC %pip install mlflow==3.1.1 databricks-vectorsearch==0.57 databricks-feature-engineering==0.12.1 databricks-sdk==0.59.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Initializing the Application
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 1: Create the Turbine Predictor as a tool to predict turbine failure
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/iot_agent_graph_v2_1.png" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC To enable our Agent System to predict turbine failtures based on industrial IoT sensor readings, we will rely on the model we deployed previously in the  [./04.3-running-inference-iot-turbine]($./04.3-running-inference-iot-turbine) notebook. 
# MAGIC
# MAGIC **Make sure you run this ML notebook to create the model serving endpoint!**
# MAGIC
# MAGIC
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
# MAGIC
# MAGIC ## Part 2: Create a Turbine Specifications Retriever as tool to retrieve turbine specifications
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/iot_agent_graph_v2_2.png" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC This is great, our agent can access the predictive maintenance model, but we now need a tool to get the latest reading on our wind turbines to be able to get the features required by our ML model. 
# MAGIC
# MAGIC Let's create a simple retriever tool function our LLM agent will be able to execute, with all the features required to call our ML model.
# MAGIC
# MAGIC In this case, we'll simply do a SQL call to our ML model.
# MAGIC
# MAGIC *Note: in a more production-grade setup, we would instead use Databricks managed postgres to fetch our features with millisecond response time to speedup our system, but this is out side of the scope of this demo!* 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Create a lakebase
# MAGIC
# MAGIC Lakebase is a new feature provided by Databricks adding support for transactional (OLTP) databases to perform as in our case transactional reads. Without any manual effort or delta table is synced to a managed postgress database for transactional operations. You can discover more here: https://learn.microsoft.com/en-us/azure/databricks/oltp/
# MAGIC
# MAGIC Let's create first a so called snyced table running on the OLTP database before creating our retriever.
# MAGIC
# MAGIC Hereby we are using the Python SDK. You can also perform those steps using the UI following the Docu above or the Demo here: https://www.databricks.com/resources/demos/tours/appdev/databricks-lakebase?itm_data=demo_center

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from databricks.sdk.service.database import SyncedDatabaseTable, SyncedTableSpec, NewPipelineSpec, SyncedTableSchedulingPolicy

# Initialize the Workspace client
w = WorkspaceClient()

# COMMAND ----------

# Get instance if already exists
try:
    instance = w.database.get_database_instance("iot-database-instance")

# Create a database instance if not exists
except Exception as e:
    if "Resource not found" in str(e):
        instance = w.database.create_database_instance(
            DatabaseInstance(
                name="iot-database-instance",
                capacity="CU_1"
            )
        )
    else:
        raise e

print(f"Created database instance: {instance.name}")
print(f"Connection endpoint: {instance.read_write_dns}")

# COMMAND ----------

# Create a synced table in a standard UC catalog
synced_table = w.database.create_synced_database_table(
    SyncedDatabaseTable(
        name=f"{catalog}.{db}.turbine_current_features_synced",  # Full three-part name
        database_instance_name="iot-database-instance",  # Required for standard catalogs
        logical_database_name="iot_db",  # Required for standard catalogs
        spec=SyncedTableSpec(
            source_table_full_name=f"{catalog}.{db}.turbine_current_features",
            primary_key_columns=["turbine_id"],
            scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
            timeseries_key="hourly_timestamp",
            create_database_objects_if_missing=True,  # Create database/schema if needed
            new_pipeline_spec=NewPipelineSpec(
                storage_catalog="storage_catalog",
                storage_schema="storage_schema"
            )
        ),
    )
)
print(f"Created synced table: {synced_table.name}")

# Check the status of a synced table
synced_table_name = f"{catalog}.{db}.turbine_current_features_synced"
status = w.database.get_synced_database_table(name=synced_table_name)
print(f"Synced table status: {status.data_synchronization_status.detailed_state}")
print(f"Status message: {status.data_synchronization_status.message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2. Create the retriever

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION turbine_specifications_retriever(turbine_id STRING)
# MAGIC RETURNS STRUCT<turbine_id STRING, hourly_timestamp STRING, avg_energy DOUBLE, std_sensor_A DOUBLE, std_sensor_B DOUBLE, std_sensor_C DOUBLE, std_sensor_D DOUBLE, std_sensor_E DOUBLE, std_sensor_F DOUBLE, country STRING, lat STRING, location STRING, long STRING, model STRING, state STRING>
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns the specifications of one specific turbine based on its turbine id'
# MAGIC RETURN (
# MAGIC   SELECT struct(turbine_id, hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, country, lat, location, long, model, state)
# MAGIC   FROM turbine_current_features_synced
# MAGIC   WHERE turbine_id = turbine_specifications_retriever.turbine_id
# MAGIC   LIMIT 1
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_specifications_retriever('25b2116a-ae6c-ff55-ce0c-3f08e12656f1') AS turbine_specifications

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Part 3: Add a tool to access our maintenance guide content and provide support to the operator during maintenance operation
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/manufacturing/lakehouse-iot-turbine/iot_agent_graph_v2_3.png" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC
# MAGIC We were provided with PDF guide containing all the error code and maintenance steps for the critical components of our wind turbine. The're saved as pdf file in our volume.
# MAGIC
# MAGIC Let's parse them and index them so that we can properly retrieve them. We'll save them in a Vector Search endpoint and leverage it to guide the operators with the maintenance step and recommendations.
# MAGIC
# MAGIC We'll use a Managed embedding index to make it simple. In this section we will:
# MAGIC
# MAGIC 1. Parse and save our PDF text in a Delta Table using Databricks AI Query `ai_parse_document`
# MAGIC 2. Create a `Vector Search endpoint` (required to host your vector search index)
# MAGIC 3. Create a `Vector Search Direct Index`  (the actual index)
# MAGIC 4. Create a `Tool (UC function)` using our vector search 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### 3.1. Parse and save our PDF text
# MAGIC Let's start by parsing the maintenance guide documents, saved as pdf in our volume:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS turbine_maintenance_guide (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   EAN STRING,
# MAGIC   weight STRING,
# MAGIC   component_type STRING,
# MAGIC   component_name STRING,
# MAGIC   full_guide STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use first the _ai_parse_document_ function (one of many ai functions we offer on Databricks out of the box) to extract the information from the pdf documents as a table. You can read more about the function here: https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_docs AS
# MAGIC SELECT ai_parse_document(content) AS parsed_document
# MAGIC FROM READ_FILES("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/maintenance_guide", format => 'binaryFile');
# MAGIC
# MAGIC SELECT * FROM parsed_docs

# COMMAND ----------

# MAGIC %md
# MAGIC The parsed document is generated as a JSON as Variant type. Let's concat the text into one cell removing the json structure which will be our final guide. Variant allows as to have flexible schemas within our data. Processing the Variant columns is a bit different though. See for more here: https://docs.databricks.com/aws/en/semi-structured/variant

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transformed_docs AS
# MAGIC SELECT array_join(
# MAGIC             transform(parsed_document:document.elements::ARRAY<STRUCT<content:STRING>>, x -> x.content), '\n') AS full_guide
# MAGIC FROM parsed_docs;
# MAGIC
# MAGIC SELECT * FROM transformed_docs

# COMMAND ----------

# MAGIC %md
# MAGIC Finally we generate our table extracting some additional information from the guides like EAN, weight, component_type and component_name. Also for this task we are using one of our ai functions _ai_extract_. See more here: https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE turbine_maintenance_guide (EAN, weight, component_type, component_name, full_guide)
# MAGIC SELECT ai_extract.*, full_guide FROM (
# MAGIC   SELECT 
# MAGIC       ai_extract(full_guide, array('EAN', 'weight', 'component_type', 'component_name')) AS ai_extract,
# MAGIC       full_guide
# MAGIC   FROM transformed_docs
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM turbine_maintenance_guide

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3.2. Creating the Vector Search endpoint
# MAGIC
# MAGIC Let's create a new Vector search endpoint. You can also use the [UI under Compute](#/setting/clusters/vector-search) to directly create your endpoint.

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
# MAGIC ### 3.3 Creating the Vector Search Index
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/index_creation.gif?raw=true" width="600px" style="float: right; margin-left: 10px">
# MAGIC
# MAGIC You can view your endpoint on the [Vector Search Endpoints UI](#/setting/clusters/vector-search). Click on the endpoint name to see all indexes that are served by the endpoint.
# MAGIC
# MAGIC All we now have to do is to as Databricks to create the index on top of our table. The Delta Table will automatically be synched with the index.
# MAGIC
# MAGIC
# MAGIC Again, you can do that using your Unity Catalog UI, and selecting the turbine_maintenance_guide table in your Unity Catalog, and click on add a vector search. 

# COMMAND ----------

# DBTITLE 1,Creating the VS from the maintenance table
import databricks.sdk.service.catalog as c

# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.turbine_maintenance_guide_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  index = vsc.create_delta_sync_index_and_wait(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    source_table_name=f"{catalog}.{db}.turbine_maintenance_guide",
    index_name=vs_index_fullname,
    pipeline_type="TRIGGERED",
    primary_key='id',
    embedding_source_column="full_guide",
    embedding_model_endpoint_name="databricks-gte-large-en"
  )
else:
  print(f"Grabbing existing index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  index = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3.4 Create our tool
# MAGIC Below, we utilize the _VECTOR\_SEARCH_ SQL function from Databricks to easily set up our maintenance reports retriever function. Our agent will utilize this function in the subsequent steps!
# MAGIC
# MAGIC **In our Agent we will not leverage the function instead we can directly integrate the vector index into the agent as the SQL function for authentification is not currently supported within the agent.**

# COMMAND ----------

spark.sql("DROP FUNCTION IF EXISTS turbine_maintenance_guide_retriever")
spark.sql(f"""
CREATE OR REPLACE FUNCTION turbine_maintenance_guide_retriever(question STRING)
RETURNS ARRAY<STRING>
LANGUAGE SQL
COMMENT 'Returns one mantainance report based on asked question'
RETURN (
  SELECT collect_list(full_guide) FROM VECTOR_SEARCH(index => '{catalog}.{schema}.turbine_maintenance_guide_vs_index', query => question, num_results => 1) ) """)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Let's test the tool we created
# MAGIC SELECT turbine_maintenance_guide_retriever('The VibeGuard TVS-950 is giving me an error code TVS-001.') AS reports

# COMMAND ----------

# MAGIC %md ## Exploring Mosaic AI Tools in Unity Catalog
# MAGIC
# MAGIC Our tools are ready! 
# MAGIC
# MAGIC You can now view the UC function tools in Catalog Explorer. Click **Catalog** in the sidebar. In the Catalog Explorer, navigate to your catalog and schema. 
# MAGIC
# MAGIC The UC function tools appears under **Functions**. 
# MAGIC
# MAGIC <img src="https://github.com/Datastohne/demo/blob/main/Screenshot%202024-09-18%20at%2016.24.24.png?raw=true"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## What’s next: test your Agents with Databricks Playground
# MAGIC
# MAGIC Now that we have our AI Tools ready and registered in Unity Catalog, we can compose them into an agent system that generates maintenance work orders using the Mosaic AI agent framework.
# MAGIC
# MAGIC Open the [05.2-agent-creation-guide]($./05.2-agent-creation-guide) notebook to create and deploy the system.
