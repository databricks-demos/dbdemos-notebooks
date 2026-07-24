# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Events Directly into Your Lakehouse with Zerobus
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/clickstream-direct-to-lakehouse/zerobus-delta-architecture.png" width="100%" alt="Clickstream events flow through Zerobus into a Delta events table, and Structured Streaming sessionizes them into a sessions table"/>
# MAGIC
# MAGIC Zerobus enables direct ingestion of application events into a Unity Catalog Delta table, eliminating the need for an intermediate message bus or separate Spark ingestion job.
# MAGIC
# MAGIC This notebook simulates an application client by generating synthetic clickstream events and streaming them to the target events table via the Zerobus REST API. For production deployment patterns, refer to the architectural notes at the end of this notebook.

# COMMAND ----------

# DBTITLE 1,Demo Configuration
dbutils.widgets.text("zerobus_client_id", "",                            "Zerobus service principal client_id (application_id)")
dbutils.widgets.text("duration_seconds",  "150",                         "Seconds to stream live events for")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Target Use Cases
# MAGIC * **Real-Time Telemetry:** Continuous ingestion of application events, clickstream data, and IoT/device telemetry requiring sub-minute query availability.
# MAGIC * **Brokerless Ingestion:** High-volume, variable-throughput streaming directly to the lakehouse without managing a dedicated message broker or decoupled ingestion jobs.
# MAGIC * **Pipeline Consolidation:** Replacing multi-stage batch pipelines (e.g., staging JSON in S3 followed by `COPY INTO`) with low-latency direct writes.
# MAGIC * **Greenfield Architecture:** Streamlining new ingestion paths by bypassing traditional message bus and Spark consumer configurations.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Authentication and Configuration
# MAGIC Zerobus authenticates via OAuth 2.0 Machine-to-Machine (M2M) using a Databricks service principal. The service principal requires `USE CATALOG`, `USE SCHEMA`, and `SELECT, MODIFY` privileges on the target table. To prevent credential exposure, the `client_secret` is retrieved via Databricks secret scopes.
# MAGIC
# MAGIC ### Setup Instructions
# MAGIC 1. **Create Service Principal:** Provision a service principal and generate an OAuth secret within your Databricks workspace (administrative privileges required). Retain the `application_id` and secret value.
# MAGIC 2. **Configure Secret Scope:** Use the Databricks CLI to store the secret securely:
# MAGIC    ```bash
# MAGIC    databricks secrets create-scope zerobus-demo
# MAGIC    databricks secrets put-secret zerobus-demo client-secret
# MAGIC    ```
# MAGIC 3. Initialize Notebook: Input the service principal's `application_id` into the `zerobus_client_id widget`. Running the notebook automatically applies the required Unity Catalog grants.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisite: Service Principal Grants
# MAGIC Before the service principal can write, it needs the right Unity Catalog grants. This cell checks the prerequisites (application_id set, secret stored), applies the grants, and re-applies them after a `reset_all_data` run, which drops the schema and the grants with it.

# COMMAND ----------

client_id = dbutils.widgets.get("zerobus_client_id")
assert client_id, "Set the zerobus_client_id widget to your service principal's application_id (see the configuration cell at the top)."

# The scope and key match the CLI commands in the setup steps.
SECRET_SCOPE = "zerobus-demo"
SECRET_KEY   = "client-secret"
try:
  client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
except Exception:
  raise RuntimeError(
    f"No secret at {SECRET_SCOPE}/{SECRET_KEY}. Store it from your terminal first: "
    f"databricks secrets create-scope {SECRET_SCOPE} && databricks secrets put-secret {SECRET_SCOPE} {SECRET_KEY} "
    f"(see the setup steps above)."
  ) from None

for _stmt in [
  f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{client_id}`",
  f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{db}` TO `{client_id}`",
  f"GRANT SELECT, MODIFY ON TABLE `{catalog}`.`{db}`.events TO `{client_id}`",
]:
  spark.sql(_stmt)
print(f"Granted USE CATALOG/SCHEMA + SELECT, MODIFY on {catalog}.{db}.events to service principal {client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Client Initialization
# MAGIC
# MAGIC Construct the region-specific Zerobus endpoint URL and request an OAuth token containing the required Unity Catalog privilege scopes.

# COMMAND ----------

import urllib.request, urllib.error, urllib.parse, base64, json

workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

# The Zerobus endpoint hostname is workspace- and region-specific, so we need this workspace's
# numeric id. In classic, this is available in a Spark conf (clusterOwnerOrgId). In serverless, we
# retrieve it from a SCIM response header.
try:
  default_ws_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
except Exception:
  hdrs = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}
  req = urllib.request.Request(workspace_url + "/api/2.0/preview/scim/v2/Me", headers=hdrs)
  with urllib.request.urlopen(req) as r:
    # The org-id is in the response headers
    default_ws_id = r.headers.get("x-databricks-org-id", "")

# Region for the endpoint. In classic, this is available in a Spark conf (clusterUsageTags.region).
# In serverless, it defaults to us-west-2 and is overridable via the widget.
try:
  default_region = spark.conf.get("spark.databricks.clusterUsageTags.region")
except Exception:
  default_region = "us-west-2"
dbutils.widgets.text("aws_region",   default_region, "AWS region (auto-detected, override if needed)")
dbutils.widgets.text("workspace_id", default_ws_id, "Workspace ID for Zerobus endpoint")
region           = dbutils.widgets.get("aws_region")
workspace_id     = dbutils.widgets.get("workspace_id")
zerobus_endpoint = f"https://{workspace_id}.zerobus.{region}.cloud.databricks.com"

print(f"Workspace:        {workspace_url}")
print(f"Zerobus endpoint: {zerobus_endpoint}")
print(f"Target table:     {catalog}.{db}.events")
print(f"Client ID:        {client_id}  (secret len={len(client_secret)})")

# The token is scoped two ways:
#   - `resource` sets the token audience to the Zerobus Direct Write API.
#   - `authorization_details` embeds the Unity Catalog privileges Zerobus checks on write (USE CATALOG,
#     USE SCHEMA, and SELECT + MODIFY on the target table).
def get_oauth_token() -> str:
  basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
  auth_details = json.dumps([
    {"type":"unity_catalog_privileges","privileges":["USE CATALOG"],"object_type":"CATALOG","object_full_path":catalog},
    {"type":"unity_catalog_privileges","privileges":["USE SCHEMA"], "object_type":"SCHEMA", "object_full_path":f"{catalog}.{db}"},
    {"type":"unity_catalog_privileges","privileges":["SELECT","MODIFY"],"object_type":"TABLE","object_full_path":f"{catalog}.{db}.events"},
  ])
  form = {
    "grant_type": "client_credentials",
    "scope": "all-apis",
    "resource": f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi",
    "authorization_details": auth_details,
  }
  req = urllib.request.Request(
    f"{workspace_url}/oidc/v1/token",
    data=urllib.parse.urlencode(form).encode(),
    headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
    method="POST",
  )
  body = urllib.request.urlopen(req).read().decode()
  return json.loads(body)["access_token"]

oauth_token = get_oauth_token()
print(f"OAuth token fetched (len={len(oauth_token)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Live Event Generation
# MAGIC Generates a synthetic, real-time event stream for a specified duration (`duration_seconds`). This simulates dynamic user sessions opening and closing. Run the downstream `02-sessionize` notebook concurrently to monitor real-time processing.
# MAGIC

# COMMAND ----------

import uuid, random, time, json
random.seed(int(time.time()))

def make_event(user_id, ts):
    return {
        "user_id":    user_id,
        "event_id":   str(uuid.uuid4()) if random.random() < 0.98 else "",  # ~2% blank - 02-sessionize filters these out
        "event_date": ts,                                                   # current time, epoch seconds
        "platform":   random.choice(["ios", "android", "other"]),
        "action":     random.choice(["view", "click", "log"]),
        "uri":        "https://databricks.com/page" + str(random.randint(0, 9)),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5/ Ingestion via Zerobus REST API
# MAGIC Periodically transmits JSON record arrays via HTTP `POST` to the Zerobus insertion endpoint:
# MAGIC `{endpoint}/zerobus/v1/tables/{catalog}.{schema}.{table}/insert`
# MAGIC
# MAGIC Ingested records are written directly to the Delta table and become queryable within seconds.

# COMMAND ----------

target_table = f"{catalog}.{db}.events"
ingest_url   = f"{zerobus_endpoint}/zerobus/v1/tables/{target_table}/insert"

# POST a JSON array of records to the table's /insert endpoint with the Zerobus token. The target
# table is also named in a header.
def post_batch(records: list) -> None:
  req = urllib.request.Request(
    ingest_url,
    data=json.dumps(records).encode(),
    headers={
      "Authorization": f"Bearer {oauth_token}",
      "Content-Type": "application/json",
      "x-databricks-zerobus-table-name": target_table,
    },
    method="POST",
  )
  try:
    urllib.request.urlopen(req)
  except urllib.error.HTTPError as e:
    body = e.read().decode()[:500]
    print(f"  POST {ingest_url}")
    print(f"  HTTP {e.code} response body: {body}")
    print(f"  response headers: {dict(e.headers)}")
    raise

duration_seconds = int(dbutils.widgets.get("duration_seconds"))

# The loop does two things:
#   1. Stamps every event with the current time, so the downstream watermark advances and the
#      idle timers fire.
#   2. Churns the active user set (some keep clicking, some drop out, new ones join) so sessions
#      both open and close.
pool   = [f"user-{i:02d}" for i in range(30)]
active = pool[:6]            # initial in-session users
idx    = 6
sent   = 0
start  = time.time()
print(f"Streaming live events for {duration_seconds}s into {target_table} (run 02-sessionize alongside to watch)...")
while time.time() - start < duration_seconds:
  now = int(time.time())
  events = []
  for u in active:
    if random.random() < 0.75:                       # most in-session users click each tick
      events += [make_event(u, now) for _ in range(random.randint(1, 3))]
  if events:
    post_batch(events)
    sent += len(events)
  # churn the active set: drop a user (its session closes after the idle gap), add a user (a session opens)
  if random.random() < 0.30 and len(active) > 3:
    active.pop(random.randrange(len(active)))
  if random.random() < 0.30 and idx < len(pool):
    active.append(pool[idx]); idx += 1
  print(f"  t+{int(time.time()-start):>3}s | +{len(events):>2} events | {sent:>5} total | {len(active)} users in-session")
  time.sleep(2.5)
print(f"\nDone. Streamed {sent} live events over {duration_seconds}s to {target_table}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6/ Data Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, COUNT(*) AS clicks,
# MAGIC        FROM_UNIXTIME(MIN(event_date)) AS first_click,
# MAGIC        FROM_UNIXTIME(MAX(event_date)) AS last_click
# MAGIC FROM events
# MAGIC GROUP BY user_id
# MAGIC ORDER BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7/ Production Deployment Patterns
# MAGIC For production environments, select one of the following deployment topologies:
# MAGIC
# MAGIC 1. **Embedded SDK:** Integrate the Zerobus SDK directly into the application layer for direct writes.
# MAGIC 2. **Sidecar Architecture:** Deploy a lightweight sidecar forwarder to accept local HTTP/JSON payloads and manage Zerobus authentication and retry logic independently.
# MAGIC 3. **Transport Bridge:** Deploy a consumer on existing message brokers (e.g., Apache Kafka, AWS Kinesis) to forward events via Zerobus during cloud migrations.
# MAGIC
# MAGIC ### Security Best Practices
# MAGIC Always authenticate production streams using a service principal via OAuth M2M rather than interactive user identities. Apply the following minimum required privileges:
# MAGIC
# MAGIC ```
# MAGIC GRANT USE CATALOG ON CATALOG `catalog` TO `<sp-application-id>`;
# MAGIC GRANT USE SCHEMA ON SCHEMA `catalog`.`schema` TO `<sp-application-id>`;
# MAGIC GRANT SELECT, MODIFY ON TABLE `catalog`.`schema`.`events` TO `<sp-application-id>`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: [02-sessionize]($./02-sessionize) - Read This Table, Deduplicate, Sessionize with transformWithState
