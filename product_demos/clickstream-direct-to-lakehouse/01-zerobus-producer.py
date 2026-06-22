# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Events Directly into Your Lakehouse with Zerobus
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/clickstream-direct-to-lakehouse/zerobus-delta-architecture.png" width="100%" alt="Clickstream events flow through Zerobus into a Delta events table, and Structured Streaming sessionizes them into a sessions table"/>
# MAGIC
# MAGIC Your application talks to Zerobus and records land directly in a Unity Catalog Delta table. No message bus, no separate Spark ingestion job.
# MAGIC
# MAGIC This notebook stands in for your application: it generates synthetic clickstream events and streams them through the Zerobus REST API into the `events` table from the previous notebook. In production the same client lives in your service or a thin sidecar - see the production note at the end.

# COMMAND ----------

# DBTITLE 1,Demo Configuration
dbutils.widgets.text("zerobus_client_id", "",                            "Zerobus service principal client_id (application_id)")
dbutils.widgets.text("duration_seconds",  "150",                         "Seconds to stream live events for")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ When to Use This Pattern
# MAGIC
# MAGIC - Production application events, clickstream, and IoT or device telemetry that need to land in the lakehouse continuously and stay queryable within seconds
# MAGIC - High-volume, variable-throughput streams where the lakehouse is the destination and you'd rather not run a broker plus a separate ingestion job
# MAGIC - Replacing custom S3-batch or file-drop pipelines (write JSON to S3, then COPY INTO) with direct ingestion
# MAGIC - Greenfield ingestion where reaching the lakehouse would otherwise mean standing up a message bus and a Spark consumer

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Authentication and One-Time Setup
# MAGIC
# MAGIC Zerobus authenticates with OAuth M2M using a service principal that holds `USE CATALOG`, `USE SCHEMA`, and `SELECT` and `MODIFY` on the target table. The `client_id` is the service principal's application_id, and the `client_secret` is read from a secret scope, so it never appears in the notebook. See [Secret management](https://docs.databricks.com/aws/en/security/secrets/).
# MAGIC
# MAGIC Setup steps:
# MAGIC 1. Create a service principal in your workspace and generate an OAuth secret for it (admin-only). Copy its application_id and the secret value.
# MAGIC 2. From your terminal, store the secret with the Databricks CLI ([install](https://docs.databricks.com/aws/en/dev-tools/cli/install), [authenticate](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)). `put-secret` prompts for the value:
# MAGIC    ```
# MAGIC    databricks secrets create-scope zerobus-demo
# MAGIC    databricks secrets put-secret zerobus-demo client-secret
# MAGIC    ```
# MAGIC 3. Put the application_id in the `zerobus_client_id` widget and Run All. The prerequisite cell grants the service principal the privileges Zerobus needs.

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
# MAGIC ## 3/ Wire Up the Zerobus Client
# MAGIC
# MAGIC Two things to assemble before we can write a record: the region-specific endpoint hostname, and an OAuth token Zerobus will accept. The next cell builds both.

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
# MAGIC ## 4/ Generate a Live Event Stream
# MAGIC
# MAGIC We emit **current-time** events for `duration_seconds`, with users joining and going idle so sessions open and close in real time. Start `02-sessionize` first, then run this, so the two stream side by side.

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
# MAGIC ## 5/ Stream Records In via the Zerobus REST API
# MAGIC
# MAGIC Each tick POSTs a small batch of JSON to `{endpoint}/zerobus/v1/tables/{catalog}.{schema}.{table}/insert`, and the records are queryable in the Delta table within seconds.

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
# MAGIC ## 6/ Verify the Data Landed

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
# MAGIC ## 7/ Production Note
# MAGIC
# MAGIC This notebook runs the producer for demo convenience. In production the Zerobus client can run in **one of three places**:
# MAGIC
# MAGIC 1. **Inside your application code.** Link the SDK directly. Most direct, but your application now owns SDK upgrades, credentials, and retry logic.
# MAGIC 2. **As a thin sidecar service.** Your application POSTs HTTP/JSON to a small forwarder (a few hundred lines) that is the Zerobus client, so your application code stays clean.
# MAGIC 3. **As a consumer of an existing transport.** If you already have events on Kafka or Kinesis and want to migrate off, run a consumer that forwards via Zerobus. Useful as a bridge.
# MAGIC
# MAGIC Whichever you pick, authenticate with a **service principal** via OAuth M2M, not a notebook user. It needs `USE CATALOG`, `USE SCHEMA`, and `SELECT` and `MODIFY` on the events table:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog TO `<sp-application-id>`;
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog.schema TO `<sp-application-id>`;
# MAGIC GRANT SELECT, MODIFY ON TABLE catalog.schema.events TO `<sp-application-id>`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: [02-sessionize]($./02-sessionize) - Read This Table, Deduplicate, Sessionize with transformWithState
