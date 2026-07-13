# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Personalization on Live Session State
# MAGIC
# MAGIC
# MAGIC Spark Real-Time Mode sessionizes live clickstreams, continuously updating active user state in Lakebase. A personalization service queries this fresh state via single-point lookups to serve next-best actions with low-millisecond latency.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/realtime-clickstream-serving/realtime-personalization-architecture.png" width="100%" alt="Real-time personalization on live session state"/>

# COMMAND ----------

# DBTITLE 1,Demo Configuration (Set These, Then Run All)
# Set widgets below before running all cells. Sliding-cohort overlap 
# approximates active concurrency at ~1.5–2x the base cohort size.
dbutils.widgets.text("lakebase_instance", "clickstream-sessions",         "Lakebase instance name")
dbutils.widgets.text("lakebase_db",       "databricks_postgres",          "Lakebase database name")
dbutils.widgets.text("lakebase_schema",   "live",                         "Lakebase schema name")
dbutils.widgets.text("lakebase_table",    "sessions",                     "Lakebase table name")
dbutils.widgets.text("total_users",       "200",                          "Total users (universe)")
dbutils.widgets.text("concurrent_users",  "120",                          "Concurrent users (live, approx)")
dbutils.widgets.text("events_per_second", "50",                           "Events per second")
dbutils.widgets.dropdown("reset_data",    "true", ["true", "false"],      "Reset data on run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Mode & Lakebase
# MAGIC
# MAGIC **Ideal Use Cases:**
# MAGIC
# MAGIC - Sub-Second SLAs: Applications requiring real-time response to user behavior (e.g., instant personalization, inline fraud mitigation, immediate churn intervention).
# MAGIC - Unified Pipelines: Architectures that consolidate cold-path warehouse ingestion and hot-path processing from a single event stream (Kafka, Kinesis, Event Hubs).
# MAGIC - Point Lookups: Read patterns limited to low-latency key-value queries for specific entity states rather than full-table scans.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Clickstream Ingestion
# MAGIC
# MAGIC This architecture utilizes Spark's built-in rate source to synthesize live event data. Because Real-Time Mode natively supports the rate source, the pipeline executes over a true low-latency continuous path rather than a simulated micro-batch framework.
# MAGIC
# MAGIC - Event Schema: Each payload captures two core behavioral dimensions: surface (application boundary context) and action (user interaction).
# MAGIC - Session Simulation: The pipeline implements a sliding user cohort to simulate realistic session lifecycles (concurrently opening, deepening, and closing sessions) to accurately test downstream personalization logic.
# MAGIC - Production Transition: To migrate to production, swap the rate source with the Kafka configuration lines provided at the bottom of the cell.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "databricks-sdk>=0.102.0" "protobuf==5.29.5" "psycopg[binary]==3.2.9"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Reset for a Clean Run (Clear Checkpoint + Truncate the Live Table)
# Clean initialization requires purging the checkpoint directory and target table.
# Set 'reset_data=false' via the configuration widget to preserve state across runs.

if dbutils.widgets.get("reset_data") == "true":
    import uuid as _uuid, psycopg as _pg
    from databricks.sdk import WorkspaceClient as _WC
    _chkpt = f"/Volumes/{catalog}/{db}/_checkpoints/realtime_sessions"
    try:
        dbutils.fs.rm(_chkpt, recurse=True)
        print(f"[reset] cleared checkpoint {_chkpt}")
    except Exception as _e:
        print(f"[reset] checkpoint clear skipped ({_e})")
    _w0 = _WC()
    _i0 = _w0.database.get_database_instance(name=lakebase_instance)
    _c0 = _w0.database.generate_database_credential(
        request_id=str(_uuid.uuid4()), instance_names=[lakebase_instance])
    _cn = _pg.connect(host=_i0.read_write_dns, dbname=lakebase_db,
                      user=_w0.current_user.me().user_name, password=_c0.token,
                      sslmode="require", connect_timeout=30)
    _cn.autocommit = True
    with _cn.cursor() as _cur:
        _cur.execute(f"TRUNCATE TABLE {lakebase_schema}.{lakebase_table}")
    _cn.close()
    print(f"[reset] truncated {lakebase_schema}.{lakebase_table}")

# COMMAND ----------

import pyspark.sql.functions as F

# Parse configuration widgets. Concurrency is modeled via sliding-window overlap:
# Concurrency ~= ACTIVE_COHORT * (session_gap / cohort_slide).
# With a fixed 30s gap and 15s slide (2x multiplier), ACTIVE_COHORT is sized at 50% of the target.
USER_POOL_SIZE     = int(dbutils.widgets.get("total_users"))
ROWS_PER_SECOND    = int(dbutils.widgets.get("events_per_second"))
COHORT_SLIDE_SEC   = 15   # How often the live cohort rotates forward (inactivity gap below is 30s)
NUM_PARTITIONS     = 4    # Rate-source partitions - raise with worker count for scale tests
_target_concurrent = int(dbutils.widgets.get("concurrent_users"))
ACTIVE_COHORT      = max(2, min(_target_concurrent // 2, USER_POOL_SIZE))
if ACTIVE_COHORT >= USER_POOL_SIZE:
    print(f"[config] concurrent_users is high relative to total_users - cohort clamped to "
          f"{ACTIVE_COHORT} of {USER_POOL_SIZE}. Raise total_users for real session turnover.")
print(f"[config] total_users={USER_POOL_SIZE}  concurrent~{_target_concurrent} "
      f"(cohort={ACTIVE_COHORT}, slide={COHORT_SLIDE_SEC}s, gap=30s)  events/s={ROWS_PER_SECOND}")

# Abstract, domain-agnostic taxonomy featuring a funnel-shaped probability distribution.
# High upper-funnel frequency ensures a realistic, multi-state user distribution (browsing/engaged/converting)
# and prevents artificial bottom-of-funnel saturation.
SURFACES = (["home"] * 5 + ["search"] * 4 + ["catalog"] * 4 + ["feature_x"] * 3
            + ["account"] * 2 + ["pricing"] * 2 + ["support"] * 1 + ["checkout"] * 1)
ACTIONS  = ["view", "click", "search", "submit"]

source_events = (
  spark.readStream
    .format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .option("numPartitions", NUM_PARTITIONS)
    .load()
    # Shifts the active user ID pool forward every COHORT_SLIDE_SEC to rotate the cohort.
    # Users remain active for ~30s before dropping out and exceeding the inactivity timeout.
    # This deterministic churn simulates a realistic distribution of concurrent session opens and closes.
    .withColumn("win",         (F.unix_timestamp(F.col("timestamp")) / F.lit(COHORT_SLIDE_SEC)).cast("long"))
    .withColumn("cohort_base", (F.col("win") * F.lit(ACTIVE_COHORT // 2)) % F.lit(USER_POOL_SIZE))
    .withColumn("user_id",     F.concat(F.lit("user-"),
                  (((F.col("cohort_base") + (F.col("value") % F.lit(ACTIVE_COHORT))) % F.lit(USER_POOL_SIZE)).cast("string"))))
    .withColumn("event_id",    F.expr("uuid()"))
    .withColumn("event_date",  F.unix_timestamp(F.col("timestamp")))
    # surface = where the user is, action = what they did. Decorrelated so a user moves across surfaces
    # rather than tracking action one-to-one.
    .withColumn("surface",     F.element_at(F.array(*[F.lit(s) for s in SURFACES]), ((F.col("value") % F.lit(len(SURFACES))) + 1).cast("int")))
    .withColumn("action",      F.element_at(F.array(*[F.lit(a) for a in ACTIONS]),  (((F.col("value") / F.lit(3)).cast("long") % F.lit(len(ACTIONS))) + 1).cast("int")))
    .select("user_id", "event_id", "event_date", "surface", "action")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## State Architecture: transformWithState Sessionization
# MAGIC
# MAGIC The stream ingestion layer leverages transformWithState to materialize a continuous, per-user state object consumed by low-latency personalization services.
# MAGIC
# MAGIC Session State Schema
# MAGIC - current_surface (String): Current application runtime boundary context.
# MAGIC - funnel_stage (String): Derived behavioral phase (browsing | engaged | converting).
# MAGIC - engagement_score (Integer): Weighted activity metric bounded from 0 to 100.
# MAGIC - needs_help (Boolean): Flag triggered by active support surface interaction.
# MAGIC
# MAGIC To meet sub-second SLAs, Spark Real-Time Mode introduces specific runtime behaviors distinct from micro-batching:
# MAGIC
# MAGIC - Non-Vectorized Ingestion: Data is processed row-by-row rather than via batch pandas structures.
# MAGIC - Wall-Clock Eviction: Session boundaries are managed via processing-time timers. Sessions undergo state eviction after 30 seconds of absolute wall-clock inactivity.
# MAGIC
# MAGIC This architecture encapsulates state maintenance entirely within the data pipeline, decoupling it from the target application's decision engine.

# COMMAND ----------

from typing import Iterator
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, BooleanType

# A session closes after this much inactivity. 
SESSION_GAP_MS = 30 * 1000


def _derive(click_count, current_surface):
  # Live snapshot: stage and needs_help come from the user's current surface, not history. A model
  # could slot in here in place of these rules.
  if current_surface == "checkout":
    stage = "converting"
  elif current_surface in ("pricing", "account"):
    stage = "engaged"
  else:
    stage = "browsing"
  needs_help = (current_surface == "support")
  bonus = 40 if stage == "converting" else (20 if stage == "engaged" else 0)
  score = min(100, int(click_count) * 2 + bonus)
  return stage, int(score), needs_help


class SessionProcessor(StatefulProcessor):
  def init(self, handle: StatefulProcessorHandle) -> None:
    # One ValueState struct per user: running counts plus the surface they are on right now.
    self.handle = handle
    state_schema = StructType([
      StructField("click_count",     LongType(),   True),
      StructField("start_time",      LongType(),   True),
      StructField("end_time",        LongType(),   True),
      StructField("current_surface", StringType(), True),
    ])
    self.session = handle.getValueState("session", state_schema)

  def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
    # Called per user per micro-batch. Under Real-Time Mode the rows arrive one at a time, but folding
    # them into the running session looks the same either way.
    (user_id,) = key

    if self.session.exists():
      click_count, start_time, end_time, current_surface = self.session.get()
    else:
      click_count, start_time, end_time, current_surface = 0, None, None, None

    for ev in rows:
      ts = ev["event_date"]
      start_time = ts if start_time is None else min(start_time, ts)
      end_time   = ts if end_time   is None else max(end_time, ts)
      click_count += 1
      current_surface = ev["surface"]                 # where they are right now

    if end_time is None:
      return

    self.session.update((click_count, start_time, end_time, current_surface))

    # Re-arm the processing-time inactivity timer. Clear the old one first so a user with frequent events
    # does not stack timers. If nothing arrives for SESSION_GAP_MS, handleExpiredTimer fires and closes the session.
    for t in self.handle.listTimers():
      self.handle.deleteTimer(t)
    self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + SESSION_GAP_MS)

    stage, score, needs_help = _derive(click_count, current_surface)
    # last_updated is emitted on every update, so the Postgres row always reflects current freshness.
    yield Row(
      user_id=user_id,
      click_count=int(click_count),
      start_time=int(start_time),
      end_time=int(end_time),
      status="online",
      current_surface=current_surface,
      funnel_stage=stage,
      engagement_score=score,
      needs_help=needs_help,
      last_updated=datetime.fromtimestamp(int(end_time), tz=timezone.utc),
    )

  def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
    # The close path: fires SESSION_GAP_MS of wall-clock after the user's last event. Emit the final
    # "offline" row and drop the state for that user.
    (user_id,) = key
    if not self.session.exists():
      return
    click_count, start_time, end_time, current_surface = self.session.get()
    self.session.clear()
    stage, score, needs_help = _derive(click_count, current_surface)
    yield Row(
      user_id=user_id,
      click_count=int(click_count),
      start_time=int(start_time),
      end_time=int(end_time),
      status="offline",
      current_surface=current_surface,
      funnel_stage=stage,
      engagement_score=score,
      needs_help=needs_help,
      last_updated=datetime.fromtimestamp(int(end_time), tz=timezone.utc),
    )

  def close(self) -> None:
    pass


output_schema = StructType([
  StructField("user_id",          StringType(),    False),
  StructField("click_count",      LongType(),      False),
  StructField("start_time",       LongType(),      False),
  StructField("end_time",         LongType(),      False),
  StructField("status",           StringType(),    False),
  StructField("current_surface",  StringType(),    True),
  StructField("funnel_stage",     StringType(),    False),
  StructField("engagement_score", LongType(),      False),
  StructField("needs_help",       BooleanType(),   False),
  StructField("last_updated",     TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Egress: Real-Time Mode & Lakebase Sink
# MAGIC
# MAGIC The pipeline leverages the native PostgreSQL streaming sink (.writeStream.format("postgresql")), available in Databricks Runtime 18.3 and above.
# MAGIC
# MAGIC - Upsert Semantics: Executes an INSERT ... ON CONFLICT (user_id) DO UPDATE statement to apply atomic state updates directly to Lakebase.
# MAGIC - Target Routing: Resolves the Lakebase compute endpoint using the project.branch.endpoint resource path, mapping the upsertkey config to the table's user_id primary key.
# MAGIC - Stream Management: Natively handles connection credentials and fully supports low-latency Real-Time Mode execution triggers.

# COMMAND ----------

sessions = (
  source_events
    .groupBy("user_id")
    .transformWithState(
      statefulProcessor=SessionProcessor(),
      outputStructType=output_schema,
      outputMode="Update",                # emit only the changed users each batch (the sink upserts them)
      timeMode="ProcessingTime",          # Real-Time Mode supports processing-time timers only - the key RTM constraint
    )
)

# COMMAND ----------

# Lakebase endpoints resolve via the 'project.branch.endpoint' canonical naming scheme.
# For provisioned instances, 'project' maps to the instance name, defaulting to 'production.primary'.
sink_endpoint = f"{lakebase_instance}.production.primary"

# Enables Real-Time Mode for continuous, sub-second streaming instead of micro-batching.
# PySpark requires a duration string; this governs the checkpoint/metadata interval
# rather than processing latency. Use trigger(processingTime="5 seconds") for micro-batch fallback.
q = (
  sessions.writeStream
    .format("postgresql")
    .outputMode("update")
    .option("endpoint", sink_endpoint)
    .option("database", lakebase_db)
    .option("dbtable", f"{lakebase_schema}.{lakebase_table}")
    .option("upsertkey", "user_id")
    .option("checkpointLocation", f"/Volumes/{catalog}/{db}/_checkpoints/realtime_sessions")
    .trigger(realTime="5 minutes")
    .start()
)

print(f"Streaming query started: {q.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Live Session State
# MAGIC
# MAGIC Give the stream a few seconds to warm up (the first batch lands ~20-30 seconds after the query starts), then run, and re-run, the cell below. It connects straight to Lakebase Postgres and reads current per-user session state, exactly the point lookup an application would issue.
# MAGIC

# COMMAND ----------

import psycopg, uuid
from psycopg.rows import dict_row
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
inst = w.database.get_database_instance(name=lakebase_instance)
cred = w.database.generate_database_credential(
  request_id=str(uuid.uuid4()),
  instance_names=[lakebase_instance],
)
conn_string = (
  f"host={inst.read_write_dns} "
  f"dbname={lakebase_db} "
  f"user={w.current_user.me().user_name} "
  f"password={cred.token} "
  f"sslmode=require"
)


def next_best_action(row):
  # The personalization decision, computed from one Postgres row.
  if row["needs_help"]:
    return "Offer live support"
  if row["funnel_stage"] == "converting":
    return "Nudge to complete (assist or incentive)"
  if row["funnel_stage"] == "engaged":
    return f"Surface tailored content for '{row['current_surface']}'"
  return "Highlight popular feature / onboarding"


with psycopg.connect(conn_string, row_factory=dict_row) as conn:
  with conn.cursor() as cur:
    cur.execute(f"""
      SELECT
        COUNT(*) FILTER (WHERE status='online')               AS active_users,
        COUNT(*) FILTER (WHERE funnel_stage='converting')     AS converting,
        COUNT(*) FILTER (WHERE needs_help)                    AS needs_help,
        MAX(last_updated)                                     AS most_recent,
        now()                                                 AS server_now
      FROM {lakebase_schema}.{lakebase_table}
    """)
    summary = cur.fetchone()
    cur.execute(f"""
      SELECT user_id, current_surface, funnel_stage, engagement_score, needs_help, last_updated
      FROM {lakebase_schema}.{lakebase_table}
      WHERE status='online'
      ORDER BY engagement_score DESC, last_updated DESC
      LIMIT 8
    """)
    top = cur.fetchall()

if not summary["most_recent"]:
  print("no rows yet - give the stream another few seconds, then re-run")
else:
  fresh = (summary["server_now"] - summary["most_recent"]).total_seconds()
  print(f"active_users={summary['active_users']}  converting={summary['converting']}  needs_help={summary['needs_help']}")
  print(f"freshest session updated {fresh:.1f}s ago")
  print()
  print("highest-engagement live sessions, and the action the engine would serve:")
  for r in top:
    print(f"  {r['user_id']:>9}  on={r['current_surface']:<9}  {r['funnel_stage']:<11}  score={r['engagement_score']:>3}  ->  {next_best_action(r)}")

# COMMAND ----------

# DBTITLE 1,Keep the Stream Alive When Run as a Job
# As a job task this blocks so the stream keeps serving the app until the run is cancelled.
# Interactively it returns immediately - the cluster keeps the stream alive after Run All.
if dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().isDefined():
  q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating Live Session State
# MAGIC
# MAGIC Allow the streaming query approximately 20–30 seconds to initialize and commit the first batch before executing the cell below.
# MAGIC
# MAGIC ### Execution Details
# MAGIC * **Direct Integration:** Establishes a direct connection to the Lakebase PostgreSQL instance.
# MAGIC * **Production Simulation:** Queries active, per-user session records, mirroring the exact low-latency point lookup pattern executed by downstream applications.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architectural Extensibility: Cross-Domain Adaptation
# MAGIC
# MAGIC The underlying stateful architecture is domain-agnostic. By remapping the primary entity boundaries and context signals, this streaming pattern seamlessly adapts to other industry verticals:
# MAGIC
# MAGIC | Vertical | Primary Entity | Operational Context ("Surface") | Targeted Next-Best Action |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | **Banking & Fintech** | Customer | Application Screen | Real-Time Promotion / Risk Hold |
# MAGIC | **Telecommunications** | Subscriber | Self-Service Workflow | Retention Nudge / Upsell Trigger |
# MAGIC | **Digital Gaming** | Player | Active Gameplay Mode | Matchmaking Queue / Anti-Cheat Flag |
# MAGIC | **Industrial IoT** | Device | Core Operating State | Telemetry Alert / Control Command |
# MAGIC
# MAGIC ### Infrastructure Invariance
# MAGIC The foundational storage and compute components—including the stateful processor, wall-clock timer eviction logic, and PostgreSQL target sink—remain identical. Scaling to a new business domain requires modifying only the schema projection layer that maps raw input events into the per-entity state vector.