# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)  5/ [ADVANCED] Sub-second sessions with **Real-Time Mode**
# MAGIC
# MAGIC <img style="float:right; height: 250px; margin: 0px 30px 0px 30px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/session_diagram.png">
# MAGIC
# MAGIC ## ⚠️ This is an ADVANCED / OPTIONAL notebook — CLASSIC compute only
# MAGIC
# MAGIC Notebook [04-Delta-session-transformWithState]($./04-Delta-session-transformWithState) sessionizes in **micro-batches** (latency of a few seconds) and runs on serverless — that's the right choice for almost every use case.
# MAGIC
# MAGIC **For true sub-second latency**, Spark **Real-Time Mode (RTM)** runs the *same* `transformWithState` sessionization continuously instead of in micro-batches.
# MAGIC
# MAGIC > **Read before you run.** RTM requires a **continuously-running classic cluster** (no autoscale, no Photon, no spot instances) and is **expensive** — the cluster runs 24/7 to keep latency low. **Don't use RTM unless you have a real sub-second SLA.** For everything else, use notebook 04.
# MAGIC
# MAGIC ### Cluster requirements (classic only — this will NOT run on serverless)
# MAGIC
# MAGIC * **DBR 16.4 LTS minimum** (18.1+ recommended)
# MAGIC * **Classic** compute in **Dedicated** or **Standard** access mode (RTM is not available on serverless)
# MAGIC * `spark.databricks.streaming.realTimeMode.enabled true` in the cluster Spark config
# MAGIC * **Photon OFF**
# MAGIC * **Autoscaling OFF** (fixed number of workers) — a continuously running query needs stable executors
# MAGIC * No spot instances (use on-demand for a long-lived cluster)
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=05-Real-Time-Mode-sessionization&demo_name=streaming-sessionization&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Source & sink constraints for RTM
# MAGIC
# MAGIC RTM has a restricted set of supported sources and sinks. Notably **Delta is not a supported RTM source or sink** — the continuous, low-latency path can't sit on top of a batch-oriented Delta table.
# MAGIC
# MAGIC For this demo we therefore use:
# MAGIC
# MAGIC * **Source: the built-in `rate` source** — the documented "demos" source for RTM. We shape its synthetic rows into `(user_id, event_id, event_datetime, action)` so it mirrors our clickstream story (in production you'd swap this for Kafka / Kinesis / Event Hubs, which RTM supports).
# MAGIC * **Sink: a custom `foreach` sink** — here it just prints the closed/updated sessions, standing in for a low-latency store (a key-value store, Lakebase, a cache, etc.). This mirrors the point-lookup pattern a personalization or fraud service would use.

# COMMAND ----------

import pyspark.sql.functions as F

# Same 30-min inactivity rule as notebooks 03/04, but timed on the wall clock (see below).
SESSION_GAP_MS = 30 * 60 * 1000

# Synthesize a live clickstream with the rate source. We rotate a sliding cohort of user ids so that
# sessions realistically open, deepen and close over time, and we shape the rows to match our story:
# (user_id, event_id, event_datetime, action).
USER_POOL_SIZE   = 50    # size of the user universe
ROWS_PER_SECOND  = 20    # synthetic events per second
COHORT_SLIDE_SEC = 15    # how often the active user cohort rotates forward
ACTIVE_COHORT    = 20    # how many users are active at once
ACTIONS          = ["view", "click", "add_to_cart", "checkout"]

source_events = (
  spark.readStream
    .format("rate")                                   # RTM natively supports the rate source
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .option("numPartitions", 4)
    .load()
    # Slide the active user-id pool forward every COHORT_SLIDE_SEC so users drop out and new ones appear,
    # producing a realistic mix of concurrent session opens and closes.
    .withColumn("win",         (F.unix_timestamp(F.col("timestamp")) / F.lit(COHORT_SLIDE_SEC)).cast("long"))
    .withColumn("cohort_base", (F.col("win") * F.lit(ACTIVE_COHORT // 2)) % F.lit(USER_POOL_SIZE))
    .withColumn("user_id",     F.concat(F.lit("user-"),
                  (((F.col("cohort_base") + (F.col("value") % F.lit(ACTIVE_COHORT))) % F.lit(USER_POOL_SIZE)).cast("string"))))
    .withColumn("event_id",       F.expr("uuid()"))
    .withColumn("event_datetime", F.col("timestamp"))                                       # rate source gives us a timestamp
    .withColumn("action",         F.element_at(F.array(*[F.lit(a) for a in ACTIONS]),
                                    ((F.col("value") % F.lit(len(ACTIONS))) + 1).cast("int")))
    .select("user_id", "event_id", "event_datetime", "action")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ The `StatefulProcessor` (same concept as notebook 04)
# MAGIC
# MAGIC It's the same sessionization: one `ValueState` struct per `user_id`, re-armed timer, close on inactivity. **The one RTM-specific change is the timer:**
# MAGIC
# MAGIC * Notebook 04 (micro-batch): **event-time** timers, driven by the watermark (`timeMode="EventTime"`).
# MAGIC * RTM (this notebook): **processing-time** timers, driven by the wall clock (`timeMode="ProcessingTime"`) — RTM supports processing-time timers only. We re-arm the timer at `now + 30 min` using `timerValues.getCurrentProcessingTimeInMs()`.
# MAGIC
# MAGIC Under RTM rows also arrive **one at a time** (non-vectorized) rather than in pandas batches, so we use the row-oriented `transformWithState` and fold each event individually.

# COMMAND ----------

from typing import Iterator
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, LongType, StringType


class SessionProcessor(StatefulProcessor):

  def init(self, handle: StatefulProcessorHandle) -> None:
    # One ValueState struct per user: the in-flight session (running count + first/last event time in ms).
    self.handle = handle
    state_schema = StructType([
      StructField("click_count",   LongType(), True),
      StructField("start_time_ms", LongType(), True),
      StructField("end_time_ms",   LongType(), True),
    ])
    self.session = handle.getValueState("session", state_schema)

  def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
    # Called per user. Under RTM rows arrive one at a time, but folding them into the running session
    # looks identical to the micro-batch case.
    (user_id,) = key

    if self.session.exists():
      click_count, start_ms, end_ms = self.session.get()
    else:
      click_count, start_ms, end_ms = 0, None, None

    for ev in rows:
      ts_ms = int(ev["event_datetime"].timestamp() * 1000)
      start_ms = ts_ms if start_ms is None else min(start_ms, ts_ms)
      end_ms   = ts_ms if end_ms   is None else max(end_ms,   ts_ms)
      click_count += 1

    if end_ms is None:
      return

    self.session.update((click_count, start_ms, end_ms))

    # Re-arm the PROCESSING-TIME inactivity timer (wall clock). Clear the old one first so frequent
    # events don't stack timers. If nothing arrives for 30 min, handleExpiredTimer fires and closes it.
    for t in self.handle.listTimers():
      self.handle.deleteTimer(t)
    self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + SESSION_GAP_MS)

    # Emit the current ("online") session. outputMode="update" emits only the changed users each batch.
    yield Row(
      user_id=user_id,
      click_count=int(click_count),
      start_time=int(start_ms // 1000),
      end_time=int(end_ms // 1000),
      status="online",
    )

  def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
    # Fires 30 min of wall-clock after the user's last event: emit the final "offline" row, drop the state.
    (user_id,) = key
    if not self.session.exists():
      return
    click_count, start_ms, end_ms = self.session.get()
    self.session.clear()
    yield Row(
      user_id=user_id,
      click_count=int(click_count),
      start_time=int(start_ms // 1000),
      end_time=int(end_ms // 1000),
      status="offline",
    )

  def close(self) -> None:
    pass


output_schema = StructType([
  StructField("user_id",     StringType(), False),
  StructField("click_count", LongType(),   False),
  StructField("start_time",  LongType(),   False),
  StructField("end_time",    LongType(),   False),
  StructField("status",      StringType(), False),
])

sessions = (
  source_events
    .groupBy("user_id")
    .transformWithState(
      statefulProcessor=SessionProcessor(),
      outputStructType=output_schema,
      outputMode="update",                # emit only the changed users each batch
      timeMode="ProcessingTime",          # RTM supports processing-time timers only - the key RTM constraint
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Real-Time Mode sink
# MAGIC
# MAGIC We start the query with **`.trigger(realTime="5 minutes")`**. Two things to note:
# MAGIC
# MAGIC * `realTime=...` switches the query into Real-Time Mode. The duration is the **checkpoint / metadata interval**, *not* how often the query fires — RTM processes continuously, sub-second.
# MAGIC * The sink is a custom **`foreach`** sink (not Delta — Delta is not a supported RTM sink). Here it prints the changed sessions; in production you'd write to a low-latency store (a key-value store, Lakebase Postgres, a cache) that your app point-looks-up.

# COMMAND ----------

# A minimal foreach sink standing in for a low-latency store. In production, open a connection to your
# key-value store / Lakebase / cache in open(), upsert each session row in process(), and close it in close().
def process_session(row):
  print(f"[session] user={row.user_id:>8}  status={row.status:<7}  clicks={row.click_count:>3}  "
        f"start={row.start_time}  end={row.end_time}")

q = (
  sessions.writeStream
    .outputMode("update")
    .foreach(process_session)                                        # custom sink (NOT Delta - unsupported under RTM)
    .option("checkpointLocation", volume_folder+"/checkpoints/sessions_rtm")
    .trigger(realTime="5 minutes")                                   # <-- enables Real-Time Mode (checkpoint interval, not fire rate)
    .start()
)

print(f"Real-Time Mode streaming query started: {q.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Give the query a few seconds to warm up, then watch the driver logs / the cell above stream the `online` / `offline` session updates in near real time.
# MAGIC
# MAGIC As a job task, block on the query so the stream keeps serving; interactively it returns immediately and the cluster keeps the stream alive after *Run All*.

# COMMAND ----------

if dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().isDefined():
  q.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Stop the stream
DBDemos.stop_all_streams(sleep_time=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### When should I actually use Real-Time Mode?
# MAGIC
# MAGIC | | Micro-batch (notebook 04) | Real-Time Mode (this notebook) |
# MAGIC |---|---|---|
# MAGIC | Latency | seconds | **sub-second** |
# MAGIC | Compute | serverless or classic | **classic only**, continuously running |
# MAGIC | Cost | pay per trigger | **expensive** (cluster runs 24/7) |
# MAGIC | Source/sink | Delta OK | Kafka/Kinesis/rate in, custom sink out (**no Delta**) |
# MAGIC | Timers | event-time | processing-time only |
# MAGIC | Use it when… | almost always | you have a **real sub-second SLA** (instant personalization, inline fraud, real-time churn intervention) |
# MAGIC
# MAGIC The stateful logic is identical — RTM only changes *how fast* and *where* it runs. Start with notebook 04, and reach for RTM only when the SLA demands it.
