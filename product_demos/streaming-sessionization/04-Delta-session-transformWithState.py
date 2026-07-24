# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)  4/ GOLD table: sessionize with the MODERN `transformWithState` API
# MAGIC
# MAGIC <img style="float:right; height: 250px; margin: 0px 30px 0px 30px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/session_diagram.png">
# MAGIC
# MAGIC In the previous notebook ([03-Delta-session-GOLD]($./03-Delta-session-GOLD)) we computed sessions with the **legacy `applyInPandasWithState`** operator.
# MAGIC
# MAGIC Databricks now recommends the **modern, general-purpose stateful API: `transformWithState`** (aka TWS). It's the replacement for `applyInPandasWithState` / `flatMapGroupsWithState` and gives you a clean, object-oriented `StatefulProcessor` with explicit state, timers and lifecycle hooks.
# MAGIC
# MAGIC We produce the **exact same sessionization result** (group clicks per user, close a session after a 30&nbsp;min inactivity gap) — just with the modern API.
# MAGIC
# MAGIC ### Why this notebook runs on SERVERLESS
# MAGIC
# MAGIC The legacy Kafka notebooks (`01`/`02`/`03`) rely on a continuous Kafka stream and can't run on serverless.<br/>
# MAGIC Here we deliberately read from the **existing `events` Delta table** (the Silver output). A Delta source runs as a micro-batch stream, so this modern path is **fully serverless-compatible** — no Kafka, no classic cluster required.
# MAGIC
# MAGIC | | `applyInPandasWithState` (notebook 03) | `transformWithState` (this notebook) |
# MAGIC |---|---|---|
# MAGIC | Status | Legacy | **Recommended** |
# MAGIC | API shape | single `func` + state/timeout enums | `StatefulProcessor` class (`init`/`handleInputRows`/`handleExpiredTimer`/`close`) |
# MAGIC | State | one opaque tuple | typed `ValueState` / `ListState` / `MapState` |
# MAGIC | Timers | single timeout mode | explicit `registerTimer` / `handleExpiredTimer` |
# MAGIC | State store | JVM memory | RocksDB (default on DBR 17.3+ & serverless) |
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=04-Delta-session-transformWithState&demo_name=streaming-sessionization&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ State store provider
# MAGIC
# MAGIC `transformWithState` requires the RocksDB state store provider. It's the **default on DBR 17.3+ and on serverless**, so the cell below is a no-op there and only matters if you backport this notebook to an older runtime. RocksDB spills state to disk instead of the JVM heap, avoiding GC pressure and letting state grow beyond executor memory.

# COMMAND ----------

try:
  spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
  )
except Exception as e:
  print(f"Skipping state store conf (DBR 17.3+ and serverless already default to RocksDB): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ The `StatefulProcessor`
# MAGIC
# MAGIC With `transformWithState` we implement a `StatefulProcessor`. Spark calls its lifecycle methods **by name** on the workers (the camelCase spellings are required):
# MAGIC
# MAGIC * `init()` — runs once per worker, declares the state we keep per key
# MAGIC * `handleInputRows()` — called per key (user) per micro-batch, with only that user's new rows
# MAGIC * `handleExpiredTimer()` — fires when a registered timer goes off: our "session closed" signal
# MAGIC * `close()` — teardown
# MAGIC
# MAGIC We keep one `ValueState` struct per `user_id` (the in-flight session: click count + start/end time) and re-arm an **event-time timer** at `last_event + 30 min`. When the watermark passes that timer, the session is closed and emitted as `offline`.

# COMMAND ----------

from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, LongType

# Same rule as notebook 03: close a session after 30 minutes of inactivity for a user.
SESSION_GAP_MS = 30 * 60 * 1000


class SessionProcessor(StatefulProcessor):

  def init(self, handle: StatefulProcessorHandle) -> None:
    # Runs once per worker. Declares the state we keep per key.
    # TWS offers ValueState (one value per key), ListState and MapState. One struct per user
    # (the in-flight session) is a ValueState. `state_schema` is the shape stored per key.
    self.handle = handle
    state_schema = StructType([
      StructField("click_count",   LongType(), True),   # events seen so far this session
      StructField("start_time_ms", LongType(), True),   # first event time, epoch milliseconds
      StructField("end_time_ms",   LongType(), True),   # latest event time, epoch ms; drives the inactivity timer
    ])
    self.session = handle.getValueState("session", state_schema)

  def handleInputRows(
    self, key: Tuple[str], rows: Iterator[pd.DataFrame], timerValues
  ) -> Iterator[pd.DataFrame]:
    # Called once per user per micro-batch with ONLY that user's new rows. In the *InPandas* variant
    # `rows` is an iterator of pandas DataFrames (batches), so we fold the whole batch into the session.
    (user_id,) = key

    # Load this user's existing session, or start a fresh one.
    if self.session.exists():
      click_count, start_ms, end_ms = self.session.get()
    else:
      click_count, start_ms, end_ms = 0, None, None

    # Fold the new events into the session window (earliest start, latest end, running count).
    # As we can receive out-of-order events, we track the min/max event time and the count.
    for pdf in rows:
      if pdf.empty:
        continue
      ts_ms = (pdf["event_datetime"].astype("int64") // 1_000_000).to_numpy()  # pandas nanoseconds -> milliseconds
      batch_min, batch_max = int(ts_ms.min()), int(ts_ms.max())
      start_ms = batch_min if start_ms is None else min(start_ms, batch_min)
      end_ms   = batch_max if end_ms   is None else max(end_ms,   batch_max)
      click_count += len(pdf)

    if end_ms is None:        # nothing usable for this key in this batch: emit nothing
      return

    # Persist the updated session for the next micro-batch.
    self.session.update((click_count, start_ms, end_ms))

    # Re-arm the idle timer in event time. Delete the old timer first so a user with frequent events
    # doesn't accumulate timers - we keep exactly one pending "close" per user.
    # registerTimer(end + gap) fires when the watermark passes (last event + 30 min).
    for t in self.handle.listTimers():
      self.handle.deleteTimer(t)
    self.handle.registerTimer(end_ms + SESSION_GAP_MS)

    # Emit the session's current ("online") state. With outputMode="Update" we emit only the changed
    # key each batch and the sink upserts it, so the sessions table always holds the latest per user.
    yield pd.DataFrame({
      "user_id":     [user_id],
      "click_count": [click_count],
      "start_time":  [start_ms // 1000],   # back to epoch seconds, matching notebook 03's sessions table
      "end_time":    [end_ms   // 1000],
      "status":      ["online"],
    })

  def handleExpiredTimer(
    self, key: Tuple[str], timerValues, expiredTimerInfo
  ) -> Iterator[pd.DataFrame]:
    # Fires when the watermark passes the registered timer, i.e. the user has been quiet for 30 min.
    # Because it's watermark-driven (event time, not wall clock) the closes are deterministic:
    # the same input always produces the same sessions.
    (user_id,) = key
    if not self.session.exists():
      return
    click_count, start_ms, end_ms = self.session.get()
    self.session.clear()      # the session is over - drop its state
    yield pd.DataFrame({
      "user_id":     [user_id],
      "click_count": [click_count],
      "start_time":  [start_ms // 1000],
      "end_time":    [end_ms   // 1000],
      "status":      ["offline"],
    })

  def close(self) -> None:
    # Teardown hook (close connections, flush buffers). Nothing to release here.
    pass


# The OUTPUT schema describes the rows the processor YIELDS (distinct from the state_schema in init).
# It matches the columns emitted in the two methods above, and the sessions table from notebook 03.
output_schema = "user_id STRING, click_count LONG, start_time LONG, end_time LONG, status STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Wiring the stream: dedup + sessionize
# MAGIC
# MAGIC We stream the **existing `events` Delta table** (Silver output). It already has an `event_datetime` timestamp, so we simply:
# MAGIC
# MAGIC 1. set a `withWatermark` bound on `event_datetime` — this bounds how long dedup state is kept and is the clock the event-time timers fire on,
# MAGIC 2. `dropDuplicatesWithinWatermark(['event_id'])` for bounded-state dedup,
# MAGIC 3. `groupBy('user_id').transformWithStateInPandas(...)` in `Update` output mode with `EventTime` timers.

# COMMAND ----------

import pyspark.sql.functions as F

DBDemos.wait_for_table("events")   # block until the Silver `events` table exists (produced by notebook 02)

events_stream = (
  spark.readStream.table("events")                 # Delta source -> micro-batch stream -> serverless friendly
    .where("event_id IS NOT NULL AND user_id IS NOT NULL")
    # The watermark bounds how late an event can arrive: it caps dedup state and is the clock the
    # event-time timers fire on. 1 hour here; raise it if your data arrives later.
    .withWatermark("event_datetime", "1 hour")
    # dropDuplicatesWithinWatermark removes duplicates inside the watermark window and frees that state
    # once the window passes, so it stays bounded over a long-running stream.
    .dropDuplicatesWithinWatermark(["event_id"])
)

sessions = (
  events_stream
    .groupBy("user_id")                            # state is partitioned by this key (one session per user)
    .transformWithStateInPandas(
      statefulProcessor=SessionProcessor(),        # the lifecycle object defined above
      outputStructType=output_schema,              # the shape of the rows it yields
      outputMode="Update",                         # emit only changed keys each batch (sessions are upserts, not an append log)
      timeMode="EventTime",                        # timers fire on the event-time watermark, not wall-clock time
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Delta sink: MERGE upserts into `sessions_tws`
# MAGIC
# MAGIC As with notebook 03, we upsert each session with a `MERGE` inside `foreachBatch`: a user's row flips between `online` and `offline` as their session evolves, so the table holds the **current** state per user (idempotent, not an append log).
# MAGIC
# MAGIC We write to a separate `sessions_tws` table so you can compare it side by side with the legacy `sessions` table from notebook 03.

# COMMAND ----------

from delta.tables import DeltaTable

def upsert_sessions(df, epoch_id):
  # Create the table on the first batch so the MERGE has a target.
  if not spark.catalog.tableExists('sessions_tws'):   # spark.catalog works on serverless/Spark Connect (_jsparkSession is JVM-only, unsupported there)
    df.limit(0).write.option('mergeSchema', 'true').mode('append').saveAsTable('sessions_tws')

  (DeltaTable.forName(spark, "sessions_tws").alias("s")
    .merge(source=df.alias("u"), condition="s.user_id = u.user_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

(sessions.writeStream
  .option("checkpointLocation", volume_folder+"/checkpoints/sessions_tws")
  .foreachBatch(upsert_sessions)
  .trigger(availableNow=True)   # process everything available then stop (so the demo build terminates).
                                # In production use a continuous trigger, e.g. .trigger(processingTime="20 seconds")
  .start()
  .awaitTermination())

DBDemos.wait_for_table("sessions_tws")

# COMMAND ----------

# MAGIC %sql SELECT * FROM sessions_tws

# COMMAND ----------

# MAGIC %sql SELECT CAST(avg(end_time - start_time) as INT) average_session_duration FROM sessions_tws

# COMMAND ----------

# MAGIC %md
# MAGIC ### That's it — modern sessionization, running on serverless!
# MAGIC
# MAGIC We produced the same sessions as notebook 03, but with the recommended `transformWithState` API, and because we read a Delta source it runs anywhere — including **serverless**.
# MAGIC
# MAGIC Want true **sub-second** latency instead of micro-batches? See the advanced (optional) notebook **[05-Real-Time-Mode-sessionization]($./05-Real-Time-Mode-sessionization)** which runs the same sessionization in Real-Time Mode.
