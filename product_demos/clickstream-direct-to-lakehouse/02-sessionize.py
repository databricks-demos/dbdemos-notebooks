# Databricks notebook source
# MAGIC %md
# MAGIC # Sessionize the Events with `transformWithState`
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/clickstream-direct-to-lakehouse/zerobus-delta-architecture.png" width="100%" alt="Clickstream events flow through Zerobus into a Delta events table, and Structured Streaming sessionizes them into a sessions table"/>
# MAGIC
# MAGIC This pipeline implements real-time clickstream sessionization using Structured Streaming's [`transformWithState`](https://docs.databricks.com/aws/en/stateful-applications/) (TWS) API.
# MAGIC
# MAGIC - Ingestion: Consumes pre-structured rows directly from a Delta events table.
# MAGIC - Deduplication: Filters at-least-once delivery duplicates using bounded event-time watermarks.
# MAGIC - Stateful Processing: Aggregates events per user and terminates sessions after a configurable inactivity gap (default: 60 seconds).
# MAGIC - Sink: Performs incremental MERGE upserts into a downstream Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC Each user is sessionized independently on their own clicks. The same idle-gap rule gives each a different set of sessions:
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/clickstream-direct-to-lakehouse/sessionization-timeline.png" width="100%" alt="Three users on a time axis; each user's clicks form session boxes that close after an idle gap, with one user still online"/>

# COMMAND ----------

# DBTITLE 1,Demo Configuration
dbutils.widgets.text("session_gap_seconds", "60",                          "Close a session after this many idle seconds")

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ State Store Provider
# MAGIC
# MAGIC `transformWithState` requires the RocksDB state store provider (`RocksDBStateStoreProvider`). While natively enabled by default on DBR 17.3+, older runtimes require explicit configuration. RocksDB eliminates JVM garbage collection (GC) overhead by caching state on disk, allowing scale-out behavior beyond available executor memory.

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
# MAGIC ## 2/ The StatefulProcessor
# MAGIC
# MAGIC `transformWithState` is Spark's general-purpose stateful operator for complex logic where native windowed aggregations are insufficient. The `StatefulProcessor` manages keyed state, timers, and fault-tolerant checkpointing.
# MAGIC
# MAGIC This implementation tracks one active `ValueState` struct per `user_id` containing:
# MAGIC *   `click_count` (Long)
# MAGIC *   `start_time_ms` (Long)
# MAGIC *   `end_time_ms` (Long)
# MAGIC
# MAGIC An event-time timer triggers session emission and clears state once the watermark passes the inactivity threshold (`session_gap_seconds`).
# MAGIC
# MAGIC ### Key Conventions
# MAGIC *   **Lifecycle Methods:** `init`, `handleInputRows`, and `handleExpiredTimer` are invoked by name via the Spark engine and must strictly use camelCase syntax.
# MAGIC *   **Schema Separation:** The internal state storage schema is declared independently of the output emission schema.

# COMMAND ----------

from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, LongType
import pyspark.sql.functions as F

# Idle gap: a session closes once this many seconds pass with no new events for a user.
SESSION_GAP_MS = int(dbutils.widgets.get("session_gap_seconds")) * 1000


# Spark runs this on the workers through a fixed lifecycle, calling each method by name (the
# camelCase spellings are required):
#   init()               - runs once per worker, declares state
#   handleInputRows()    - per key, per micro-batch, handed that key's new rows
#   handleExpiredTimer() - fires when a registered timer goes off (the "session closed" signal)
#   close()              - teardown
# State is keyed by the groupBy column (user_id), lives in the RocksDB state store, and is
# checkpointed, so it survives across micro-batches and failures.
class SessionProcessor(StatefulProcessor):

  def init(self, handle: StatefulProcessorHandle) -> None:
    # Runs once when the processor starts on a worker and declares all state.
    # TWS offers three state types: ValueState (one value per key), ListState, and MapState. One
    # struct per user (the in-flight session) needs ValueState.
    # `state_schema` is the shape of what is stored per key.
    self.handle = handle
    state_schema = StructType([
      StructField("click_count",   LongType(), True),   # events seen so far this session
      StructField("start_time_ms", LongType(), True),   # first event time, in epoch milliseconds
      StructField("end_time_ms",   LongType(), True),   # latest event time, epoch ms, drives the inactivity timer
    ])
    self.session = handle.getValueState("session", state_schema)

  def handleInputRows(
    self, key: Tuple[str], rows: Iterator[pd.DataFrame], timerValues
  ) -> Iterator[pd.DataFrame]:
    # Called once per key (user) per micro-batch, with ONLY that user's new rows.
    # In the *InPandas* variant, `rows` is an iterator of pandas DataFrames (batches), not
    # single rows - so we fold the whole batch into the running session state.
    (user_id,) = key

    # Load this user's existing session from state, or start a fresh one if they have none.
    if self.session.exists():
      click_count, start_ms, end_ms = self.session.get()
    else:
      click_count, start_ms, end_ms = 0, None, None

    # Fold the new events into the session window (earliest start, latest end, running count).
    for pdf in rows:
      if pdf.empty:
        continue
      ts_ms = (pdf["event_datetime"].astype("int64") // 1_000_000).to_numpy()  # pandas nanoseconds to milliseconds
      batch_min, batch_max = int(ts_ms.min()), int(ts_ms.max())
      start_ms = batch_min if start_ms is None else min(start_ms, batch_min)
      end_ms   = batch_max if end_ms   is None else max(end_ms,   batch_max)
      click_count += len(pdf)

    if end_ms is None:        # nothing usable for this key in this batch, so emit nothing
      return

    # Persist the updated session back to the state store for the next micro-batch.
    self.session.update((click_count, start_ms, end_ms))

    # Re-arm the idle timer in event time. Delete the old timer first so a user with frequent
    # events does not accumulate timers - one pending "close" per user.
    # registerTimer(end + gap) fires when the watermark passes (last event + gap).
    for t in self.handle.listTimers():
      self.handle.deleteTimer(t)
    self.handle.registerTimer(end_ms + SESSION_GAP_MS)

    # Emit the session's current ("online") state. With outputMode="Update" we emit just the
    # changed key each batch, and the sink upserts it - so the sessions table always holds the
    # latest state per user. We convert ms back to epoch seconds for the output columns.
    yield pd.DataFrame({
      "user_id":        [user_id],
      "click_count":    [click_count],
      "start_time":     [start_ms // 1000],
      "end_time":       [end_ms   // 1000],
      "status":         ["online"],
      "event_datetime": [pd.Timestamp(end_ms, unit="ms", tz="UTC")],
    })

  def handleExpiredTimer(
    self, key: Tuple[str], timerValues, expiredTimerInfo
  ) -> Iterator[pd.DataFrame]:
    # Fires when the watermark passes the registered timer, ie the user has been quiet for the gap.
    # Closes the session. Because it is watermark-driven (event time, not wall clock), the closes are
    # deterministic - the same input produces the same result.
    (user_id,) = key
    if not self.session.exists():
      return
    click_count, start_ms, end_ms = self.session.get()
    self.session.clear()      # the session is over - drop its state
    # Emit the final "offline" record. The sink upserts the user's row to closed.
    yield pd.DataFrame({
      "user_id":        [user_id],
      "click_count":    [click_count],
      "start_time":     [start_ms // 1000],
      "end_time":       [end_ms   // 1000],
      "status":         ["offline"],
      "event_datetime": [pd.Timestamp(end_ms, unit="ms", tz="UTC")],
    })

  def close(self) -> None:
    # Lifecycle hook for teardown (close connections, flush buffers). Nothing to release here.
    pass


# The OUTPUT schema describes the rows the processor YIELDS - distinct from the state_schema in
# init(). It must line up with the columns and types we emit in the two methods above.
output_schema = (
  "user_id STRING, click_count LONG, start_time LONG, end_time LONG, "
  "status STRING, event_datetime TIMESTAMP"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Stream Wiring: Deduplication & Sessionization
# MAGIC
# MAGIC Prior to stateful tracking, the stream converts epoch integer fields to explicit timestamps to establish an event-time column. A 30-second `withWatermark` bound is applied, enabling `dropDuplicatesWithinWatermark` to execute bounded-state deduplication on `event_id`.
# MAGIC
# MAGIC The deduplicated stream is then partitioned by `user_id` and processed via `transformWithStateInPandas` using `Update` output mode.

# COMMAND ----------

DBDemos.wait_for_table("events")   # block until `events` exists and the producer has landed rows

events_stream = (
  spark.readStream.table("events")                                   # stream the Zerobus-fed Delta table
    .where("event_id IS NOT NULL AND event_id != ''")                # drop the ~2% synthetic blank event_ids
    .withColumn("event_datetime",                                    # turn epoch seconds into a real timestamp,
                F.to_timestamp(F.from_unixtime(F.col("event_date"))))  # so Spark has an event-time column to work with
    # The watermark sets how late an event can arrive. It bounds how long Spark keeps deduplication
    # state, and it is the clock the event-time timers fire on. 30 seconds here, raise it if your
    # data arrives later.
    .withWatermark("event_datetime", "30 seconds")
    # dropDuplicatesWithinWatermark deduplicates inside the watermark window and frees that state once
    # the window passes, so it stays bounded over a long-running stream.
    .dropDuplicatesWithinWatermark(["event_id"])
)

sessions = (
  events_stream
    .groupBy("user_id")                          # state is partitioned by this key (one session per user)
    .transformWithStateInPandas(
      statefulProcessor=SessionProcessor(),      # the lifecycle object defined above
      outputStructType=output_schema,            # the shape of the rows it yields
      outputMode="Update",                       # emit only changed keys each batch (sessions are upserts, not an append log)
      timeMode="EventTime",                      # timers fire on the event-time watermark, not wall-clock time
      # eventTimeColumnName is intentionally unset. A closing session emits its last-event time, which
      # sits behind the current watermark, so declaring an output event-time column would conflict with
      # it. The timers fire off the input watermark and nothing downstream needs an output watermark.
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Delta Sink: MERGE Upserts
# MAGIC
# MAGIC The streaming output utilizes a `foreachBatch` sink to execute a SQL `MERGE` operation against the target sessions table. As user interactions evolve, session records alternate between `online` and `offline` statuses, modifying the target table idempotently rather than appending raw historical logs.
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{db}.sessions (
    user_id        STRING,
    click_count    LONG,
    start_time     LONG,
    end_time       LONG,
    status         STRING,
    event_datetime TIMESTAMP
  ) USING DELTA
""")

def upsert_sessions(df, epoch_id):
  (DeltaTable.forName(spark, f"{catalog}.{db}.sessions").alias("s")
    .merge(source=df.alias("u"), condition="s.user_id = u.user_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

q = (sessions.writeStream
     .option("checkpointLocation", f"{volume_folder}/sessions")
     .trigger(processingTime="5 seconds")
     .foreachBatch(upsert_sessions)
     .start())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5/ Live Metrics Monitoring
# MAGIC
# MAGIC To validate execution:
# MAGIC 1. Execute the companion producer notebook (`01-zerobus-producer`) in a parallel tab.
# MAGIC 2. Render the `display(sessions)` streaming DataFrame.
# MAGIC 3. Add a **Bar Chart** visualization using `status` as the X-axis and `COUNT(user_id)` as the Y-axis to monitor session state lifecycles in real time.
# MAGIC

# COMMAND ----------

# Live transformWithState output - add a Bar visualization per the steps above. Streams until you Interrupt.
display(sessions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6/ Alternative Schema Implementations
# MAGIC
# MAGIC This architecture is schema-agnostic and generalizes to any partitioned time-series workload:
# MAGIC *   **Mobile Application Analytics:** Tracks user navigation using `device_id`, `screen`, and `tap_target`.
# MAGIC *   **IoT Telemetry:** Monitors operational active windows using `device_id`, `reading_type`, and sensor thresholds.
# MAGIC *   **Gaming Events:** Measures continuous play sessions partitioned by `player_id` and game state changes.
# MAGIC *   **Server Logs:** Groups network latency or request bursts by service endpoints.
