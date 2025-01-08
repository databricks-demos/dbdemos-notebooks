# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)  3/ GOLD table: extract the sessions
# MAGIC
# MAGIC <img style="float:right; height: 250px; margin: 0px 30px 0px 30px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/streaming-sessionization/session_diagram.png">
# MAGIC
# MAGIC ### Why is this a challenge?
# MAGIC Because we don't have any event to flag the user disconnection, detecting the end of the session is hard. After 10 minutes without any events, we want to be notified that the session has ended.
# MAGIC However, spark will only react on event, not the absence of event.
# MAGIC
# MAGIC Thanksfully, Spark Structured Streaming has the concept of timeout. 
# MAGIC
# MAGIC **We can set a 10 minutes timeout in the state engine** and be notified 10 minutes later in order to close the session
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=01-Delta-session-GOLD&demo_name=streaming-sessionization&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Implementing the aggregation function to update our Session
# MAGIC
# MAGIC In this simple example, we'll just be counting the number of click in the session.

# COMMAND ----------

from typing import Tuple, Iterator
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

DBDemos.wait_for_table("events") #Wait until the previous table is created to avoid error if all notebooks are started at once


#If we don't have activity after 30sec, close the session
max_session_duration = 30000
def func(
    key: Tuple[str], events: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
  (user_id,) = key
  print(user_id)
  if state.exists:
    (user_id, click_count, start_time, end_time) = state.get
  else:
    click_count = 0
    start_time = sys.maxsize
    end_time = 0
  state.getOption
  if state.hasTimedOut:
    #Drop the session from the state and emit a final offline session update (end of the session)
    state.remove() 
    yield pd.DataFrame({"user_id": [user_id], "click_count": [click_count], "start_time": [start_time], "end_time": [end_time],  "status": ["offline"]})
  else:
    # as we can receive out-of-order events, we need to get the min/max date and the sum
    for df in events:
      start_time = min(start_time, df['event_date'].min())
      end_time = max(df['event_date'].max(), end_time)
      click_count += len(df)
    #update the state with the new values
    state.update((user_id, int(click_count), int(start_time), int(end_time)))
    # Set the timeout as max_session_duration seconds.
    state.setTimeoutDuration(max_session_duration)
    #compute the status to flag offline session in case of restart
    now = int(time.time())
    status = "offline" if end_time >= now - max_session_duration else "online"
    #emit the change. We could also yield an empty dataframe if we only want to emit when the session is closed: yield pd.DataFrame()
    yield pd.DataFrame({"user_id": [user_id], "click_count": [click_count], "start_time": [start_time], "end_time": [end_time],  "status": [status]})


output_schema = "user_id STRING, click_count LONG, start_time LONG, end_time LONG, status STRING"
state_schema = "user_id STRING, click_count LONG, start_time LONG, end_time LONG"

sessions = spark.readStream.table("events").groupBy(F.col("user_id")).applyInPandasWithState(
    func,
    output_schema,
    state_schema,
    "append",
    GroupStateTimeout.ProcessingTimeTimeout)

display(sessions, checkpointLocation = get_chkp_folder()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Updating the session table with number of clicks and end/start time
# MAGIC
# MAGIC We want to have the session information in real time for each user. 
# MAGIC
# MAGIC To do that, we'll create a Session table. Everytime we update the state, we'll UPSERT the session information:
# MAGIC
# MAGIC - if the session doesn't exist, we add it
# MAGIC - if it exists, we update it with the new count and potential new status
# MAGIC
# MAGIC This can easily be done with a MERGE operation using Delta and calling `foreachBatch`

# COMMAND ----------

from delta.tables import DeltaTable

def upsert_sessions(df, epoch_id):
  #Create the table if it's the first time (we need it to be able to perform the merge)
  if epoch_id == 0 and not spark._jsparkSession.catalog().tableExists('sessions'):
    df.limit(0).write.option('mergeSchema', 'true').mode('append').saveAsTable('sessions')

  (DeltaTable.forName(spark, "sessions").alias("s").merge(
    source = df.alias("u"),
    condition = "s.user_id = u.user_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute())
  
(sessions.writeStream
  .option("checkpointLocation", volume_folder+"/checkpoints/sessions")
  .foreachBatch(upsert_sessions)
  .start())

DBDemos.wait_for_table("sessions")

# COMMAND ----------

# MAGIC %sql SELECT * FROM sessions

# COMMAND ----------

# MAGIC %sql SELECT CAST(avg(end_time - start_time) as INT) average_session_duration FROM sessions

# COMMAND ----------

# DBTITLE 1,Stop all the streams 
DBDemos.stop_all_streams(sleep_time=120)

# COMMAND ----------

# MAGIC %md
# MAGIC ### We now have our sessions stream running!
# MAGIC
# MAGIC We can set the output of this streaming job to a SQL database or another queuing system.
# MAGIC
# MAGIC We'll be able to automatically detect cart abandonments in our website and send an email to our customers, our maybe just give them a call asking if they need some help! 
