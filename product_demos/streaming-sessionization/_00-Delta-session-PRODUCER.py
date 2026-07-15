# Databricks notebook source
dbutils.widgets.text("produce_time_sec", "300", "How long we'll produce data (sec)")

# COMMAND ----------

# MAGIC %md
# MAGIC #Kafka producer
# MAGIC
# MAGIC Use this producer to create a stream of fake user in your website and sends the message to kafka, live.
# MAGIC
# MAGIC Run all the cells, once. Currently requires to run on a cluster with instance profile allowing kafka connection (one-env, aws).
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=01-Delta-session-GOLD&demo_name=streaming-sessionization&event=VIEW">

# COMMAND ----------

# MAGIC %uv pip install faker confluent-kafka

# COMMAND ----------

from confluent_kafka import Producer
import json
import random

kafka_bootstrap_servers_tls = "b-1.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9094,b-2.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9094,b-3.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9094"
#kafka_bootstrap_servers_tls = "<Replace by your own kafka servers>"
# Also make sure to have the proper instance profile to allow the access if you're on AWS.

conf = {
    'bootstrap.servers': kafka_bootstrap_servers_tls,
    'security.protocol': 'SSL'
}

producer = Producer(conf)

def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_message(event, topic = 'dbdemos-sessions'):
    event_json = json.dumps(event)
    producer.produce(topic, value=event_json, callback=delivery_report)
    producer.poll(0)  # Trigger delivery report callbacks

    # Simulate duplicate events to test deduplication
    if random.uniform(0, 1) > 0.96:
        producer.produce(topic, value=event_json, callback=delivery_report)
        producer.poll(0)
    producer.flush()

#send_message({"test": "toto"},  'test')

# COMMAND ----------

import re
from faker import Faker
from collections import OrderedDict 
from random import randrange
import time
import uuid
fake = Faker()
import random
import json

platform = OrderedDict([("ios", 0.5),("android", 0.1),("other", 0.3),(None, 0.01)])
action_type = OrderedDict([("view", 0.5),("log", 0.1),("click", 0.3),(None, 0.01)])

def create_event(user_id, timestamp):
  fake_platform = fake.random_elements(elements=platform, length=1)[0]
  fake_action = fake.random_elements(elements=action_type, length=1)[0]
  fake_uri = re.sub(r'https?:\/\/.*?\/', "https://databricks.com/", fake.uri())
  #adds some noise in the timestamp to simulate out-of order events
  timestamp = timestamp + randrange(10)-5
  #event id with 2% of null event to have some errors/cleanup
  fake_id = str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None
  return {"user_id": user_id, "platform": fake_platform, "event_id": fake_id, "event_date": timestamp, "action": fake_action, "uri": fake_uri}

print(create_event(str(uuid.uuid4()), int(time.time())))

# COMMAND ----------

#How long it'll produce messages
produce_time_sec = int(dbutils.widgets.get("produce_time_sec"))
#How many new users join the website per second
user_creation_rate = 2
#Max duration a user stays in the website (after this time user will stop producing events)
user_max_duration_time = 120

# NOTE: dbdemos runs this producer as the first step of the bundle test, before the
# streaming consumers. To keep the bundle deterministic and fast, we generate a bounded
# batch of events as quickly as possible and exit (instead of the slow real-time loop).
# The original real-time simulation is kept below (commented) for the interactive demo.
number_of_users = 300
events_per_user_min, events_per_user_max = 3, 15
now = int(time.time())
for _ in range(number_of_users):
  user_id = str(uuid.uuid4())
  # each user generates a burst of clicks within a short session window
  session_start = now - randrange(user_max_duration_time)
  for _click in range(randrange(events_per_user_min, events_per_user_max)):
    event = create_event(user_id, session_start + randrange(user_max_duration_time))
    send_message(event)

# Ensure all messages are delivered before exiting
producer.flush()
print("Done producing the bounded batch of events.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real-time simulation (optional)
# MAGIC The cell above produces a bounded batch so the demo pipeline can be tested end to end.
# MAGIC To instead simulate a *live* stream of users on your website, run the loop below (it
# MAGIC produces events continuously for `produce_time_sec` seconds):
# MAGIC ```python
# MAGIC users = {}
# MAGIC for _ in range(produce_time_sec):
# MAGIC   for id in list(users.keys()):
# MAGIC     user = users[id]
# MAGIC     now = int(time.time())
# MAGIC     if (user['end_date'] < now):
# MAGIC       del users[id]
# MAGIC     else:
# MAGIC       if (randrange(100) > 80):  # 10% chance to click on something
# MAGIC         send_message(create_event(id, now))
# MAGIC   for i in range(user_creation_rate):  # Re-create new users
# MAGIC     user_id = str(uuid.uuid4())
# MAGIC     now = int(time.time())
# MAGIC     users[user_id] = {"id": user_id, "creation_date": now, "end_date": now + randrange(user_max_duration_time)}
# MAGIC   time.sleep(1)
# MAGIC producer.flush()
# MAGIC ```
