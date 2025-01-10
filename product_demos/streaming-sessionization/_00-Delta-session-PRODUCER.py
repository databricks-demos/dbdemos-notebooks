# Databricks notebook source
dbutils.widgets.text("produce_time_sec", "600", "How long we'll produce data (sec)")

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

# MAGIC %pip install faker confluent-kafka

# COMMAND ----------

from confluent_kafka import Producer
import json
import random

kafka_bootstrap_servers_tls = "b-1.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9092,b-2.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9092,b-3.oneenvkafka.fso631.c14.kafka.us-west-2.amazonaws.com:9092"
#kafka_bootstrap_servers_tls = "<Replace by your own kafka servers>"
# Also make sure to have the proper instance profile to allow the access if you're on AWS.

conf = {
    'bootstrap.servers': kafka_bootstrap_servers_tls}

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

users = {}
#How long it'll produce messages
produce_time_sec = int(dbutils.widgets.get("produce_time_sec"))
#How many new users join the website per second
user_creation_rate = 2
#Max duration a user stays in the website (after this time user will stop producing events)
user_max_duration_time = 120

for _ in range(produce_time_sec):
  #print(len(users))
  for id in list(users.keys()):
    user = users[id]
    now = int(time.time())
    if (user['end_date'] < now):
      del users[id]
      #print(f"User {id} removed")
    else:
      #10% chance to click on something
      if (randrange(100) > 80):
        event = create_event(id, now)
        send_message(event)
        #print(f"User {id} sent event {event}")
        
  #Re-create new users
  for i in range(user_creation_rate):
    #Add new user
    user_id = str(uuid.uuid4())
    now = int(time.time())
    #end_date is when the user will leave and the session stops (so max user_max_duration_time sec and then leaves the website)
    user = {"id": user_id, "creation_date": now, "end_date": now + randrange(user_max_duration_time) }
    users[user_id] = user
    #print(f"User {user_id} created")
  time.sleep(1)


# Ensure all messages are delivered before exiting
producer.flush()
