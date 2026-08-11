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
# MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&org_id=923710833314495&notebook=%2F_00-Delta-session-PRODUCER&demo_name=streaming-sessionization&event=VIEW&path=%2F_dbdemos%2Fdata-engineering%2Fstreaming-sessionization%2F_00-Delta-session-PRODUCER&version=1">

# COMMAND ----------

# MAGIC %pip install faker kafka-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import logging
import socket
from kafka import KafkaProducer

# COMMAND ----------

# Enable logging to get more information about the connection process
logging.basicConfig(level=logging.DEBUG)

# COMMAND ----------

# Check network connectivity to the Kafka broker
def check_kafka_connectivity(host, port):
    try:
        socket.create_connection((host, port), timeout=10)
        logging.info(f"Successfully connected to Kafka broker at {host}:{port}")
    except Exception as e:
        logging.error(f"Failed to connect to Kafka broker at {host}:{port}: {e}")

# COMMAND ----------

bootstrap_server = "pkc-rgm37.us-west-2.aws.confluent.cloud:9092"
sasl_username = "XXXXXXXXXXXXXXXX"
sasl_password = "****************************************************************"

# COMMAND ----------

# Extract host and port from the bootstrap_servers
host, port = bootstrap_server.split(":")
check_kafka_connectivity(host, int(port))

# COMMAND ----------

config = {
    "bootstrap_servers": [bootstrap_server],
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": sasl_username,
    "sasl_plain_password": sasl_password,
    # Serializers
    "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    # "key_serializer": lambda k: k.encode("utf-8"),
    # Additional configurations for reliability
    "request_timeout_ms": 45000,
    "retry_backoff_ms": 500,
    "max_block_ms": 120000,  # Increase timeout to 2 minutes
    "connections_max_idle_ms": 300000,
    "api_version_auto_timeout_ms": 60000,
}

# Configuration for the Kafka producer
try:
    producer = KafkaProducer(**config)
    logging.info("Kafka producer created successfully")
except Exception as e:
    logging.error(f"Failed to create Kafka producer: {e}")

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

def sendMessage(event): 
  event = json.dumps(event)
  producer.send('dbdemos-sessions1', value=event)
  #print(event)
  #Simulate duplicate events to drop the duplication
  if random.uniform(0, 1) > 0.96:
    producer.send('dbdemos-sessions1', value=event)

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
        sendMessage(event)
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

print("closed")


# COMMAND ----------

