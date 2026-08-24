# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %uv pip install faker

# COMMAND ----------

# MAGIC %run ./00-init

# COMMAND ----------

data_exists = False
try:
  data_exists = spark.catalog.tableExists('tools_orders') and spark.catalog.tableExists('tools_customers')
  if data_exists:
    data_exists = spark.sql('select count(*) as c from tools_customers where email=current_user').collect()[0]['c'] > 0
except Exception as e:
  print(f"folder doesn't exists, generating the data...")

# COMMAND ----------

if not data_exists:
    from pyspark.sql import functions as F
    from faker import Faker
    import pandas as pd
    import numpy as np
    import uuid
    fake = Faker()
    from datetime import datetime, timedelta

    # Generate the fake data with Faker in pandas (driver-side), then create a Spark
    # DataFrame. Spark Python UDFs + non-deterministic columns trip the serverless
    # Spark Connect analyzer (spurious MISSING_GROUP_BY on write); a materialized
    # createDataFrame is deterministic and serverless-safe.
    canal_values = ["WEBAPP", "MOBILE", "PHONE", None]
    canal_probs = np.array([0.5, 0.1, 0.3, 0.01]); canal_probs = canal_probs / canal_probs.sum()
    countries = ['FR', 'USA', 'SPAIN']
    current_email = spark.sql('select current_user() as email').collect()[0]['email']

    def get_df(size, month, id=None):
        # date_between window mirrors the original fake_date_between(month)
        start = datetime.now() - timedelta(days=30*month)
        pdf = pd.DataFrame({
            "id": [id] * size if id else [str(uuid.uuid4()) for _ in range(size)],
            "firstname": [fake.first_name() for _ in range(size)],
            "lastname": [fake.last_name() for _ in range(size)],
            "email": [current_email] * size if id else [fake.ascii_company_email() for _ in range(size)],
            "address": [fake.address().replace('\n', ' ') for _ in range(size)],
            "canal": np.random.choice(canal_values, size, p=canal_probs),
            "country": [countries[np.random.randint(0, 3)] for _ in range(size)],
            "creation_date": [fake.date_between_dates(date_start=start, date_end=start + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S") for _ in range(size)],
            "last_activity_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(size)],
            "age_group": np.round(np.random.rand(size) * 10),
        })
        return spark.createDataFrame(pdf)

    df_customers = get_df(1000, 12*30)
    df_customers = df_customers.union(get_df(1, 12*30, id='d8ca793f-7f06-42d3-be1b-929e32fc8bc9'))
    #for i in range(1, 24):
    #  df_customers = df_customers.union(get_df(2000+i*200, 24-i))

    df_customers.write.mode('overwrite').saveAsTable('tools_customers')

    ids = spark.read.table(('tools_customers')).select("id").collect()
    ids = [r["id"] for r in ids]


# COMMAND ----------

if not data_exists:
    #Number of order per customer to generate a nicely distributed dataset
    import numpy as np
    np.random.seed(0)
    mu, sigma = 3, 2 # mean and standard deviation
    s = np.random.normal(mu, sigma, int(len(ids)))
    s = [i if i > 0 else 0 for i in s]

    #Most of our customers have ~3 orders
    import matplotlib.pyplot as plt
    count, bins, ignored = plt.hist(s, 30, density=False)
    plt.show()
    s = [int(i) for i in s]

    order_user_ids = list()
    for i, id in enumerate(ids):
        order_count = s[i]
        if id == 'd8ca793f-7f06-42d3-be1b-929e32fc8bc9':
            order_count = 5
        for j in range(1, order_count):
            order_user_ids.append(id)

    print(f"Generated {len(order_user_ids)} orders for {len(ids)} users")

# COMMAND ----------


if not data_exists:
    # Build the order columns in pandas (Faker + numpy) then createDataFrame, to
    # avoid Spark Python UDFs + non-deterministic columns on serverless
    # (spurious MISSING_GROUP_BY on write via the Spark Connect analyzer).
    n_orders = len(order_user_ids)
    order_status_values = ["Pending", "Shipped", "Delivered", "Cancelled"]
    order_status_probs = np.array([0.3, 0.4, 0.25, 0.05]); order_status_probs = order_status_probs / order_status_probs.sum()

    item_count = np.round(np.random.rand(n_orders) * 2) + 1
    orders_pdf = pd.DataFrame({
        "user_id": order_user_ids,
        "id": [str(uuid.uuid4()) for _ in range(n_orders)],
        "transaction_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(n_orders)],
        "item_count": item_count,
        "amount": item_count * (np.round(np.random.rand(n_orders) * 30 + 10)),
        "order_status": np.random.choice(order_status_values, n_orders, p=order_status_probs),
    })
    orders = spark.createDataFrame(orders_pdf)
    orders.write.mode('overwrite').saveAsTable('tools_orders')
