# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

dbutils.widgets.text("reset_all_data", "false", "Reset Data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data)

data_exists = False
try:
  data_exists = spark.catalog.tableExists('tools_orders') and spark.catalog.tableExists('tools_customers')
except Exception as e:
  print(f"folder doesn't exists, generating the data...")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def get_shared_warehouse(name=None):
    w = WorkspaceClient()
    warehouses = w.warehouses.list()
    for wh in warehouses:
        if wh.name == name:
            return wh
    for wh in warehouses:
        if wh.name == "dbdemos-shared-endpoint":
            return wh
    #Try to fallback to an existing shared endpoint.
    for wh in warehouses:
        if "dbdemos" in wh.name.lower():
            return wh
    for wh in warehouses:
        if "shared" in wh.name.lower():
            return wh
    for wh in warehouses:
        if wh.num_clusters > 0:
            return wh       
    raise Exception("Couldn't find any Warehouse to use. Please create a wh first to run the demo and add the id here")


def display_tools(tools):
    display(pd.DataFrame([{k: str(v) for k, v in vars(tool).items()} for tool in tools]))


# COMMAND ----------

if not data_exists:
    from pyspark.sql import functions as F
    from faker import Faker
    from collections import OrderedDict 
    import uuid
    fake = Faker()
    import random
    from datetime import datetime, timedelta

    fake_firstname = F.udf(fake.first_name)
    fake_lastname = F.udf(fake.last_name)
    fake_email = F.udf(fake.ascii_company_email)

    def fake_date_between(months=0):
        start = datetime.now() - timedelta(days=30*months)
        return F.udf(lambda: fake.date_between_dates(date_start=start, date_end=start + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"))

    fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
    fake_date_old = F.udf(lambda:fake.date_between_dates(date_start=datetime(2012,1,1), date_end=datetime(2015,12,31)).strftime("%m-%d-%Y %H:%M:%S"))
    fake_address = F.udf(fake.address)
    canal = OrderedDict([("WEBAPP", 0.5),("MOBILE", 0.1),("PHONE", 0.3),(None, 0.01)])
    fake_canal = F.udf(lambda:fake.random_elements(elements=canal, length=1)[0])
    fake_id = F.udf(lambda: str(uuid.uuid4()))
    countries = ['FR', 'USA', 'SPAIN']
    fake_country = F.udf(lambda: countries[random.randint(0,2)])
    current_email = spark.sql('select current_user() as email').collect()[0]['email']
    def get_df(size, month, id=None):
        df = spark.range(0, size).repartition(10)
        df = df.withColumn("id", F.lit(id) if id else fake_id())
        df = df.withColumn("firstname", fake_firstname())
        df = df.withColumn("lastname", fake_lastname())
        df = df.withColumn("email", F.lit(current_email) if id else fake_email())
        df = df.withColumn("address", fake_address())
        df = df.withColumn("canal", fake_canal())
        df = df.withColumn("country", fake_country())  
        df = df.withColumn("creation_date", fake_date_between(month)())
        df = df.withColumn("last_activity_date", fake_date())
        return df.withColumn("age_group", F.round(F.rand()*10))

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
    # Define the weighted probabilities for the order status
    order_status_prob = OrderedDict([("Pending", 0.3), ("Shipped", 0.4), ("Delivered", 0.25), ("Cancelled", 0.05)])

    # UDF to generate random order status based on the defined probabilities
    fake_order_status = F.udf(lambda: fake.random_elements(elements=order_status_prob, length=1)[0])

    orders = spark.createDataFrame([(i,) for i in order_user_ids], ['user_id'])
    orders = orders.withColumn("id", fake_id())
    orders = orders.withColumn("transaction_date", fake_date())
    orders = orders.withColumn("item_count", F.round(F.rand()*2)+1)
    orders = orders.withColumn("amount", F.col("item_count")*F.round(F.rand()*30+10))
    orders = orders.withColumn("order_status", fake_order_status())  # Add the order status column
    orders.write.mode('overwrite').saveAsTable('tools_orders')
