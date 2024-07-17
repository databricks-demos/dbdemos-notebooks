# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Leveraging Databricks Lakehouse & system table to forecast your billing
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/uc-system-tables-flow.png?raw=true" width="800px" style="float: right">
# MAGIC
# MAGIC As your billing information is saved in your system table, it's easy to leverage the lakehouse capabilities to forecast your consumption, and add alerts.
# MAGIC
# MAGIC In this notebook, we'll run some analysis on the current consumption and build a Prophet model to forecast the future usage, based on SKU and workspace.
# MAGIC
# MAGIC Because each workspace can have a different pattern / trend, we'll specialize multiple models on each SKU and Workspace and train them in parallel.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=02-forecast-billing-tables&demo_name=04-system-tables&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Note: refresh your forecast data every day
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/uc-system-job.png?raw=true" style="float: left; margin: 20px" width="550px">
# MAGIC
# MAGIC Make sure you refresh your forecast every day to get accurate previsions. 
# MAGIC
# MAGIC To do that, simply click on Schedule and select "Every Day" to create a new Workflow refreshing this notebook on a daily basis. 
# MAGIC
# MAGIC If you don't do it, your forecast will quickly expire and won't reflect potential consumption change / trigger alerts.
# MAGIC
# MAGIC To make sure this happens, we also added a tracker in the Databricks SQL dashboard to display the number of days since the last forecast. 

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md 
# MAGIC ## A note on pricing tables
# MAGIC Note that Pricing tables (containing the price information in `$` for each SKU) is available as a system table.
# MAGIC
# MAGIC **Please consider these numbers as estimates which do not include any add-ons or discounts. It is using list price, not contractual. Please review your contract for more accurate information.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Granular Forecasts
# MAGIC
# MAGIC SKUs are detailed per line of product and per region. To simplify our billing dashboard, we'll merge them under common SKU types i.e. grouping `STANDARD_ALL_PURPOSE_COMPUTE` and `PREMIUM_ALL_PURPOSE_COMPUTE` as `ALL_PURPOSE`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.usage u
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

# COMMAND ----------

# DBTITLE 1,Group by main SKU category
from pyspark.sql.functions import col, when, sum, current_date, lit
data_to_predict = spark.sql("""
  select u.workspace_id, 
  u.usage_date as ds, 
  u.sku_name as sku, 
  cast(u.usage_quantity as double) as dbus, 
  cast(lp.pricing.default*usage_quantity as double) as cost_at_list_price 
  
  from system.billing.usage u 
      inner join system.billing.list_prices lp on u.cloud = lp.cloud and
        u.sku_name = lp.sku_name and
        u.usage_start_time >= lp.price_start_time and
        (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
  where u.usage_unit = 'DBU'
""")

#Group the SKU in main family that we'll predict
data_to_predict = data_to_predict.withColumn("sku",
                     when(col("sku").contains("ALL_PURPOSE"), "ALL_PURPOSE")
                    .when(col("sku").contains("JOBS"), "JOBS")
                    .when(col("sku").contains("DLT"), "DLT")
                    .when(col("sku").contains("SQL"), "SQL")
                    .when(col("sku").contains("INFERENCE"), "MODEL_INFERENCE")
                    .otherwise("OTHER"))
                  
# Sum the consumption at a daily level (1 value per day and per sku+workspace)
data_to_predict_daily = data_to_predict.groupBy(col("ds"), col('sku'), col('workspace_id')).agg(sum('dbus').alias("dbus"), sum('cost_at_list_price').alias("cost_at_list_price"))

display(data_to_predict_daily)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Using prophet to forecast our billing data
# MAGIC
# MAGIC Once aggregated at a daily level, billing information can be forecasted as a timeseries. For this demo, we will use a simple Prophet model to extend the timeseries to the next quarter 

# COMMAND ----------

from prophet import Prophet

#Predict days, for the next 3 months
forecast_frequency='d'
forecast_periods=31*3

interval_width=0.8
include_history=True

def generate_forecast(history_pd, display_graph = True):
    # remove any missing values
    history_pd = history_pd.dropna()
    if history_pd.shape[0] > 10:
        # train and configure the model
        model = Prophet(interval_width=interval_width )
        model.add_country_holidays(country_name='US')

        model.fit(history_pd)

        # make predictions
        future_pd = model.make_future_dataframe(periods=forecast_periods, freq=forecast_frequency, include_history=include_history)
        forecast_pd = model.predict(future_pd)

        if display_graph:
           model.plot(forecast_pd)
        # add back y to the history dataset 
        f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
        # join history and forecast
        results_pd = f_pd.join(history_pd[['ds','y','dbus']].set_index('ds'), how='left')
        results_pd.reset_index(level=0, inplace=True)
        results_pd['ds'] = results_pd['ds'].dt.date
        # get sku & workspace id from incoming data set
        results_pd['sku'] = history_pd['sku'].iloc[0]
        results_pd['workspace_id'] = history_pd['workspace_id'].iloc[0]
    else:
        # not enough data to predict, return history
        for c in ['yhat', 'yhat_upper', 'yhat_lower']:
            history_pd[c] = history_pd['y']
        results_pd = history_pd[['ds','y','dbus','yhat', 'yhat_upper', 'yhat_lower', 'sku', 'workspace_id']]
    return results_pd

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create the billing forecast table:
# MAGIC CREATE TABLE IF NOT EXISTS billing_forecast (ds DATE, yhat DOUBLE, yhat_upper DOUBLE, yhat_lower DOUBLE, y DOUBLE, dbus DOUBLE, sku STRING, workspace_id STRING, training_date DATE);

# COMMAND ----------

# DBTITLE 1,Forecast for the sum of all DBUS
#Sum all the SKUs & Workspace for global consumption trend (by default we want a view for all our billing usage across all workspace, so we need to train a specific model on that too)
global_forecast = data_to_predict_daily.groupBy(col("ds")).agg(sum('cost_at_list_price').alias("y"), sum('dbus').alias("dbus")) \
                                       .withColumn('sku', lit('ALL')) \
                                       .withColumn('workspace_id', lit('ALL')).toPandas()
global_forecast = generate_forecast(global_forecast)
spark.createDataFrame(global_forecast).withColumn('training_date', current_date()) \
                                      .write.mode('overwrite').option("mergeSchema", "true").saveAsTable("billing_forecast")

# COMMAND ----------

# DBTITLE 1,Distribute training for each DBU SKUs to have specialized models & predictions
def generate_forecast_udf(history_pd):
  return generate_forecast(history_pd, False)

#Add an entry with the sum of all types of DBU per workspace
all_per_workspace = data_to_predict_daily.groupBy('ds', 'workspace_id').agg(sum('cost_at_list_price').alias("cost_at_list_price"), sum('dbus').alias("dbus")) \
                                         .withColumn('sku', lit('ALL'))
all_skus = data_to_predict_daily.groupBy('ds', 'sku').agg(sum('cost_at_list_price').alias("cost_at_list_price"), sum('dbus').alias("dbus")) \
                                         .withColumn('workspace_id', lit('ALL'))

results = (
  data_to_predict_daily
    .union(all_per_workspace.select(data_to_predict_daily.columns)) #Add all workspaces for all skus
    .union(all_skus.select(data_to_predict_daily.columns)) #Add all sku across all workspaces
    .withColumnRenamed('cost_at_list_price', 'y') #Prophet expect 'y' as input to predict
    .groupBy('workspace_id', 'sku') # Group by SKU + Workspace and call our model for each group
    .applyInPandas(generate_forecast_udf, schema="ds date, yhat double, yhat_upper double, yhat_lower double, y double, dbus double, sku string, workspace_id string")
    .withColumn('training_date', current_date()))

#Save back to tables to our final table that we'll be using in our dashboard
results.write.mode('append').saveAsTable("billing_forecast")

# COMMAND ----------

# DBTITLE 1,Our forecasting data is ready
# MAGIC %sql 
# MAGIC select * from billing_forecast

# COMMAND ----------

# DBTITLE 1,Create a view on top to simplify access
# MAGIC %sql
# MAGIC create or replace view detailed_billing_forecast as with forecasts as (
# MAGIC   select
# MAGIC     f.ds as date,
# MAGIC     f.workspace_id,
# MAGIC     f.sku,
# MAGIC     f.dbus,
# MAGIC     f.y,
# MAGIC     GREATEST(0, f.yhat) as yhat,
# MAGIC     GREATEST(0, f.yhat_lower) as yhat_lower,
# MAGIC     GREATEST(0, f.yhat_upper) as yhat_upper,
# MAGIC     f.y > f.yhat_upper as upper_anomaly_alert,
# MAGIC     f.y < f.yhat_lower as lower_anomaly_alert,
# MAGIC     f.y >= f.yhat_lower AND f.y <= f.yhat_upper as on_trend,
# MAGIC     f.training_date
# MAGIC   from billing_forecast f
# MAGIC )
# MAGIC select
# MAGIC   `date`,
# MAGIC   workspace_id,
# MAGIC   dbus,
# MAGIC   sku,
# MAGIC   y as past_list_cost, -- real
# MAGIC   y is null as is_prediction ,
# MAGIC   coalesce(y, yhat) as list_cost, -- contains real and forecast
# MAGIC   yhat as forecast_list_cost,  -- forecast
# MAGIC   yhat_lower as forecast_list_cost_lower,
# MAGIC   yhat_upper as forecast_list_cost_upper,
# MAGIC   -- smooth upper and lower bands for better visualization
# MAGIC   avg(yhat) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_ma, 
# MAGIC   avg(yhat_lower) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_upper_ma,
# MAGIC   avg(yhat_upper) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_lower_ma,
# MAGIC   upper_anomaly_alert,
# MAGIC   lower_anomaly_alert,
# MAGIC   on_trend,
# MAGIC   training_date
# MAGIC from
# MAGIC   forecasts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from detailed_billing_forecast  where sku = 'ALL' limit 100

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Your forecast tables are ready for BI
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/system_tables/dashboard-governance-billing.png?raw=true" width="500px" style="float: right">
# MAGIC
# MAGIC dbdemos installed a dashboard for you to start exploring your data. 
# MAGIC <a dbdemos-dashboard-id="account-usage" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1'>Open the dashboard</a> to start exploring your billing data.<br/>
# MAGIC Remember that if you changed your Catalog and Schema, you'll have to update the queries.
# MAGIC
# MAGIC
# MAGIC ## Exploring our forecasting data from the notebook
# MAGIC
# MAGIC Our forecasting data is ready! We can explore it within the notebook directly, using Databricks built-in widget our any python plot library:

# COMMAND ----------

# DBTITLE 1,Forecast consumption for all SKU in all our workspaces
import plotly.graph_objects as go

df = spark.table('detailed_billing_forecast').where("sku = 'ALL' and workspace_id='ALL'").toPandas()
#Remove the last point as the day isn't completed (not relevant)
df.at[df['past_list_cost'].last_valid_index(), 'past_list_cost'] = None

# Create traces
fig = go.Figure()
fig.add_trace(go.Scatter(x=df['date'], y=df['past_list_cost'][:-4], name='actual usage'))
fig.add_trace(go.Scatter(x=df['date'], y=df['forecast_list_cost'], name='forecast cost (pricing list)'))
fig.add_trace(go.Scatter(x=df['date'], y=df['forecast_list_cost_upper'], name='forecast cost up', line = dict(color='grey', width=1, dash='dot')))
fig.add_trace(go.Scatter(x=df['date'], y=df['forecast_list_cost_lower'], name='forecast cost low', line = dict(color='grey', width=1, dash='dot')))

# COMMAND ----------

# DBTITLE 1,Detailed view for each SKU
import plotly.express as px
df = spark.table('detailed_billing_forecast').where("workspace_id='ALL'").groupBy('sku', 'date').agg(sum('list_cost').alias('list_cost')).orderBy('date').toPandas()
px.line(df, x="date", y="list_cost", color='sku')

# COMMAND ----------

# DBTITLE 1,Top 5 workspaces view
#Focus on the ALL PURPOSE dbu
df = spark.table('detailed_billing_forecast').where("sku = 'ALL_PURPOSE' and workspace_id != 'ALL'")
#Get the top 5 worpskaces consuming the most
top_workspace = df.groupBy('workspace_id').agg(sum('list_cost').alias('list_cost')).orderBy(col('list_cost').desc()).limit(5)
workspace_id = [r['workspace_id'] for r in top_workspace.collect()]
#Group consumption per workspace
df = df.where(col('workspace_id').isin(workspace_id)).groupBy('workspace_id', 'date').agg(sum('list_cost').alias('list_cost')).orderBy('date').toPandas()
px.bar(df, x="date", y="list_cost", color="workspace_id", title="Long-Form Input")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Alert Creation
# MAGIC
# MAGIC Using Databricks SQL Alerts, users are able to create a system that will automatically detect when actual usage goes out of the expected bounds. In our scenario we will be alerted if the usage goes above or below the lower and upper bounds of the forecast. In our forecasting algorithm we generate the following columns:  
# MAGIC - `upper_anomaly_alert`: True if actual amount is greater than the upper bound. 
# MAGIC - `lower_anomaly_alert`: True if actual amount is less than the lower bound. 
# MAGIC - `on_trend`: True if `upper_anomaly_alert` or `lower_anomaly_alert` is True. 
# MAGIC
# MAGIC Please see the output forecast output dataset below. 
