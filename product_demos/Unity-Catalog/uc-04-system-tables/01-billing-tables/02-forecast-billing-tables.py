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

# MAGIC %sql
# MAGIC -- NOTE: make sure you run this notebook using a SQL Warehouse or Serverless endpoint (not a classic cluster).
# MAGIC SELECT assert_true(current_version().dbsql_version is not null, 'YOU MUST USE A SQL WAREHOUSE OR SERVERLESS, not a classic cluster');

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

# MAGIC %md
# MAGIC ## Leveraging Databricks AI_FORECAST function
# MAGIC
# MAGIC Databricks provides a built-in AI Forecast capability. See the [AI_FORECAST documentation](https://docs.databricks.com/en/sql/language-manual/functions/ai_forecast.html) for more details.
# MAGIC
# MAGIC **Note that this might require the preview to be enabled to your workspace. If the preview isn't enable, we show you how to do the forecast below using prophet in python.**
# MAGIC
# MAGIC **Make sure you run these next cells using a SQL WAREHOUSE as compute, not a classic cluster as the AI_FORECAST preview is only available in serverless for now.**
# MAGIC
# MAGIC *Note: If the `AI_FORECAST` isn't yet available in your workspace, you can skip to the next section where we show you how to do the same in python.*

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.usage u
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

# COMMAND ----------

# DBTITLE 1,Create the view, grouping by type of DBUs
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW data_to_predict AS (
# MAGIC WITH 
# MAGIC -- Classify SKU types and calculate usage metrics
# MAGIC classified_data AS (
# MAGIC     SELECT 
# MAGIC         u.workspace_id, 
# MAGIC         u.usage_date AS ds, 
# MAGIC         CASE
# MAGIC             WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE'
# MAGIC             WHEN u.sku_name LIKE '%JOBS%' THEN 'JOBS'
# MAGIC             WHEN u.sku_name LIKE '%DLT%' THEN 'DLT'
# MAGIC             WHEN u.sku_name LIKE '%SQL%' THEN 'SQL'
# MAGIC             WHEN u.sku_name LIKE '%INFERENCE%' THEN 'MODEL_INFERENCE'
# MAGIC             ELSE 'OTHER'
# MAGIC         END AS sku,
# MAGIC         CAST(u.usage_quantity AS DOUBLE) AS dbus, 
# MAGIC         CAST(lp.pricing.default * u.usage_quantity AS DOUBLE) AS cost_at_list_price
# MAGIC     FROM 
# MAGIC         system.billing.usage u
# MAGIC     INNER JOIN 
# MAGIC         system.billing.list_prices lp 
# MAGIC     ON 
# MAGIC         u.cloud = lp.cloud 
# MAGIC         AND u.sku_name = lp.sku_name 
# MAGIC         AND u.usage_start_time >= lp.price_start_time 
# MAGIC         AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC     WHERE 
# MAGIC         u.usage_unit = 'DBU'
# MAGIC ),
# MAGIC -- Aggregate data by day, SKU, and workspace
# MAGIC daily_data AS (
# MAGIC     SELECT 
# MAGIC         ds,
# MAGIC         sku,
# MAGIC         workspace_id,
# MAGIC         SUM(dbus) AS dbus,
# MAGIC         SUM(cost_at_list_price) AS cost_at_list_price
# MAGIC     FROM 
# MAGIC         classified_data
# MAGIC     GROUP BY 
# MAGIC         ds, sku, workspace_id
# MAGIC ),
# MAGIC -- Generate totals: workspace, SKU, and global
# MAGIC workspace_totals AS (
# MAGIC     SELECT ds, 'ALL' AS sku, workspace_id, SUM(dbus) AS dbus, SUM(cost_at_list_price) AS cost_at_list_price
# MAGIC     FROM daily_data
# MAGIC     GROUP BY ds, workspace_id
# MAGIC ),
# MAGIC sku_totals AS (
# MAGIC     SELECT ds, sku, 'ALL' AS workspace_id, SUM(dbus) AS dbus, SUM(cost_at_list_price) AS cost_at_list_price
# MAGIC     FROM daily_data
# MAGIC     GROUP BY ds, sku
# MAGIC ),
# MAGIC global_totals AS (
# MAGIC     SELECT ds, 'ALL' AS sku, 'ALL' AS workspace_id, SUM(dbus) AS dbus, SUM(cost_at_list_price) AS cost_at_list_price
# MAGIC     FROM daily_data
# MAGIC     GROUP BY ds
# MAGIC ),
# MAGIC -- Filter for active workspaces
# MAGIC active_workspaces AS (
# MAGIC     SELECT DISTINCT workspace_id
# MAGIC     FROM daily_data
# MAGIC     WHERE ds >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ),
# MAGIC -- Combine all data into a single dataset
# MAGIC combined_data AS (
# MAGIC     SELECT * FROM daily_data WHERE workspace_id IN (SELECT workspace_id FROM active_workspaces)
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM workspace_totals WHERE workspace_id IN (SELECT workspace_id FROM active_workspaces)
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM sku_totals
# MAGIC     UNION ALL
# MAGIC     SELECT * FROM global_totals
# MAGIC )
# MAGIC -- Add the MAX computation after the UNION (we use it as cap in our forecast)
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     MAX(cost_at_list_price) OVER (PARTITION BY sku, workspace_id) AS max_cost_at_list_price
# MAGIC FROM combined_data
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM data_to_predict ORDER BY sku, workspace_id, ds limit 1000 ;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now leverage the `AI_FORECAST` function to forecast the future pricing fur each sku/workspace id.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.dbdemos_billing_forecast.billing_forecast;
# MAGIC CREATE TABLE main.dbdemos_billing_forecast.billing_forecast AS 
# MAGIC WITH data_to_predict_with_params AS (
# MAGIC   SELECT 
# MAGIC     '{"global_floor": 0, "min_changepoint_samples": 30, "global_cap": ' || (max_cost_at_list_price * 5) || '}' AS parameters,
# MAGIC     ds, 
# MAGIC     cost_at_list_price, 
# MAGIC     sku, 
# MAGIC     workspace_id
# MAGIC   FROM data_to_predict
# MAGIC )
# MAGIC SELECT 
# MAGIC   *, 
# MAGIC   current_date() as training_date
# MAGIC FROM ai_forecast(
# MAGIC   TABLE(data_to_predict_with_params),
# MAGIC   horizon => (SELECT MAX(ds) + INTERVAL 120 DAYS FROM data_to_predict),
# MAGIC   time_col => 'ds',
# MAGIC   value_col => 'cost_at_list_price',
# MAGIC   prediction_interval_width => 0.8,
# MAGIC   frequency => 'D',
# MAGIC   group_col => ARRAY('sku', 'workspace_id'),
# MAGIC   parameters => 'parameters'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Our forecast are ready!
# MAGIC %sql 
# MAGIC select * from main.dbdemos_billing_forecast.billing_forecast

# COMMAND ----------

# DBTITLE 1,Merge forecast data and history in a single table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.dbdemos_billing_forecast.detailed_billing_forecast;
# MAGIC CREATE OR REPLACE TABLE main.dbdemos_billing_forecast.detailed_billing_forecast AS 
# MAGIC   WITH forecast_data as (
# MAGIC     SELECT 
# MAGIC       NULL as training_date, 
# MAGIC       ds, 
# MAGIC       sku, 
# MAGIC       workspace_id, 
# MAGIC       cost_at_list_price as past_list_cost, 
# MAGIC       NULL as cost_at_list_price_forecast, 
# MAGIC       NULL as cost_at_list_price_upper, 
# MAGIC       NULL as cost_at_list_price_lower 
# MAGIC       FROM data_to_predict 
# MAGIC         UNION 
# MAGIC     SELECT 
# MAGIC       training_date,
# MAGIC       ds, 
# MAGIC       sku, 
# MAGIC       workspace_id, 
# MAGIC       NULL as past_list_cost, 
# MAGIC       GREATEST(0, cost_at_list_price_forecast), 
# MAGIC       GREATEST(0, cost_at_list_price_upper), 
# MAGIC       GREATEST(0, cost_at_list_price_lower) 
# MAGIC       FROM  main.dbdemos_billing_forecast.billing_forecast)
# MAGIC
# MAGIC     SELECT * EXCEPT(ds), 
# MAGIC       ds as date,
# MAGIC       past_list_cost is null as is_prediction,
# MAGIC       coalesce(past_list_cost, cost_at_list_price_forecast) as list_cost,
# MAGIC       avg(coalesce(past_list_cost, cost_at_list_price_forecast)) OVER (PARTITION BY sku, workspace_id ORDER BY ds ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS list_cost_ma,
# MAGIC       avg(cost_at_list_price_forecast) OVER (PARTITION BY sku, workspace_id ORDER BY ds ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_ma, 
# MAGIC       avg(cost_at_list_price_lower) OVER (PARTITION BY sku, workspace_id ORDER BY ds ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_lower_ma,
# MAGIC       avg(cost_at_list_price_upper) OVER (PARTITION BY sku, workspace_id ORDER BY ds ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_upper_ma
# MAGIC     from forecast_data
# MAGIC     ORDER BY sku, workspace_id, ds;
# MAGIC
# MAGIC SELECT * FROM main.dbdemos_billing_forecast.detailed_billing_forecast order by date desc limit 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.dbdemos_billing_forecast.billing_forecast order by ds desc limit 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual AI Forecast leveraging Prophet and pandas UDF with spark
# MAGIC
# MAGIC If the AI_FORECAST function isn't available yet in your workspace, you can do the prediction manually in python with prophet.
# MAGIC
# MAGIC **Note: if you already ran the AI_FORECAST function, you can skip these next steps** 

# COMMAND ----------

# MAGIC %pip install prophet==1.1.6

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

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
        future_pd['floor'] = 0  # Set the floor for future predictions
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
# MAGIC DROP TABLE IF EXISTS billing_forecast_manual;
# MAGIC -- create the billing forecast table:
# MAGIC CREATE TABLE IF NOT EXISTS billing_forecast_manual (ds DATE, yhat DOUBLE, yhat_upper DOUBLE, yhat_lower DOUBLE, y DOUBLE, dbus DOUBLE, sku STRING, workspace_id STRING, training_date DATE);

# COMMAND ----------

# DBTITLE 1,Forecast for the sum of all DBUS
#Sum all the SKUs & Workspace for global consumption trend (by default we want a view for all our billing usage across all workspace, so we need to train a specific model on that too)
global_forecast = data_to_predict_daily.groupBy(col("ds")).agg(sum('cost_at_list_price').alias("y"), sum('dbus').alias("dbus")) \
                                       .withColumn('sku', lit('ALL')) \
                                       .withColumn('workspace_id', lit('ALL')).toPandas()
global_forecast = generate_forecast(global_forecast)
spark.createDataFrame(global_forecast).withColumn('training_date', current_date()) \
                                      .write.mode('overwrite').option("mergeSchema", "true").saveAsTable("billing_forecast_manual")

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
results.write.mode('append').saveAsTable("billing_forecast_manual")

# COMMAND ----------

# DBTITLE 1,Our forecasting data is ready
# MAGIC %sql 
# MAGIC select * from billing_forecast_manual

# COMMAND ----------

# DBTITLE 1,Create a view on top to simplify access
# MAGIC %sql
# MAGIC create or replace view detailed_billing_forecast_manual as 
# MAGIC with forecasts as (
# MAGIC   select
# MAGIC     f.ds as date,
# MAGIC     f.workspace_id,
# MAGIC     f.sku,
# MAGIC     f.y,
# MAGIC     GREATEST(0, f.yhat) as yhat,
# MAGIC     GREATEST(0, f.yhat_lower) as yhat_lower,
# MAGIC     GREATEST(0, f.yhat_upper) as yhat_upper,
# MAGIC     f.training_date
# MAGIC   from main.dbdemos_billing_forecast.billing_forecast_manual f
# MAGIC )
# MAGIC select
# MAGIC   `date`,
# MAGIC   workspace_id,
# MAGIC   sku,
# MAGIC   y as past_list_cost, -- real
# MAGIC   y is null as is_prediction ,
# MAGIC   coalesce(y, yhat) as list_cost, -- contains real and forecast
# MAGIC   yhat as cost_at_list_price_forecast,  -- forecast
# MAGIC   yhat_lower as cost_at_list_price_lower,
# MAGIC   yhat_upper as cost_at_list_price_upper,
# MAGIC   -- smooth upper and lower bands for better visualization
# MAGIC   avg(yhat) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_ma, 
# MAGIC   avg(yhat_lower) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_upper_ma,
# MAGIC   avg(yhat_upper) OVER (PARTITION BY sku, workspace_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS forecast_list_cost_lower_ma,
# MAGIC   training_date
# MAGIC from
# MAGIC   forecasts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from detailed_billing_forecast_manual  where sku = 'ALL' limit 100

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

df = spark.table('detailed_billing_forecast_manual').where("sku = 'ALL' and workspace_id='ALL'").toPandas()
#Remove the last point as the day isn't completed (not relevant)
df.at[df['past_list_cost'].last_valid_index(), 'past_list_cost'] = None

# Create traces
fig = go.Figure()
fig.add_trace(go.Scatter(x=df['date'], y=df['past_list_cost'][:-4], name='actual usage'))
fig.add_trace(go.Scatter(x=df['date'], y=df['cost_at_list_price_forecast'], name='forecast cost (pricing list)'))
fig.add_trace(go.Scatter(x=df['date'], y=df['cost_at_list_price_upper'], name='forecast cost up', line = dict(color='grey', width=1, dash='dot')))
fig.add_trace(go.Scatter(x=df['date'], y=df['cost_at_list_price_lower'], name='forecast cost low', line = dict(color='grey', width=1, dash='dot')))

# COMMAND ----------

# DBTITLE 1,Detailed view for each SKU
import plotly.express as px
from pyspark.sql.functions import date_sub

one_year_ago = date_sub(current_date(), 365)
df = spark.table('detailed_billing_forecast_manual').where("workspace_id='ALL'").where(col('date') >= one_year_ago).groupBy('sku', 'date').agg(sum('list_cost').alias('list_cost')).orderBy('date').toPandas()
px.line(df, x="date", y="list_cost", color='sku')

# COMMAND ----------

# DBTITLE 1,Top 5 workspaces view
#Focus on the ALL PURPOSE dbu
df = spark.table('detailed_billing_forecast_manual').where("sku = 'ALL_PURPOSE' and workspace_id != 'ALL'")
#Get the top 5 worpskaces consuming the most
top_workspace = df.groupBy('workspace_id').agg(sum('list_cost').alias('list_cost')).orderBy(col('list_cost').desc()).limit(5)
workspace_id = [r['workspace_id'] for r in top_workspace.collect()]
#Group consumption per workspace
df = df.where(col('workspace_id').isin(workspace_id)).groupBy('workspace_id', 'date').agg(sum('list_cost').alias('list_cost')).orderBy('date').toPandas()
px.bar(df, x="date", y="list_cost", color="workspace_id", title="Long-Form Input")
