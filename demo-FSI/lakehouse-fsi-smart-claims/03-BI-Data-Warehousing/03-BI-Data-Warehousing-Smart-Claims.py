# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Your Lakehouse is the best Warehouse
# MAGIC
# MAGIC Traditional Data Warehouses can’t keep up with the variety of data and use cases. Business agility requires reliable, real-time data, with insight from ML models.
# MAGIC
# MAGIC Working with the lakehouse unlock traditional BI analysis but also real time applications having a direct connection to your entire data, while remaining fully secured.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/dbsql.png" width="700px" style="float: left" />
# MAGIC
# MAGIC <div style="float: left; margin-top: 240px; font-size: 23px">
# MAGIC   Instant, elastic compute<br>
# MAGIC   Lower TCO with Serveless<br>
# MAGIC   Zero management<br><br>
# MAGIC
# MAGIC   Governance layer - row level<br><br>
# MAGIC
# MAGIC   Your data. Your schema (star, data vault…)
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffsi%2Flakehouse_credit_scoring%2Fbi&dt=LAKEHOUSE_CREDIT_SCORING">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks SQL Warehouses: best-in-class BI engine
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
# MAGIC
# MAGIC Databricks SQL is a warehouse engine packed with thousands of optimizations to provide you with the best performance for all your tools, query types and real-world applications. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>It won the Data Warehousing Performance Record.</a>
# MAGIC
# MAGIC This includes the next-generation vectorized query engine Photon, which together with SQL warehouses, provides up to 12x better price/performance than other cloud data warehouses.
# MAGIC
# MAGIC **Serverless warehouse** provide instant, elastic SQL compute — decoupled from storage — and will automatically scale to provide unlimited concurrency without disruption, for high concurrency use cases.
# MAGIC
# MAGIC Make no compromise. Your best Datawarehouse is a Lakehouse.
# MAGIC
# MAGIC ### Creating a SQL Warehouse
# MAGIC
# MAGIC SQL Wharehouse are managed by databricks. [Creating a warehouse](/sql/warehouses) is a 1-click step: 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating your first Query
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-query.png" />
# MAGIC
# MAGIC Our users can now start running SQL queries using the SQL editor and add new visualizations.
# MAGIC
# MAGIC By leveraging auto-completion and the schema browser, we can start running adhoc queries on top of our data.
# MAGIC
# MAGIC While this is ideal for Data Analyst to start analysing our customer Churn, other personas can also leverage DBSQL to track our data ingestion pipeline, the data quality, model behavior etc.
# MAGIC
# MAGIC Open the [Queries menu](/sql/queries) to start writting your first analysis.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A Summary dashboard gives a birds eye view to overall business operations
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/smart-claims/summary_dashboard.png" width="800px" style="float: left; margin-right: 50px">
# MAGIC
# MAGIC
# MAGIC * Loss Ratio is computed by insurance claims paid plus adjustment expenses divided by total earned premiums.
# MAGIC For example, if a company pays $80 in claims for every $160 in collected premiums, the loss ratio would be 50%.
# MAGIC The lower the ratio, the more profitable the insurance company. Each insurance company has its target loss ratio. A typical range is between 40%-60%.
# MAGIC * Damage is captured in 2 categories - property & liability - their loss ratios are tracked separately
# MAGIC The 80/20 Rule generally requires insurance companies to spend at least 80% of the money they take in from premiums on care costs and quality improvement activities. The other 20% can go to administrative, overhead, and marketing costs.
# MAGIC * Summary visualization captures count of incident type by severity
# MAGIC * Incident type refers to damage on account of
# MAGIC theft, collision (at rest, in motion (single/multiple vehicle collision)
# MAGIC * Damage Severity is categorized as a trivial, minor, major, total loss
# MAGIC * Analyzing recent trends helps to prepare for handling similar claims in the near future, for Eg.
# MAGIC * What is the frequency of incident/damage amount by hour of day
# MAGIC * Are there certain times in a day such as peak hours that are more prone to incidents?
# MAGIC * Is there a corelation to the age of the driver and the normalized age of the driver
# MAGIC * Note there are very few driver below or above a certain threshold
# MAGIC * What about the number of incident coreelated to the age/make of the vehicle.
# MAGIC * Which areas of the city have a higher incidence rate(construction, congestion, layout, density, etc)
# MAGIC <br/><br/> 
# MAGIC
# MAGIC
# MAGIC <b><a dbdemos-dashboard-id="claims-report" href='/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956' target="_blank">Open the Summary Report Dashboard</a> </b>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Claims Investigation Dashboard
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/smart-claims/ClaimsInvestigation.png" width="800px"  style="float: right; margin-right: 10px">
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC A per claim Investigation dashboard gives additional where a claims officer picks a claim number and can drill into its various facets
# MAGIC
# MAGIC - The first panel uses counter widgets to provide statistics on rolling counts on number of
# MAGIC   - Claims filed and of those how many were flagged as 
# MAGIC   - suspicious or
# MAGIC   - had expired policies or
# MAGIC   - had a severity assessment mimatch or
# MAGIC   - claims amount exceeded the policy limits
# MAGIC - The next widget uses a table view to provide recent claims that are auto scored in the pipeline using ML iferecing and rule engine
# MAGIC   - A green tick is used to denote auto-assessmentt matches claims description
# MAGIC   - A red cross indicates a mismatch that warrants further manual investigation
# MAGIC - Drill down to a specific claim to see
# MAGIC   - Images of the damaged vehicle
# MAGIC   - Claim, Policy & Driver details
# MAGIC   - Telematic data draws the path taken by the vehicle
# MAGIC   - Reported data is contrasted with assessed data insight 
# MAGIC
# MAGIC <b><a dbdemos-dashboard-id="claims-investigation" href='/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956' target="_blank">Open the Investigation Dashboard</a> </b>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Orchestrating with Workflow
# MAGIC
# MAGIC We're now ready to create an end to end job to deploy our entire pipeline in production, from data ingestion to model serving and DBSQL Dashboard.
# MAGIC
# MAGIC Open the [04-Workflow-Orchestration-Smart-Claims]($../04-Workflow-Orchestration/04-Workflow-Orchestration-Smart-Claims) notebook to see how Databricks can orchestrate your pipelines.
