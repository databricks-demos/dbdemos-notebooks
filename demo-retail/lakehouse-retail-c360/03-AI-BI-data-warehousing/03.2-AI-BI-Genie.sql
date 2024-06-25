-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # AI / BI On Databricks: Genie
-- MAGIC
-- MAGIC Once your data ready, you can share it with your business users leveraging Databricks AI/BI Genie. 
-- MAGIC
-- MAGIC The data used to answer questions can be coming from your Dashboards, or from any table.
-- MAGIC
-- MAGIC Create a Genie room, share it with your business user and ask any question!
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=03.2-AI-BI-Genie&demo_name=lakehouse-retail-c360&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # BI & Datawarehousing with Databricks SQL
-- MAGIC
-- MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-churn-ai-bi.png?raw=true" />
-- MAGIC
-- MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
-- MAGIC
-- MAGIC Let's explore how Databricks SQL support your Data Analyst team with interactive BI and start analyzing our customer Churn.
-- MAGIC
-- MAGIC To start with Databricks SQL, open the SQL view on the top left menu.
-- MAGIC
-- MAGIC You'll be able to:
-- MAGIC
-- MAGIC - Create a SQL Warehouse to run your queries
-- MAGIC - Use DBSQL to build your own dashboards
-- MAGIC - Plug any BI tools (Tableau/PowerBI/..) to run your analysis

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Databricks SQL Warehouses: best-in-class BI engine
-- MAGIC
-- MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
-- MAGIC
-- MAGIC Databricks SQL is a warehouse engine packed with thousands of optimizations to provide you with the best performance for all your tools, query types and real-world applications. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>It won the Data Warehousing Performance Record.</a>
-- MAGIC
-- MAGIC This includes the next-generation vectorized query engine Photon, which together with SQL warehouses, provides up to 12x better price/performance than other cloud data warehouses.
-- MAGIC
-- MAGIC **Serverless warehouse** provide instant, elastic SQL compute — decoupled from storage — and will automatically scale to provide unlimited concurrency without disruption, for high concurrency use cases.
-- MAGIC
-- MAGIC Make no compromise. Your best Datawarehouse is a Lakehouse.
-- MAGIC
-- MAGIC ### Creating a SQL Warehouse
-- MAGIC
-- MAGIC SQL Wharehouse are managed by databricks. [Creating a warehouse](/sql/warehouses) is a 1-click step: 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Creating your first Query
-- MAGIC
-- MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-query.png" />
-- MAGIC
-- MAGIC Our users can now start running SQL queries using the SQL editor and add new visualizations.
-- MAGIC
-- MAGIC By leveraging auto-completion and the schema browser, we can start running adhoc queries on top of our data.
-- MAGIC
-- MAGIC While this is ideal for Data Analyst to start analysing our customer Churn, other personas can also leverage DBSQL to track our data ingestion pipeline, the data quality, model behavior etc.
-- MAGIC
-- MAGIC Open the [Queries menu](/sql/queries) to start writting your first analysis.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Creating our Churn Dashboard
-- MAGIC
-- MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png" />
-- MAGIC
-- MAGIC The next step is now to assemble our queries and their visualization in a comprehensive SQL dashboard that our business will be able to track.
-- MAGIC
-- MAGIC The Dashboard has been loaded for you. Open the <a dbdemos-dashboard-id="churn-universal" href="/sql/dashboards/b14e9d86-478d-4ea8-83db-8211cee8d3fc">DBSQL Churn Dashboard</a> to start reviewing our Churn stats.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Using Third party BI tools
-- MAGIC
-- MAGIC <iframe style="float: right" width="560" height="315" src="https://www.youtube.com/embed/EcKqQV0rCnQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
-- MAGIC
-- MAGIC SQL warehouse can also be used with an external BI tool such as Tableau or PowerBI.
-- MAGIC
-- MAGIC This will allow you to run direct queries on top of your table, with a unified security model and Unity Catalog (ex: through SSO). Now analysts can use their favorite tools to discover new business insights on the most complete and freshest data.
-- MAGIC
-- MAGIC To start using your Warehouse with third party BI tool, click on "Partner Connect" on the bottom left and chose your provider.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Going further with DBSQL & Databricks Warehouse
-- MAGIC
-- MAGIC Databricks SQL offers much more and provides a full warehouse capabilities
-- MAGIC
-- MAGIC <img style="float: right" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-pk-fk.png" />
-- MAGIC
-- MAGIC ### Data modeling
-- MAGIC
-- MAGIC Comprehensive data modeling. Save your data based on your requirements: Data vault, Star schema, Inmon...
-- MAGIC
-- MAGIC Databricks let you create your PK/FK, identity columns (auto-increment): `dbdemos.install('identity-pk-fk')`
-- MAGIC
-- MAGIC ### Data ingestion made easy with DBSQL & DBT
-- MAGIC
-- MAGIC Turnkey capabilities allow analysts and analytic engineers to easily ingest data from anything like cloud storage to enterprise applications such as Salesforce, Google Analytics, or Marketo using Fivetran. It’s just one click away. 
-- MAGIC
-- MAGIC Then, simply manage dependencies and transform data in-place with built-in ETL capabilities on the Lakehouse (Delta Live Table), or using your favorite tools like dbt on Databricks SQL for best-in-class performance.
-- MAGIC
-- MAGIC ### Query federation
-- MAGIC
-- MAGIC Need to access cross-system data? Databricks SQL query federation let you define datasources outside of databricks (ex: PostgreSQL)
-- MAGIC
-- MAGIC ### Materialized view
-- MAGIC
-- MAGIC Avoid expensive queries and materialize your tables. The engine will recompute only what's required when your data get updated. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Taking our analysis one step further: Predicting Churn
-- MAGIC
-- MAGIC Being able to run analysis on our past data already gives us a lot of insight. We can better understand which customers are churning evaluate the churn impact.
-- MAGIC
-- MAGIC However, knowing that we have churn isn't enough. We now need to take it to the next level and build a predictive model to determine our customers at risk of churn to be able to increase our revenue.
-- MAGIC
-- MAGIC Let's see how this can be done with [Databricks Machine Learning notebook]($../04-Data-Science-ML/04.1-automl-churn-prediction) | go [Go back to the introduction]($../00-churn-introduction-lakehouse)
