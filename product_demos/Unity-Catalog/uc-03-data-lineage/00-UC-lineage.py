# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Show Lineage for Delta Tables in Unity Catalog
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/lineage/uc-lineage-slide.png?raw=true" style="float:right; margin-left:10px" width="700"/>
# MAGIC
# MAGIC Unity Catalog captures runtime data lineage for any table to table operation executed on a Databricks cluster or SQL endpoint. Lineage operates across all languages (SQL, Python, Scala and R) and it can be visualized in the Data Explorer in near-real-time, and also retrieved via REST API.
# MAGIC
# MAGIC Lineage is available at two granularity levels:
# MAGIC - Tables
# MAGIC - Columns: ideal to track GDPR dependencies
# MAGIC
# MAGIC Lineage takes into account the Table ACLs present in Unity Catalog. If a user is not allowed to see a table at a certain point of the graph, its information are redacted, but they can still see that a upstream or downstream table is present.
# MAGIC
# MAGIC Lineage can also include **external assets and workflows** that are run **outside** of Databricks. This external lineage metadata feature is in Public Preview. See [Bring your own data lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/external-lineage).
# MAGIC
# MAGIC ## Working with Lineage
# MAGIC
# MAGIC No modifications are needed to the existing code to generate lineage. As long as you operate with tables saved in Unity Catalog, Databricks will capture all lineage information for you.
# MAGIC
# MAGIC ## Requirements:
# MAGIC
# MAGIC - Source and target tables must be registered in a Unity Catalog metastore.
# MAGIC - External assets (not in the metastore) must be added as external metadata objects and linked to registered securable objects.
# MAGIC - Queries must use Spark DataFrame APIs (e.g., Spark SQL functions returning a DataFrame) or Databricks SQL interfaces (notebooks, SQL query editor).
# MAGIC - To view lineage, users must have at least the `BROWSE` privilege on the parent catalog, and the catalog must be accessible from the workspace.
# MAGIC - Permissions are required on notebooks, jobs, or dashboards as per workspace access control settings.
# MAGIC - For UC-enabled pipelines, users must have `CAN VIEW` permission on the pipeline.
# MAGIC - Streaming lineage between Delta tables requires DBR `11.3 LTS+`.
# MAGIC - Column lineage for Lakeflow Declarative Pipelines requires DBR `13.3 LTS+`.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=00-UC-lineage&demo_name=uc-03-UC-lineage&event=VIEW">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md ## 1/ Create a Delta Table In Unity Catalog
# MAGIC
# MAGIC The first step is to create a Delta Table in Unity Catalog.
# MAGIC
# MAGIC We want to do that in SQL, to show multi-language support:
# MAGIC
# MAGIC 1. Use the `CREATE TABLE` command and define a schema
# MAGIC 1. Use the `INSERT INTO` command to insert some rows in the table

# COMMAND ----------

# DBTITLE 1,Display the active catalog you are working with
# MAGIC %sql
# MAGIC SELECT CURRENT_CATALOG()

# COMMAND ----------

# DBTITLE 1,I. Create the `MENU` table and Insert data
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS menu (recipe_id INT, app string, main string, desert string);
# MAGIC
# MAGIC DELETE from menu;
# MAGIC
# MAGIC INSERT INTO menu (recipe_id, app, main, desert)
# MAGIC   VALUES
# MAGIC     (1, "Ceviche", "Tacos", "Flan"),
# MAGIC     (2, "Tomato Soup", "Souffle", "Creme Brulee"),
# MAGIC     (3, "Chips", "Grilled Cheese", "Cheescake");

# COMMAND ----------

# MAGIC %md-sandbox ## 2/ Create a Delta Table from the Original table
# MAGIC
# MAGIC To show dependencies between tables, we create a new table using the `CREATE TABLE AS SELECT (CTAS)` statement from the previous table `menu`, concatenating three columns into a new one

# COMMAND ----------

# DBTITLE 1,II. Create the `dinner` table from the `menu` table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dinner AS
# MAGIC SELECT
# MAGIC   recipe_id,
# MAGIC   concat(app, " + ", main, " + ", desert) as full_menu
# MAGIC FROM
# MAGIC   menu;

# COMMAND ----------

# MAGIC %md-sandbox ## 3/ Create a Delta Table by joining Two Tables
# MAGIC
# MAGIC The last step is to create a third table as a join from the two previous ones. This time we will use Python instead of SQL.
# MAGIC
# MAGIC - We create a Dataframe with some random data formatted according to two columns, `id` and `recipe_id`
# MAGIC - We save this Dataframe as a new table, `main.lineage.price`
# MAGIC - We read as two Dataframes the previous two tables, `main.lineage.dinner` and `main.lineage.price`
# MAGIC - We join them on `recipe_id` and save the result as a new Delta table `main.lineage.dinner_price`

# COMMAND ----------

# DBTITLE 1,III. Create the `dinner_price` table by joining `dinner` and `price` tables
df = (
    spark.range(3)
    .withColumn("price", F.round(10 * F.rand(seed=42), 2))
    .withColumnRenamed("id", "recipe_id")
)

df.write.mode("overwrite").saveAsTable("price")

dinner = spark.read.table("dinner")
price = spark.read.table("price")

dinner_price = dinner.join(price, on="recipe_id")
dinner_price.write.mode("overwrite").saveAsTable("dinner_price")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Visualize Table Lineage
# MAGIC
# MAGIC The Table lineage can be visualized by folowing the steps below:
# MAGIC
# MAGIC 1. Select the `Catalog` explorer icon 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/icon/catalog-explorer-icon.png?raw=true" width="25"/> on the navigation bar to the left.
# MAGIC 2. Search for `uc_lineage` in the search tab.
# MAGIC 3. Expand the `dbdemos_uc_lineage` schema that is used for this demo.
# MAGIC 3. Click the kebab menu on any of these tables `dinner`, `menu` or `price` under the `dbdemos_uc_lineage` schema.
# MAGIC 4. Click the `Open in Catalog Explorer` option.
# MAGIC 5. Click the `Lineage` tab.
# MAGIC 6. Explore the page, feel free to click the `See Lineage Graph` option as well.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/lineage/lineage-table.gif?raw=true"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 5/ Visualize Column Lineage
# MAGIC
# MAGIC Lineage is also available at the column level, making it useful for tracking column dependencies and ensuring compliance with GDPR standards. Column-level lineage can also be accessed via the API.
# MAGIC
# MAGIC You can access the column lineage on the `Lineage graph` view by clicking the `+` icon at the end of the table box boundary, followed by clicking each column. In this case we see that the column `full_menu` in the `dinner` table is derived from the three columns `app`, `main`, and `desert` of the `menu` table:
# MAGIC <br/><br/>
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/lineage/lineage-column.gif?raw=true"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6/ Lineage Permission Model
# MAGIC
# MAGIC Lineage graphs share the same permission model as Unity Catalog. If a user does not have the `BROWSE` or `SELECT` privilege on a table, they cannot explore its lineage. 
# MAGIC
# MAGIC Lineage graphs display Unity Catalog objects across all workspaces attached to the metastore, as long as the user has adequate object permissions.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Conclusion
# MAGIC
# MAGIC Databricks Unity Catalog let you track data lineage out of the box.
# MAGIC
# MAGIC No extra setup required, just read and write from your table and the engine will build the dependencies for you. Lineage can work at a table level but also at the column level, which provide a powerful tool to track dependencies on sensible data.
# MAGIC
# MAGIC Lineage can also show you the potential impact updating a table/column and find who will be impacted downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Existing Limitations
# MAGIC
# MAGIC Review the data lineage documentation [[AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage#lineage-limitations), [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/data-lineage), [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage)] for the latest limitations.
