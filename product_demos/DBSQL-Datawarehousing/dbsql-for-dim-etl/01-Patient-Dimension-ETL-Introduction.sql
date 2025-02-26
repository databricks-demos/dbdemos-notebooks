-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Demo: Create and Populate Patient Dimension
-- MAGIC
-- MAGIC The demo will illustrate the data architecture and data workflow that creates and populates a dimension in a Star Schema using **Databricks SQL**.<br>
-- MAGIC This will utilize a Patient dimension in the Healthcare domain.<br>
-- MAGIC <br>
-- MAGIC The demo will illustrate all facets of an end-to-end ETL to transform, validate, and load an SCD2 dimension.
-- MAGIC
-- MAGIC NOTE: The ETL assumes that the source data is extracted to cloud storage as incremental CSV files.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## <span style="color:blue">What we will build
-- MAGIC
-- MAGIC #### This end-to-end demo builds a Databricks Workflows Job that will perform the following tasks:
-- MAGIC
-- MAGIC **<span style="color:orange">1. <u>Create Tables</u>**
-- MAGIC <br>
-- MAGIC A. Global Configuration
-- MAGIC -  **ETL Log table**: This table captures the runtime metadata for a table that includes the table name, load start time and load end time.
-- MAGIC  
-- MAGIC _See [Create Config Table notebook]($./01-Create/Config Table) to review._
-- MAGIC
-- MAGIC B. Standardization<br>
-- MAGIC -  **Codes table**: Master table initialized with standardized codes used for coded attributes in the schema.<br>
-- MAGIC
-- MAGIC _See [Create Code Table notebook]($./01-Create/Code Table) to review._
-- MAGIC
-- MAGIC C. Patient tables<br>
-- MAGIC - **Patient Staging table**<br>
-- MAGIC - **Patient Integration table<br>**
-- MAGIC - **Patient Dimension table<br>**
-- MAGIC
-- MAGIC _See [Create Patient Tables notebook]($./01-Create/Patient Tables) to review._
-- MAGIC
-- MAGIC **<span style="color:orange">2. <u>Stage Initial Data</u>**<br>
-- MAGIC   This task will download an initial CSV file with patient data onto a staging Volume.
-- MAGIC
-- MAGIC **<span style="color:orange">3. <u>Patient load</u>**<br>
-- MAGIC This will initiate the ETL which will read new files from the staging Volume and populate the staging, integration, and patient dimension tables.
-- MAGIC
-- MAGIC **<span style="color:orange">4. <u>Stage Incremental Data</u>**<br>
-- MAGIC   This task will download 2 incremental CSV files with patient data onto the staging Volume.
-- MAGIC
-- MAGIC **<span style="color:orange">5. <u>Patient load</u>**<br>
-- MAGIC This will initiate the ETL which will read new files from the staging Volume and populate the staging, integration, and patient dimension tables.
-- MAGIC
-- MAGIC _See [Patient Dimension ETL notebook]($./02-Populate/Patient Dimension ETL) to review._
-- MAGIC
-- MAGIC <br>
-- MAGIC
-- MAGIC You can also browse the results of each ETL run. This will show the data that is present in the log, exceptions, and patient tables, as it appears at the end of the initial load and each incremental load. Click on each of the 'Browse Results' task.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## <span style="color:blue">Patient tables
-- MAGIC
-- MAGIC ![](https://github.com/shyamraodb/star-schema-elt/blob/0e2d39288f77c7b500af6565f0eec27b140d7f6a/images/patient_tables_dw.png?raw=true)
-- MAGIC
-- MAGIC You can view the tables within the catalog.schema that is specified in notebook 00-Setup/Initialize.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## <span style="color:blue">Sample Source Data
-- MAGIC
-- MAGIC ![](https://github.com/shyamraodb/star-schema-elt/blob/main/images/patient_data.png?raw=true)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## <span style="color:blue">Data Flow
-- MAGIC
-- MAGIC ![](https://github.com/shyamraodb/star-schema-elt/blob/main/images/data_flow_no_excpt.png?raw=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create Workflows Job
-- MAGIC
-- MAGIC Open and Run notebook [02-Create-SQL-Warehouse-Workflows-Job]($./02-Create-SQL-Warehouse-Workflows-Job)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC