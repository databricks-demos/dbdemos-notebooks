# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Upgrade your tables to Databricks Unity Catalog
# MAGIC
# MAGIC Unity catalog provides all the features required to your data governance & security:
# MAGIC
# MAGIC - Table ACL
# MAGIC - Row level access with dynamic view
# MAGIC - Secure access to external location (blob storage)
# MAGIC - Lineage at row & table level for data traceability
# MAGIC - Traceability with audit logs
# MAGIC - Lineage
# MAGIC - Volumes
# MAGIC - Genie
# MAGIC - System Tables
# MAGIC - Delta Sharing
# MAGIC - Lakehouse federation
# MAGIC - And many more
# MAGIC
# MAGIC Because unity catalog is added as a supplement to your existing account, migrating your existing data to the new UC is very simple.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/uc-base-1.png?raw=true" style="float: right" width="700px"/> 
# MAGIC
# MAGIC Unity Catalog works with 3 layers:
# MAGIC
# MAGIC * CATALOG
# MAGIC * SCHEMA (or DATABASE)
# MAGIC * TABLE
# MAGIC
# MAGIC The table created without Unity Catalog are available under the default `hive_metastore` catalog, and they're scoped at a workspace level.
# MAGIC
# MAGIC New tables created with Unity Catalog will available at the account level, meaning that they're cross-workspace.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=00-Upgrade-database-to-UC&demo_name=uc-05-upgrade&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introducing UCX
# MAGIC
# MAGIC UCX is a toolkit for enabling Unity Catalog (UC) in your Databricks workspace. 
# MAGIC UCX provides commands and workflows for migrating tables and views to UC. UCX allows you to rewrite dashboards, jobs and notebooks to use the migrated data assets in UC. And there are many more features!
# MAGIC
# MAGIC UCX is a public source project developed and maintained by Databricks Labs.
# MAGIC
# MAGIC UCX documentation is available here: [https://databrickslabs.github.io/ucx/](https://databrickslabs.github.io/ucx/)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="float: right" width="300px">
# MAGIC
# MAGIC ![UC Migration](https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/ucx/ucx-migration-overview.gif?raw=true)
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ## UC Migration - Overview
# MAGIC Migrating to Unity Catalog requires the follwing steps:
# MAGIC * Assessing your current Databricks deployment
# MAGIC * Creating and Attaching a metastore
# MAGIC * Migrating Workspace Groups
# MAGIC * Upgrading Tables
# MAGIC * Upgrading Code
# MAGIC * Validation
# MAGIC
# MAGIC
# MAGIC ## Let's try it!
# MAGIC Using UCX to upgrade to Unity Catalog streamlines the process and reduces the risk.<br/>
# MAGIC To upgrade a workspace to Unity Catalog the following steps have to be followed.<br/>
# MAGIC 1. Installation
# MAGIC 2. Assessment
# MAGIC 3. Account and Cloud asset upgrade
# MAGIC 4. Table Migration
# MAGIC Always refer to the [documentation](https://databrickslabs.github.io/ucx/) when using UCX as the product evolves on a regular basis.
# MAGIC 5. Code linting/upgrade

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # 1/ Installation
# MAGIC
# MAGIC <iframe style="float:right" width="560" height="315" src="https://www.youtube.com/embed/7tu74nJwHJM?si=A8qzRLREotg5OYED" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC UCX (and other Databricks Labs projects) is now embeded with the [Databrick CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install).<br/>
# MAGIC The updated list of requirements for installation is avalailable in the [Documentation Site](https://databrickslabs.github.io/ucx/docs/installation/#installation-requirements).<br/>
# MAGIC Installing UCX is as simple as issuing the command `databricks labs install ucx`.
# MAGIC
# MAGIC During the installation you'll be prompted with a number of questions pertaining to the installation and use of UCX.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # 2/ Assessment
# MAGIC
# MAGIC <iframe style="float:right" width="560" height="315" src="https://www.youtube.com/embed/cihqXmfkLD4?si=0wfnK_WKK84G00kt" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC The next step is running the assessment workflow. The assessment workflow produce a report with an inventory of all the assets that needs to be upgraded to UC and all the potential problems and incompatibilities.
# MAGIC
# MAGIC Your assessment dashboard will be available for you in one command line: `databricks labs ucx ensure-assessment-run`
# MAGIC
# MAGIC More information is available in the [Documentation Site](https://databrickslabs.github.io/ucx/docs/reference/workflows/#assessment-workflow).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="float:right" width=600>
# MAGIC <img width=600 src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/ucx/ucx-cloud-account.png?raw=true">
# MAGIC </div>
# MAGIC
# MAGIC # 3/ Account and Cloud Assets
# MAGIC Before we can migrate tables, we have to complete the following steps to make sure your account and cloud policies matche your organization requirements:
# MAGIC - [Create Account Groups](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-account-groups)
# MAGIC - [Assigning Metastore](https://databrickslabs.github.io/ucx/docs/reference/commands/#assign-metastore)
# MAGIC - [Migrate Permissions from Workspace Groups to Account Groups](https://databrickslabs.github.io/ucx/docs/reference/workflows/#group-migration-workflow)
# MAGIC - [Table Mapping](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-table-mapping)
# MAGIC - [Create Uber Principal](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-uber-principal)
# MAGIC - [Create Storage Credentials](https://databrickslabs.github.io/ucx/docs/reference/commands/#migrate-credentials)
# MAGIC - [Create External Locations](https://databrickslabs.github.io/ucx/docs/reference/commands/#migrate-locations)
# MAGIC - [Create Catalogs and Schemas](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-catalogs-schemas)
# MAGIC
# MAGIC
# MAGIC UCX can perform all these operations, check the documentation for more detail!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # 4/ Table Migration
# MAGIC
# MAGIC <iframe style="float:right" width="560" height="315" src="https://www.youtube.com/embed/ywa8ya-7hww?si=U_68oxX3mbBurZof" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC The Table Migration workflow migrate multiple kind of tables and views. It can be run from the command line or by invoking the workflow.<br/>
# MAGIC The Table Migration workflow is deployed to the workspace when UCX is installed.<br/>
# MAGIC It can be invoked from the UI or using the `databricks labs ucx migrate-tables` command.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 5/ Update your code to use the new migrated table
# MAGIC Now that the tables and view are migrated, we can migrate the code.<br/>
# MAGIC Code linting highlights all the changes we have to make in our code.<br/>
# MAGIC It is composed of three steps that are performed by the linter:
# MAGIC - Scanning Code Resources - Gathering Code and Snippets from:
# MAGIC   - Jobs and dashboards
# MAGIC   - Notebooks
# MAGIC   - Files
# MAGIC   - Wheels
# MAGIC   - Eggs
# MAGIC   - Cluster configurations
# MAGIC   - PyPi
# MAGIC
# MAGIC - Graphing - Construct a dependency graph by resolving:
# MAGIC   - References from jobs to dependencies
# MAGIC   - References from notebooks to notebooks
# MAGIC   - References from files to files
# MAGIC   - Breaking jobs into tasks
# MAGIC   - Breaking notebooks into cells
# MAGIC
# MAGIC - Linting - Lint the graph to find:
# MAGIC   - Direct file system access
# MAGIC   - Table references
# MAGIC   - JVM access
# MAGIC   - RDD API
# MAGIC   - SQL context
# MAGIC   - Spark logging
# MAGIC   - dbutils.notebook.run
# MAGIC   - And more!
# MAGIC
# MAGIC UCX has multiple options of migrating code:
# MAGIC - Workflow Linting
# MAGIC - Static Code Linting
# MAGIC - Dashboard Migration
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Analyzing your Workflow for UC updates
# MAGIC <iframe style="float:right" width="560" height="315" src="https://www.youtube.com/embed/OOlik-ostRc?si=jBG-rLk5rPvAiiuC" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC UCX can analyze the code related to all workflows or to a specific set of workflows.<br/>
# MAGIC Analyzing the workflows is performed as part of the assessment.
# MAGIC
# MAGIC Analyzing the workflows involves the following steps:
# MAGIC 1. Create a graph of all the dependent tasks/notebooks/scripts
# MAGIC 2. Analyze all the code related to the workflows
# MAGIC 3. Report the findings to the assessment tables
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Static Code Linting
# MAGIC
# MAGIC <iframe style="float:right" width="560" height="315" src="https://www.youtube.com/embed/9U7RgdPRsNY?si=HK44_mBRgVsHXIpP" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC Static code linting crawls code accessible by the workstation that runs the CLI. Typically it is used to analyze code that is hosted in a GIT repository and is cloned/pulled locally.<br/>
# MAGIC
# MAGIC Lint local code is inoked by the `databricks labs ucx lint-local-code` cli command.<br/>
# MAGIC Running this command from a modern IDE (such as PyCharm/IntelliJ/VSCode) will yield hyperlinks to the code issues.<br/>
# MAGIC
# MAGIC More information is available in the [documentation](https://databrickslabs.github.io/ucx/docs/reference/commands/#lint-local-code).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulation, you're ready to upgrade your asset and benefit from all Unity Catalog power!
# MAGIC
# MAGIC For more details, please use the [official UCX site](https://databrickslabs.github.io/ucx) or reach out to your account team.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interested by an example to migrate your tables manually?
# MAGIC
# MAGIC Check the [01-manual-upgrade-to-UC]($./01-manual-upgrade-to-UC) for a custom example upgrading your table to UC!
# MAGIC
