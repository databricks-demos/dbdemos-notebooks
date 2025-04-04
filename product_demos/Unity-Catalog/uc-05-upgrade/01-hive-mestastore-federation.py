# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Access your Hive Metastore catalog through Unity Catalog 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/uc/upgrade/hive-federation.png?raw=true"  style="float: right" width="600px">
# MAGIC
# MAGIC
# MAGIC ## Unity Catalog
# MAGIC Unity Catalog is a fine-grained governance solution for data and AI on the Databricks platform. It helps simplify security and governance of your data and AI assets by providing a central place to administer and audit access to data and AI assets.
# MAGIC Unity Catalog is a core component of Databricks and a large portion of the current and future capabilities rely on Unity Catalog.
# MAGIC
# MAGIC However, many systems still leave on legacy Hive Metastore, without any advanced governance capabilities. 
# MAGIC
# MAGIC <strong>Databricks let you add your external Hive Metastore through Federation, making them available while benefiting from Unity Catalog capabilities!</strong>
# MAGIC
# MAGIC ## Hive metastore federation, 
# MAGIC Hive metastore federation is a feature that enables Unity Catalog to govern tables that are stored in a Hive metastore. You can federate an external Hive metastore, AWS Glue, or a legacy internal Databricks Hive metastore.
# MAGIC
# MAGIC Hive metastore federation can be used for the following use cases:
# MAGIC
# MAGIC 1. As a step in the migration path to Unity Catalog, enabling incremental migration without code adaptation, with some of your workloads continuing to use data registered in your Hive metastore while others are migrated.<br/>
# MAGIC This use case is most suited for organizations that use a legacy internal Databricks Hive metastore today, because federated internal Hive metastores allow both read and write workloads.
# MAGIC
# MAGIC 1. To provide a longer-term hybrid model for organizations that must maintain some data in a Hive metastore alongside their data that is registered in Unity Catalog.<br/>
# MAGIC This use case is most suited for organizations that use an external Hive metastore or AWS Glue today, because foreign catalogs for these Hive metastores are read-only.
# MAGIC
# MAGIC Refer to the [Hive metastore federation documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/hms-federation) for further information.
# MAGIC
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=01-hive-mestastore-federation&demo_name=uc-05-upgrade&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying Hive Metastore Federation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Deployment of Hive metastore federation can be performed manually using the UI or using [UCX](https://databrickslabs.github.io/ucx/).
# MAGIC We recommend using UCX for more complex deployments where we have a large number of external locations.
# MAGIC This demo we will demonstrate both option.
# MAGIC
# MAGIC Hivemetastore federation can be applied to internal workspace metastores and to external metastores (such as aws glue).
# MAGIC
# MAGIC For more information and limitation refer to [Hive metastore federation documentation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/hms-federation).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1/ Account and Cloud Assets
# MAGIC Before we can enable HMS federation, we have to complete the following activities:
# MAGIC - [Create Account Groups](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-account-groups)
# MAGIC - [Assigning Metastore](https://databrickslabs.github.io/ucx/docs/reference/commands/#assign-metastore)
# MAGIC - [Migrate Permissions from Workspace Groups to Account Groups](https://databrickslabs.github.io/ucx/docs/reference/workflows/#group-migration-workflow)
# MAGIC - [Create Storage Credentials](https://databrickslabs.github.io/ucx/docs/reference/commands/#migrate-credentials)
# MAGIC - [Create Glue Service Credential (Optional)](https://databrickslabs.github.io/ucx/docs/reference/commands/#migrate-glue-credentials)
# MAGIC - [Create External Locations](https://databrickslabs.github.io/ucx/docs/reference/commands/#migrate-locations)
# MAGIC - [Create Catalogs and Schemas](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-catalogs-schemas)
# MAGIC
# MAGIC UCX can perform all these operations, or you can perform these manually.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2/ Manually Enabling Hive metastore federation
# MAGIC Once all the account and cloud assets were created enabling HMS federation is simple.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <iframe style="float:right" width="660" height="371" src="https://www.youtube.com/embed/G0Hw-ibQYNo" title="1 create catalog ui" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC ## 2.1/ Creating a Connection and Catalog
# MAGIC
# MAGIC Creating a connection is performed by navigating to Catalog/External Data/Connections tab and then clicking the "create connection" button.
# MAGIC Follow these steps:
# MAGIC 1. Type in the connection name (note that it will be used as the default catalog name with _catalog appended to it)
# MAGIC 2. Select "Hive Metastore" as the connection type.
# MAGIC 3. Select the Metastore type
# MAGIC     1. AWS Glue (AWS Only) - for AWS Glue you'll be required to create a UC compatible IAM Role and a service credential in UC.
# MAGIC     1. External - External Metastore (SQL Server/Postgress SQL for example). You would have to provide connection information and credentials for an external Metastore.
# MAGIC     1. Internal - Workspace local warehouse.
# MAGIC 4. Click "Next"
# MAGIC 5. Click "Create Connection"
# MAGIC 6. On the next page you can rename the Catalog and also assign the external locations and paths the catalog will have access to.
# MAGIC 7. You can set the access permission for the Catalog or leave the default and tend to it later.
# MAGIC 8. On the last page you can set tags to the newly created catalog.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <iframe style="float:right" width="660" height="371" src="https://www.youtube.com/embed/MqI6H6Nh6CI" title="2 secure catalog" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC ## 2.2/ Securing federated objects
# MAGIC The newly created catalog and databases/tables within it can be secured like any other native UC object.<br/>
# MAGIC All the UC capabilities are available for the federated schemas tables and views. Including:
# MAGIC - Access Cotrol
# MAGIC - Row level filters and Column level masking
# MAGIC - Delta Sharing
# MAGIC - Lineage<br/>
# MAGIC And more.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Using UCX to Enable Hive metastore federation
# MAGIC UCX is very handy when enabling Hive metastore federation. It helps with creating all the required principals, credentials and external locations.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3.1/ Enable HMS Federation
# MAGIC Before you can use UCX for Hive metastore federation you have to apply the `enable-hms-federation` [cli command](https://databrickslabs.github.io/ucx/docs/reference/commands/#enable-hms-federation). This will make sure that UCX creates all the required IAM roles for federation.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <iframe style="float:right" width="660" height="371" src="https://www.youtube.com/embed/lx2aHfRSyAo" title="3 create catalog ucx" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
# MAGIC
# MAGIC ## 3.2/ Creating Connection/Catalog
# MAGIC Creating the connection and catalog with UCX is done by issuing the `create-federated-catalog` [cli command](https://databrickslabs.github.io/ucx/docs/reference/commands/#create-federated-catalog).
