# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Asset Bundles
# MAGIC Databricks Asset Bundles are the recommended approach to CI/CD on Databricks. Use Databricks Asset Bundles to describe Databricks resources such as jobs and pipelines as source files, and bundle them together with other assets to provide an end-to-end definition of a deployable project. These bundles of files can be source controlled, and you can use external CI/CD automation such as Github Actions to trigger deployments.
# MAGIC
# MAGIC You can use Databricks Asset Bundles to define and programmatically manage your Databricks CI/CD implementation, which usually includes:
# MAGIC
# MAGIC * **Notebooks**: Databricks notebooks are often a key part of data engineering and data science workflows. You can use version control for notebooks, and also validate and test them as part of a CI/CD pipeline. You can run automated tests against notebooks to check whether they are functioning as expected.
# MAGIC * **Libraries**: Manage the library dependencies required to run your deployed code. Use version control on libraries and include them in automated testing and validation.
# MAGIC * **Workflows**: Lakeflow Jobs are comprised of jobs that allow you to schedule and run automated tasks using notebooks or Spark jobs.
# MAGIC * **Data pipelines**: You can also include data pipelines in CI/CD automation, using Spark Declarative Pipelines, the framework in Databricks for declaring data pipelines.
# MAGIC * **Infrastructure**: Infrastructure configuration includes definitions and provisioning information for clusters, workspaces, and storage for target environments. Infrastructure changes can be validated and tested as part of a CI/CD pipeline, ensuring that they are consistent and error-free.
# MAGIC
# MAGIC ## Example Development Workflow
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/declarative-pipelines/bundles-cicd.png?raw=true" />
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## The basics of `databricks.yml`
# MAGIC Each bundle must contain exactly one configuration file named `databricks.yml`, typically at the root of the project folder. The most simple `databricks.yml` you can create defines the bundle `name`, and a default `target`. For example:  
# MAGIC ```yml
# MAGIC bundle:
# MAGIC   name: my_bundle
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     default: true
# MAGIC ```
# MAGIC
# MAGIC Resources you want to deploy can either be included directly in this `databricks.yml` or defined in additional yml files by using the `include` configuration. Here's an example of a simple asset bundle that deploys a job.
# MAGIC
# MAGIC ```yml
# MAGIC bundle:
# MAGIC   name: my_bundle
# MAGIC
# MAGIC # Use include to break up bundle into multiple files. 
# MAGIC # Paths within a bundle are always relative to the yml they're defined in.
# MAGIC include:
# MAGIC   - resources/*.yml
# MAGIC
# MAGIC # Targets that you can deploy into
# MAGIC # These can be different workspaces or different configurations of the pipeline
# MAGIC targets:
# MAGIC   dev:
# MAGIC     default: true
# MAGIC     workspace:
# MAGIC       host: https://company.cloud.databricks.com
# MAGIC
# MAGIC
# MAGIC # Resources can be defined in multiple files
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     # The resource name used here is used to reference this job elsewhere in your DAB if needed
# MAGIC     default_python_job:
# MAGIC       # Use the REST API Documentation to understand what configuration options are available for each resource type
# MAGIC       name: default_python_job
# MAGIC       tasks:
# MAGIC         - task_key: notebook_task
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/notebook.ipynb
# MAGIC ```
# MAGIC
# MAGIC See more examples and read the full documentation [here](https://docs.databricks.com/aws/en/dev-tools/bundles/).

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Databricks Asset Bundles with Spark Declarative Pipelines
# MAGIC For each of the resource types you can deploy using DABs, you can refer to the Databricks REST API documentation to see what fields are available. Here's a (incomplete) list of configuration options for pipelines today.
# MAGIC ```yml
# MAGIC resources:
# MAGIC     pipelines:
# MAGIC         <pipeline-resource-name>: # DAB resource name for reference within asset bundle (this does not impact the deployed pipeline)
# MAGIC             name: <pipeline name> # Friendly identifier for this pipeline.
# MAGIC             catalog: <catalog> # A catalog in Unity Catalog to publish data from this pipeline to. 
# MAGIC             schema: <schema> # The default schema (database) where tables are read from or published to.
# MAGIC             root_path: <pipeline root> # Relative path for the root of this pipeline. This is used as the root directory when editing the pipeline in the Databricks user interface and it is added to sys.path when executing Python sources during pipeline execution.
# MAGIC             development: <true/false> # Whether the pipeline is in Development mode. Defaults to false.
# MAGIC             libraries:
# MAGIC                 - glob:
# MAGIC                     include: <path to folder or file> # Files to include as part of the pipeline. Path can be a notebook, a sql or python file or a folder path that ends in /**
# MAGIC                 - glob:
# MAGIC                     include: <path to folder or file> # Multiple paths can be included here
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC See all the available configurations for pipelines [here](https://docs.databricks.com/api/workspace/pipelines/create).

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploying this demo using DABs
# MAGIC
# MAGIC 1. Install the Databricks CLI and authenticate to your workspace. [See the Databricks documentation for instructions on how to set the CLI up.](https://docs.databricks.com/aws/en/dev-tools/cli/tutorial)
# MAGIC
# MAGIC 2. Download the pipeline-bike folder from your workspace onto your local computer. Use the "Download as Zip (Notebook Source + File)" option, and unzip the folder once it's downloaded.
# MAGIC
# MAGIC 3. Copy `databricks.yml` from the deployment folder into the pipeline-bike folder and update the catalog, schema and workspace host values to reflect your environment.
# MAGIC     ```yml
# MAGIC     ...
# MAGIC
# MAGIC     variables:
# MAGIC     catalog:
# MAGIC       description: Default catalog that pipeline will publish assets to
# MAGIC       default: <replace with your catalog>
# MAGIC     schema:
# MAGIC       description: Default schema that pipeline will publish assets to when no schema is specified in code
# MAGIC       default: <replace with your schema>
# MAGIC     
# MAGIC     ...
# MAGIC
# MAGIC     targets:
# MAGIC     dev:
# MAGIC       mode: development
# MAGIC       default: true
# MAGIC       workspace:
# MAGIC         host: <replace with your workspace>
# MAGIC     
# MAGIC     ...
# MAGIC
# MAGIC     ```
# MAGIC
# MAGIC 4. From a terminal, run `databricks bundle validate`. If there are any errors, make sure to review them and address them as necessary.
# MAGIC     ```bash
# MAGIC       $ databricks bundle validate
# MAGIC   
# MAGIC       Name: pipeline-bike
# MAGIC       Target: dev
# MAGIC       Workspace:
# MAGIC         Host: https://company.cloud.databricks.com/
# MAGIC         User: user@company.com
# MAGIC         Path: /Workspace/Users/user@company.com/.bundle/pipeline-bike/dev
# MAGIC     ```
# MAGIC
# MAGIC 5. Run `databricks bundle deploy` to deploy the bundle.
# MAGIC     ```bash
# MAGIC       $ databricks bundle deploy
# MAGIC
# MAGIC       Uploading bundle files to /Workspace/Users/user@company.com/.bundle/pipeline-bike/dev/files...
# MAGIC       Deploying resources...
# MAGIC       Updating deployment state...
# MAGIC       Deployment complete!
# MAGIC     ```
# MAGIC
# MAGIC 6. Run `databricks bundle run generate_bike_data` to kick off a job to populate the raw data and start the pipeline.
# MAGIC    ```bash
# MAGIC     databricks bundle run generate_bike_data
# MAGIC     Run URL: https://company.cloud.databricks.com/...
# MAGIC
# MAGIC     2025-08-29 14:35:33 "[dev user_name] init-pipeline-bike" RUNNING
# MAGIC     2025-08-29 14:40:05 "[dev user_name] init-pipeline-bike" TERMINATED SUCCESS
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## More Examples
# MAGIC To see more examples of building asset bundles for pipeliens, check out the lakeflow_pipelines_python and lakeflow_pipelines_sql examples in the [bundle-examples](https://github.com/databricks/bundle-examples/tree/main) Github repository.