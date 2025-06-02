# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Deploying Your AI Functions with Databricks AI Agents
# MAGIC
# MAGIC In this notebook, you'll learn how to take the functions you defined in your previous Databricks notebook and integrate them into a **Databricks AI Agent**. This will allow you to use them in applications, the Databricks Playground, or other contexts where AI-driven functionality is needed.
# MAGIC
# MAGIC We'll walk through the process step by step, with **GIFs** to guide you along the way. By the end, you'll have a working AI Agent powered by your own functions, ready to deploy and use. Let's get started!
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05.2-Agent-Creation-Guide&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Your Workspace
# MAGIC
# MAGIC ### Quick Setup Tips
# MAGIC * 📌 Duplicate this browser window
# MAGIC * 💡 Keep this guide open for reference
# MAGIC * 🎯 Arrange windows side-by-side
# MAGIC
# MAGIC This simple preparation will make your journey much smoother! 😎

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2: Access the Databricks Playground
# MAGIC
# MAGIC <hr>
# MAGIC
# MAGIC <div style="float: right; width: 70%;">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_open_playground.gif" 
# MAGIC     alt="Opening the Playground" 
# MAGIC     width="100%"
# MAGIC   >
# MAGIC </div>
# MAGIC
# MAGIC ### Location Guide
# MAGIC
# MAGIC Find the **Playground** under the **Machine Learning** section in your Databricks Workspace's left sidebar.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3: Configure Your Agent Functions
# MAGIC
# MAGIC <hr>
# MAGIC
# MAGIC <div style="float: right; width: 70%;">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_function_selection.gif" 
# MAGIC     alt="Function Selection" 
# MAGIC     width="100%"
# MAGIC   >
# MAGIC </div>
# MAGIC
# MAGIC ### Location Guide
# MAGIC
# MAGIC Your functions are organized in Unity Catalog using this structure:
# MAGIC
# MAGIC #### Example Path:
# MAGIC `my_catalog.my_schema.my_awesome_function`
# MAGIC
# MAGIC 💡 Note: Replace the example names with your actual catalog and schema names.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4: Export Your Agent
# MAGIC
# MAGIC <hr>
# MAGIC
# MAGIC <div style="float: right; width: 70%;">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_export_from_playground.gif" 
# MAGIC     alt="Exporting Agent" 
# MAGIC     width="100%"
# MAGIC   >
# MAGIC </div>
# MAGIC
# MAGIC ### Export Checklist
# MAGIC * ✅ Verify all of the functions from 05.1-Agent-Functions-Creation (_or more that you may have added_) are **selected tools** in the Playground.
# MAGIC * ✅ Click the "_Export_" button in the Playground
# MAGIC * ✅ Save the exported notebooks to this directory (**"YOUR_WORKSPACE_PATH/05-Generative-AI/"**)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Step 5: Deploy Your AI Agent
# MAGIC
# MAGIC ### Final Steps
# MAGIC 1. Navigate to where your exported "driver" notebook is located.
# MAGIC 2. Follow that notebook's documentation and guide.
# MAGIC 3. Close this notebook. You may proceeed to using the "driver" and "agent" notebooks from here. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### What's next after you finish this notebook?
# MAGIC * 🔍 **Agent Evaluation:** Test and validate performance
# MAGIC * 🌟 **Agent Deployment:** Place your agent in a Databricks App or in the Playground for others to use!
# MAGIC * ⚙️ **Additional Agent Features:** _and much more!_
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Interested in finishing up this demo? Check out [in the orchestration notebook]($../06-Workflow-orchestration/06-Workflow-orchestration-fsi-fraud)
