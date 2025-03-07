# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Deploying Your AI Functions with Databricks AI Agents
# MAGIC
# MAGIC In this notebook, you'll learn how to take the functions you defined in your previous Databricks notebook and integrate them into a **Databricks AI Agent**. This will allow you to use them in applications, the Databricks Playground, or other contexts where AI-driven functionality is needed.
# MAGIC
# MAGIC We'll walk through the process step by step, with **GIFs** to guide you along the way. By the end, you'll have a working AI Agent powered by your own functions, ready to deploy and use. Let's get started!

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
# MAGIC
# MAGIC
# MAGIC <p align="center">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_open_playground.gif" 
# MAGIC     alt="Opening the Playground" 
# MAGIC     width="40%"
# MAGIC   >
# MAGIC </p>
# MAGIC
# MAGIC ### Location Guide
# MAGIC
# MAGIC TODO: add playground link directlty to open as a new window
# MAGIC
# MAGIC Find the Playground under the **Machine Learning** section in your left sidebar.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3: Configure Your Agent Functions
# MAGIC
# MAGIC <p align="center">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_function_selection.gif" 
# MAGIC     alt="Function Selection" 
# MAGIC     width="40%"
# MAGIC   >
# MAGIC </p>
# MAGIC
# MAGIC ### Function Location Guide
# MAGIC Your functions are organized in Unity Catalog using this structure:
# MAGIC
# MAGIC ```
# MAGIC my_catalog.my_schema.function_name
# MAGIC ```
# MAGIC
# MAGIC #### Example Path:
# MAGIC * **Catalog:** my_catalog
# MAGIC * **Schema:** my_schema
# MAGIC * **Function:** my_awesome_function
# MAGIC
# MAGIC > 💡 Note: Replace the example names with your actual catalog and schema names.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4: Export Your Agent
# MAGIC
# MAGIC <p align="center">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_export_from_playground.gif" 
# MAGIC     alt="Exporting Agent" 
# MAGIC     width="40%"
# MAGIC   >
# MAGIC </p>
# MAGIC
# MAGIC ### Export Checklist
# MAGIC * ✅ Click the "Export" button in the Playground
# MAGIC * ✅ Save to your demo's folder for better organization
# MAGIC * ✅ Verify all selected functions are included

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Step 5: Deploy Your AI Agent
# MAGIC
# MAGIC ### Final Steps
# MAGIC 1. Navigate to your exported notebooks
# MAGIC 2. Follow the AI Agent's deployment instructions
# MAGIC 3. Close this guide
# MAGIC
# MAGIC ### What's next after you finish this notebook?
# MAGIC * 🔍 **Agent Evaluation:** Test and validate performance
# MAGIC * 🌟 **Agent Deployment:** Place your agent in a Databricks App or in the Playground for others to use!
# MAGIC * ⚙️ **Additional Agent Features:** _and much more!_
