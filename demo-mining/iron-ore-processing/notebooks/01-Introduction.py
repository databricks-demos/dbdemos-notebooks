# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Predicting & Optimising Iron Ore Concentrations 
# MAGIC
# MAGIC This demo is a simplified, real-world-inspired example designed to showcase how Databricks enables end-to-end model development and deployment — from data ingestion and exploration to model training, optimisation, and operationalisation — all within a unified platform.
# MAGIC
# MAGIC In the context of iron ore processing, controlling iron (Fe) and silica (SiO₂) concentrations is critical for producing high-quality feedstock for steelmaking. One commonly used method in mineral processing is froth flotation, which removes silica from iron-bearing minerals like magnetite and hematite.
# MAGIC
# MAGIC ## How Flotation Works in Iron Ore Processing
# MAGIC
# MAGIC Froth flotation is a common method used to separate silica (SiO₂) from iron-bearing minerals like hematite or magnetite. The goal is to reduce the silica content in the iron ore concentrate, improving the ore's quality for steel production.
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../demo_setup/images/FlotationProcess.jpg" width="400px"/> 
# MAGIC <p><em>Figure 1: The process of froth flotation. Source: https://www.sciencedirect.com/topics/earth-and-planetary-sciences/flotation-froth</em></p>
# MAGIC </div>
# MAGIC
# MAGIC ### 📌 Process Steps
# MAGIC
# MAGIC #### Grinding:
# MAGIC
# MAGIC - Iron ore is ground into fine particles to liberate iron minerals from gangue (waste rock), primarily silica.
# MAGIC
# MAGIC #### Conditioning:
# MAGIC
# MAGIC - Reagents are added to the slurry (water + ground ore):
# MAGIC   - Collectors (e.g., ether amines) make silica hydrophobic.
# MAGIC   - Depressants (e.g., starch) prevent iron minerals from floating.
# MAGIC - Air Injection & Frothing:
# MAGIC   - Air bubbles are introduced into the slurry.
# MAGIC   - Hydrophobic silica particles attach to the bubbles and float to the surface.
# MAGIC   - Hydrophilic iron particles stay in the slurry and are collected as concentrate.
# MAGIC
# MAGIC - Separation:
# MAGIC   - Froth containing silica is skimmed off.
# MAGIC   - The remaining slurry contains the upgraded iron concentrate with reduced silica.
# MAGIC
# MAGIC ### ✅ Silica Concentration Quality Criteria
# MAGIC The acceptable level of silica (SiO₂) in iron ore concentrate depends on its end-use (usually for steelmaking) and customer requirements.  Higher silica affects the slag volume, increases energy consumption, and decreases efficiency in the furnace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - MLOps Databricks Capabilities
# MAGIC
# MAGIC In this demo, we use historical plant [data](https://www.kaggle.com/code/aditya100/quality-prediction-in-iron-ore) (e.g., pH, reagent dosage, pulp density, air flow rate) to train machine learning models that predict Fe and SiO₂ concentrations. We also run an optimisation routines, for example, to determine the ideal setpoints that maximise Fe concentration while keeping SiO₂ within specification.
# MAGIC
# MAGIC As we go through this end-to-end workflow, we will highlight Databricks capabilities such as collaborative notebooks, feature engineering, experiment tracking, model registry, and integration with ML tools — illustrating how data engineers, data scientists, and ML engineers can collaborate to deliver production-ready solutions faster.
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../demo_setup/images/intro_sections.png" width="1300px"/> 
# MAGIC </div>
# MAGIC