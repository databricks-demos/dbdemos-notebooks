# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Responsible AI with the Databricks Data Intelligence Platform
# MAGIC
# MAGIC <br />
# MAGIC
# MAGIC Artificial intelligence (AI) is transforming industries by enhancing productivity, reducing costs, and improving decision-making. The rise of Generative AI, especially after the launch of ChatGPT in late 2022, has further accelerated interest in AI. [McKinsey estimates](https://www.mckinsey.com/featured-insights/mckinsey-explainers/whats-the-future-of-generative-ai-an-early-view-in-15-charts) that AI-driven productivity and innovation could contribute between $17 trillion and $26 trillion to the global economy. As a result, businesses are prioritizing AI implementation to gain a competitive edge, with AI investments projected to reach $100 billion in the U.S. and $200 billion globally by 2025, according to [Goldman Sachs](https://www.goldmansachs.com/intelligence/pages/ai-investment-forecast-to-approach-200-billion-globally-by-2025.html).
# MAGIC
# MAGIC However, responsible AI practices—focusing on quality, security, and governance—are essential for building trust and ensuring sustainable adoption. [Gartner](https://www.gartner.com/en/information-technology/insights/top-technology-trends) highlights AI trust, risk, and security management as the top strategy trend for 2024, predicting that by 2026, organizations that prioritize AI transparency and governance will see a [50% increase](https://www.gartner.com/en/articles/what-it-takes-to-make-ai-safe-and-effective) in adoption and business success.
# MAGIC
# MAGIC With AI regulations expanding globally, compliance is becoming a critical part of responsible AI strategies. This demo will show how the Databricks Data Intelligence Platform can help organizations align with emerging AI governance requirements, ensuring they navigate the evolving regulatory landscape effectively.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Core Challenges in Responsible AI
# MAGIC
# MAGIC There are three core challenges in responsible AI:<br />
# MAGIC <br />
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>
# MAGIC
# MAGIC
# MAGIC <div style="margin-left: 20px">
# MAGIC   <div class="badge_b"><div class="badge">1</div> <strong>Lack of visibility into model quality:</strong> Insufficient visibility into the consequences of AI models has become a prevailing challenge. Companies grapple with a lack of trust in the reliability of AI models to consistently deliver outcomes that are safe and fair for their users. Without clear insights into how these models function and the potential impacts of their decisions, organizations struggle to build and maintain confidence in AI-driven solutions. </div>
# MAGIC   <div class="badge_b"><div class="badge">2</div>  <strong>Inadequate security safeguards:</strong> Interactions with AI models expand an organization's attack surface by providing a new way for bad actors to interact with data. Generative AI is particularly problematic, as a lack of security safeguards can allow applications like chatbots to reveal (and in some cases to potentially modify) sensitive data and proprietary intellectual property. This vulnerability exposes organizations to significant risks, including data breaches and intellectual property theft, necessitating robust security measures to protect against malicious activities.</div>
# MAGIC   <div class="badge_b"><div class="badge">3</div> <strong>Siloed governance:</strong> Organizations frequently deploy separate data and AI platforms, creating governance silos that result in limited visibility and explainability of AI models. This disjointed approach leads to inadequate cataloging, monitoring, and auditing of AI models, impeding the ability to guarantee their appropriate use. Furthermore, a lack of data lineage complicates understanding of which data is being utilized for AI models and obstructs effective oversight. Unified governance frameworks are essential to ensure that AI models are transparent, traceable, and accountable, facilitating better management and compliance.
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Building AI responsibly with the Databricks Data Intelligence Platform
# MAGIC
# MAGIC Databricks emphasizes trust in AI by enabling organizations to maintain ownership and control over their data and models. [Databricks Data Intelligence Platform](https://www.databricks.com/product/data-intelligence-platform) unifies data, model training, management, monitoring, and governance across the AI lifecycle. This approach ensures that systems are **high-quality**, **safe**, and **well-governed**, helping organizations implement responsible AI practices effectively while fostering trust in intelligent applications.</br>
# MAGIC
# MAGIC > “Databricks empowers us to develop cutting-edge generative AI solutions efficiently - without sacrificing data security or governance.”  
# MAGIC > — **Greg Rokita, Vice President of Technology, Edmunds**
# MAGIC
# MAGIC > “Azure Databricks has enabled KPMG to modernize the data estate with a platform that powers data transformation, analytics and AI workloads, meeting our emerging AI requirements across the firm while also reducing complexity and costs.”  
# MAGIC > — **KPMG**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1-Quality
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/Lineage-ML_0.png?v=1722259413" width="800px" style="float: right; border: 20px solid white;"/>
# MAGIC
# MAGIC Quality in AI begins with trustworthy data and transparent models. Databricks empowers teams to track data lineage with Unity Catalog, ensuring that every transformation—from raw input to trained model—is documented. This visibility helps prevent issues like training data poisoning and facilitates reproducible pipelines.
# MAGIC
# MAGIC With MLflow experiment tracking built in, organizations can monitor datasets, model performance metrics, and even fairness and bias checks. This ensures that models remain accurate, interpretable, and reliable. Automatic logging of parameters, metrics, and source code accelerates troubleshooting and iteration, while the Feature Store centralizes curated features to reduce online-offline skew.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2-Security
# MAGIC
# MAGIC Ensuring AI safety and security involves robust controls and proactive threat mitigation. The [Databricks AI Security Framework (DASF)](https://www.databricks.com/resources/whitepaper/databricks-ai-security-framework-dasf) identifies potential risks across each AI component, offering actionable recommendations to safeguard data and models. Through Lakehouse Monitoring, teams can continuously track model performance, detect bias, and identify threats in real time.
# MAGIC
# MAGIC When deploying and serving models, Databricks Model Serving and AI Gateway provide secure, governed endpoints with easy rollback and A/B testing capabilities. LLM guardrail solutions filter out inappropriate model outputs and help mitigate hallucinations. By combining cryptographic security, access management, and real-time alerts, Databricks offers a robust foundation for the safe operation of AI systems.<br/><br/>
# MAGIC
# MAGIC <center><img src="https://www.databricks.com/sites/default/files/inline-images/ai-system-components-and-associated-risks-transparent-022324.png?v=1722257756" 
# MAGIC      style="width: 1000px; height: auto; display: block; margin-left: auto; margin-right: auto;" /></center>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### 3-Governance
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/UC-Graphic-2x.png?v=1722319673" width="480px" style="float: right; border: 20px solid white;"/>
# MAGIC
# MAGIC Effective governance ensures that AI initiatives remain both compliant and transparent. At the heart of this approach is Unity Catalog, which unifies the governance of all data and AI assets—including tables, files, models, and AI tools—within a single platform.
# MAGIC
# MAGIC - **Access Control:** Centralized policies that ensure only authorized users can view, modify, or deploy assets.
# MAGIC - **Data Sharing:** Secure collaboration across teams or organizations, with fine-grained controls that protect sensitive information.
# MAGIC - **Discovery:** An indexed view of available assets—complete with owners, tags, and quality metrics—accelerates innovation and reuse.
# MAGIC - **Lineage:** Automatic tracking of data flows and model transformations, providing end-to-end traceability for auditing and troubleshooting.
# MAGIC - **Monitoring:** Continuous checks for data and model compliance, coupled with real-time alerts to identify anomalies and potential risks.
# MAGIC - **Auditing:** Immutable logs showing who accessed or changed data and models, simplifying the fulfillment of both internal governance and external regulations.
# MAGIC
# MAGIC
# MAGIC By unifying these capabilities within Unity Catalog, Databricks delivers unified governance for data and AI. This holistic approach not only protects the integrity of AI workflows but also establishes the accountability needed to maintain stakeholder confidence and meet evolving regulatory standards.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # DEMO: Building a Responsible Credit Scoring Model with Databricks Data Intelligence Platform 
# MAGIC <br />
# MAGIC In this demo, we’ll step into the shoes of a retail bank aiming to responsibly enhance its credit decisioning processes amidst evolving economic conditions. In addition to boosting revenue by identifying underbanked customers with strong credit potential, the bank also needs to reduce losses by accurately predicting potential defaults among existing credit holders. The goal is to address three core pillars of Responsible AI—<strong>quality</strong>, <strong>security</strong>, and <strong>governance</strong>—to meet regulatory requirements, mitigate risks, and ensure stakeholder confidence. We’ll showcase how Databricks unifies data, secures it effectively, and enables teams to build, deploy, and monitor machine learning solutions responsibly.
# MAGIC
# MAGIC
# MAGIC ### What we will build
# MAGIC
# MAGIC To accomplish these objectives responsibly, we’ll construct an end-to-end solution on the Databricks Lakehouse platform, with a strong focus on transparency, fairness, and governance at each step. Specifically, we will:<br/><br/>
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>
# MAGIC
# MAGIC
# MAGIC </style>
# MAGIC
# MAGIC <div style="margin-left: 20px">
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">1</div>
# MAGIC     Run exploratory analysis to identify anomalies, biases, and data drift early, paving the way for responsible feature engineering.
# MAGIC   </div>
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">2</div>
# MAGIC     Continuously update features and log transformations for full lineage and compliance.
# MAGIC   </div>
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">3</div>
# MAGIC     Train and evaluate models against fairness and accuracy criteria, logging artifacts for reproducibility.
# MAGIC   </div>
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">4</div>
# MAGIC     Validate models by performing compliance checks and pre-deployment tests, ensuring alignment with Responsible AI standards.
# MAGIC   </div>
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">5</div>
# MAGIC     Integrate champion and challenger models to deploy the winning solution, maintaining traceability and accountability at each decision point.
# MAGIC   </div>
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">6</div>
# MAGIC     Provide batch or real-time inference with robust explainability and filtering mechanisms for safe decision-making.
# MAGIC   </div>
# MAGIC   <div class="badge_b">
# MAGIC     <div class="badge">7</div>
# MAGIC     Monitor production models to detect data drift, alert on performance decay, and trigger recalibration workflows whenever the model’s effectiveness declines.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <center><img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/_resources/images/architecture_0.png?raw=true" 
# MAGIC      style="width: 1200px; height: auto; display: block; margin: 0;" /></center>
# MAGIC
# MAGIC With this approach, we can confidently meet regulatory and ethical benchmarks while improving organizational outcomes—demonstrating that Responsible AI is not just about checking boxes but about building trust and value across the entire ML lifecycle.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next steps
# MAGIC Below is a concise overview of each notebook’s role in the Responsible AI pipeline, as depicted in the architecture diagram. Together, they illustrate how Databricks supports transparency (explainability), effectiveness (model performance and bias control), and reliability (ongoing monitoring) throughout the model lifecycle.
# MAGIC
# MAGIC * [06.1-Exploratory-Analysis]($./06-Responsible-AI/06.1-Exploratory-Analysis): Examines data distributions and identifies biases or anomalies, providing transparency early in the lifecycle. Sets the stage for responsible feature selection and model design.
# MAGIC * [06.2-Feature-Updates]($./06-Responsible-AI/06.2-Feature-Updates): Continuously ingests new data, refreshes features, and logs transformations, ensuring model effectiveness. Maintains transparency around feature lineage for compliance and traceability.
# MAGIC * [06.3-Model-Training]($./06-Responsible-AI/06.3-Model-Training): Trains, evaluates, and documents candidate models, tracking performance and fairness metrics. Stores artifacts for reproducibility, aligning with responsible AI goals.
# MAGIC * [06.4-Model-Validation]($./06-Responsible-AI/06.4-Model-Validation): Performs compliance checks, pre-deployment tests, and fairness evaluations. Verifies reliability and transparency standards before the model progresses to production.
# MAGIC * [06.5-Model-Integration]($./06-Responsible-AI/06.5-Model-Integration): Compares champion and challenger models, enabling human oversight for final selection. Deploys the chosen pipeline responsibly, ensuring accountability at each handoff.
# MAGIC * [06.6-Model-Inference]($./06-Responsible-AI/06.6-Model-Inference): Executes batch or real-time predictions using the deployed model, applying filtering rules. Ensures outputs remain consistent and explainable for responsible decision-making.
# MAGIC * [06.7-Model-Monitoring]($./06-Responsible-AI/06.7-Model-Monitoring): Continuously tracks data drift, prediction stability, and performance degradation. Generates alerts for timely retraining, preserving reliability and trustworthiness across the model’s lifecycle.
