# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "aibi-customer-support",
    "category": "AI-BI",
    "title": "AI/BI: Customer Support Performance Review",
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_aibi_customer_support",
    "description": "Enhance your customer support effectiveness and analytics with AI/BI Dashboards. Then, utilize Genie to drill deeper into team performance.",
    "bundle": True,
    "notebooks": [
      {
        "path": "AI-BI-Customer-support",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "AI BI: Customer Support Review",
        "description": "Discover Databricks Intelligence Data Platform capabilities."
      }
    ],
    "init_job": {},
    "serverless_supported": True,
    "cluster": {},
    "pipelines": [],
    "dashboards": [
      {
        "name": "[dbdemos] AIBI - Customer Support Team Review",
        "id": "customer-support"
      }
    ],
    "data_folders": [
      {
        "source_folder": "aibi/dbdemos_aibi_customer_support/customer_support_review",
        "source_format": "parquet",
        "target_table_name": "customer_support_review",
        "target_format": "delta"
      }
    ],
    "genie_rooms": [
      {
        "id": "customer-support",
        "display_name": "DBDemos - AI/BI - Customer Support Review",
        "description": "Leverage Databricks AI and BI to gain actionable insights into your customer support performance! As a customer support manager, understanding team performance and customer engagement is critical to delivering exceptional service. With Databricks, you can analyze your support data seamlessly, exploring key metrics like response efficiency, ticket volume trends, and seasonal patterns. Dive deep into customer interactions to identify bottlenecks, improve workflows, and uncover insights that enhance customer satisfaction. Use the power of Databricks’ AI-driven analytics to make data-driven decisions, optimize resource allocation, and forecast support demand, ensuring your team consistently exceeds expectations.",
        "table_identifiers": [
          "{{CATALOG}}.{{SCHEMA}}.customer_support_review"
        ],
        "sql_instructions": [
          {
            "title": "Agent performance by tickets closed per month",
            "content": "WITH monthly_ranked_agents AS ( SELECT `agent_name`, DATE_TRUNC('month', `created_time`) AS month, SUM(`survey_results`) AS total_survey_results, ROW_NUMBER() OVER ( PARTITION BY DATE_TRUNC('month', `created_time`) ORDER BY SUM(`survey_results`) DESC ) AS performance_rank FROM {{CATALOG}}.{{SCHEMA}}.customer_support_review WHERE `agent_name` IS NOT NULL GROUP BY `agent_name`, DATE_TRUNC('month', `created_time`) ) SELECT `agent_name`, `month`, `total_survey_results` FROM monthly_ranked_agents WHERE performance_rank <= 10 ORDER BY `month`, performance_rank;"
          },
          {
            "title": "Proportion of tickets per month that violate first response SLA",
            "content": "WITH monthly_tickets AS ( SELECT DATE_TRUNC('month', `created_time`) AS month, COUNT(*) AS total_tickets, SUM( CASE WHEN `first_response_time` > `expected_sla_to_first_response` THEN 1 ELSE 0 END ) AS sla_violations FROM {{CATALOG}}.{{SCHEMA}}.customer_support_review WHERE `created_time` IS NOT NULL AND `first_response_time` IS NOT NULL AND `expected_sla_to_first_response` IS NOT NULL GROUP BY DATE_TRUNC('month', `created_time`) ) SELECT month, ROUND( (sla_violations / total_tickets :: decimal) * 100, 2 ) AS violation_percentage FROM monthly_tickets ORDER BY month;"
          },
          {
            "title": "Which agents violate the resolution SLA most often compared to their number of closed tickets?",
            "content": "WITH agent_ticket_counts AS ( SELECT `agent_name`, COUNT(*) AS total_tickets FROM {{CATALOG}}.{{SCHEMA}}.customer_support_review WHERE `agent_name` IS NOT NULL AND `close_time` IS NOT NULL GROUP BY `agent_name` ), agent_sla_violations AS ( SELECT `agent_name`, COUNT(*) AS violation_count FROM {{CATALOG}}.{{SCHEMA}}.customer_support_review WHERE `agent_name` IS NOT NULL AND `resolution_time` > `expected_sla_to_resolve` GROUP BY `agent_name` ) SELECT a.`agent_name`, a.`violation_count`, t.`total_tickets`, ROUND( (a.`violation_count` :: decimal / t.`total_tickets`) * 100, 2 ) AS violation_percentage FROM agent_sla_violations a JOIN agent_ticket_counts t ON a.`agent_name` = t.`agent_name` ORDER BY violation_percentage DESC;"
          }
        ],
        "instructions": "SLA stands for \"Software License Agreement,\" and violating an SLA (whether for first response time to each customer issue, or for time-to-resolution, is a huge problem for a business.)",
        "curated_questions": [
            "Proportion of tickets per month that violate first response SLA",
            "Agent performance by tickets closed per month",
            "Number of tickets in 2023 by country",
            "How has my team's percentage of resolution SLA violations changed over all time?"
        ]
      }
    ]
  }

# COMMAND ----------


