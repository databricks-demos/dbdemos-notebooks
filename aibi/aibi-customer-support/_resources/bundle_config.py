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
        "source_folder": "aibi/dbdemos_aibi_customer_support/agents_bronze",
        "source_format": "parquet",
        "target_volume_folder": "agents_bronze",
        "target_format": "parquet"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_customer_support/tickets_bronze",
        "source_format": "parquet",
        "target_volume_folder": "tickets_bronze",
        "target_format": "parquet"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_customer_support/sla_bronze",
        "source_format": "parquet",
        "target_volume_folder": "sla_bronze",
        "target_format": "parquet"
      }      
    ],
    "sql_queries": [
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.agents_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze table containing customer support agent information' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/agents_bronze', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sla_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze table containing service level agreement metrics for customer support tickets' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/sla_bronze', format => 'parquet', pathGlobFilter => '*.parquet')",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.tickets_bronze TBLPROPERTIES (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true) COMMENT 'Bronze table containing customer support ticket information and history' AS SELECT * EXCEPT (_rescued_data) FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/tickets_bronze', format => 'parquet', pathGlobFilter => '*.parquet')"
      ],
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.agents_clean (ticket_id BIGINT COMMENT 'Unique identifier for the customer support ticket, allowing tracking of individual tickets and their support.', agent_group STRING COMMENT 'Represents the group of agents responsible for the ticket, providing a way to analyze performance and workload.', agent_name STRING COMMENT 'Identifies the individual agent responsible for the ticket, allowing tracking of individual agent performance.', agent_interactions DOUBLE COMMENT 'Represents the number of interactions the agent has had with the customer, giving insight into the complexity of the issue and the agents workload.') USING delta COMMENT 'The agents_clean table contains information about the customer support agents and their interactions with customers. It includes details such as the number of interactions per agent and the agent group they belong to. This data can be used to analyze agent performance, identify high-performing and low-performing groups, and optimize agent allocation for better customer support. Additionally, it can help in identifying potential training needs for individual agents or groups.'; INSERT OVERWRITE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.agents_clean SELECT ticket_id, agent_group, agent_name, try_cast(agent_interactions AS DOUBLE) AS agent_interactions FROM `{{CATALOG}}`.`{{SCHEMA}}`.agents_bronze;",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.tickets_clean (ticket_id BIGINT COMMENT 'Unique identifier for each ticket, allowing for easy tracking and reference.', status STRING COMMENT 'Represents the current status of the ticket, indicating whether it is open, closed, or in progress.', priority STRING COMMENT 'Describes the urgency of the ticket, allowing for prioritization and efficient allocation of resources.', source STRING COMMENT 'Identifies the source of the ticket, providing information about the customer or system that generated it.', topic STRING COMMENT 'Represents the topic or issue related to the ticket, allowing for categorization and efficient handling.', created_time TIMESTAMP COMMENT 'The timestamp when the ticket was created, indicating when it was first reported.', close_time TIMESTAMP COMMENT 'The timestamp when the ticket was closed, indicating when it was resolved.', product_group STRING COMMENT 'Identifies the product or service group associated with the ticket, allowing for categorization and efficient handling.', support_level STRING COMMENT 'Represents the level of support provided for the ticket, indicating the resources allocated to its resolution.', country STRING COMMENT 'Identifies the country where the ticket was generated, providing information about the geographical location of the customer or system.', latitude DOUBLE COMMENT 'Represents the latitude of the customer or system location, providing more precise information about the geographical location.', longitude DOUBLE COMMENT 'Represents the longitude of the customer or system location, providing more precise information about the geographical location.', PRIMARY KEY (ticket_id) RELY) USING delta COMMENT 'The tickets_clean table contains customer support tickets that have been cleaned and preprocessed. It includes details such as ticket status, priority, source, topic, and geographical location. This data can be used for monitoring and managing customer support tickets, tracking ticket resolution times, and analyzing customer support patterns based on factors like ticket source, priority, and geographical location. This information can help improve customer support processes and identify potential areas for improvement.'; INSERT OVERWRITE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.tickets_clean SELECT ticket_id, status, priority, source, topic, created_time, close_time, product_group, support_level, country, try_cast(latitude AS DOUBLE) AS latitude, try_cast(longitude AS DOUBLE) AS longitude FROM `{{CATALOG}}`.`{{SCHEMA}}`.tickets_bronze`",
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sla_clean (ticket_id BIGINT COMMENT 'Unique identifier for the customer support ticket.', expected_sla_to_resolve TIMESTAMP COMMENT 'The expected service level agreement (SLA) for resolving the ticket.', expected_sla_to_first_response TIMESTAMP COMMENT 'The expected SLA for the first response to the ticket.', first_response_time TIMESTAMP COMMENT 'The actual time taken for the first response to the ticket.', sla_for_first_response STRING COMMENT 'The SLA achieved for the first response to the ticket.', resolution_time TIMESTAMP COMMENT 'The actual time taken to resolve the ticket.', sla_for_resolution STRING COMMENT 'The SLA achieved for resolving the ticket.', survey_results INT COMMENT 'The survey results for the customers experience with the support ticket, measured on a numerical scale.', PRIMARY KEY (ticket_id) RELY) USING delta COMMENT 'The sla_clean table contains information about the Service Level Agreements (SLAs) for customer support tickets. It includes details about the expected SLAs for first response and resolution, as well as the actual SLAs achieved. This data can be used to assess the performance of customer support teams, identify bottlenecks, and track improvements in SLAs over time. Additionally, survey results are included to provide feedback on the quality of customer support.'; INSERT OVERWRITE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sla_clean SELECT ticket_id, expected_sla_to_resolve, expected_sla_to_first_response, first_response_time, sla_for_first_response, resolution_time, sla_for_resolution, try_cast(survey_results AS INT) AS survey_results FROM `{{CATALOG}}`.`{{SCHEMA}}`.sla_bronze`"
      ],
      [
        "CREATE OR REPLACE FUNCTION `{{CATALOG}}`.`{{SCHEMA}}`.get_top_agents_by_survey_score(catalog_name STRING, schema_name STRING, rank_cutoff INT) RETURNS TABLE (agent_name STRING, month TIMESTAMP, total_survey_results DECIMAL(10,2), performance_rank INT) RETURN (WITH monthly_ranked_agents AS (SELECT a.agent_name, DATE_TRUNC('month', t.created_time) AS month, SUM(s.survey_results) AS total_survey_results, ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', t.created_time) ORDER BY SUM(s.survey_results) DESC) AS performance_rank FROM `{{CATALOG}}`.`{{SCHEMA}}`.tickets_clean t JOIN `{{CATALOG}}`.`{{SCHEMA}}`.agents_clean a ON t.ticket_id = a.ticket_id JOIN `{{CATALOG}}`.`{{SCHEMA}}`.sla_clean s ON t.ticket_id = s.ticket_id GROUP BY a.agent_name, DATE_TRUNC('month', t.created_time)) SELECT agent_name, month, total_survey_results, performance_rank FROM monthly_ranked_agents WHERE performance_rank <= rank_cutoff);",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.agents_clean ADD CONSTRAINT agents_clean_tickets_fk FOREIGN KEY (ticket_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.tickets_clean (ticket_id)",
        "ALTER TABLE `{{CATALOG}}`.`{{SCHEMA}}`.sla_clean ADD CONSTRAINT sla_clean_tickets_fk FOREIGN KEY (ticket_id) REFERENCES `{{CATALOG}}`.`{{SCHEMA}}`.tickets_clean (ticket_id)"
      ]
    ],
    "genie_rooms": [
      {
        "id": "customer-support",
        "display_name": "DBDemos - AI/BI - Customer Support Review",
        "description": "Leverage Databricks AI and BI to gain actionable insights into your customer support performance! As a customer support manager, understanding team performance and customer engagement is critical to delivering exceptional service. With Databricks, you can analyze your support data seamlessly, exploring key metrics like response efficiency, ticket volume trends, and seasonal patterns. Dive deep into customer interactions to identify bottlenecks, improve workflows, and uncover insights that enhance customer satisfaction. Use the power of Databricks' AI-driven analytics to make data-driven decisions, optimize resource allocation, and forecast support demand, ensuring your team consistently exceeds expectations.",
        "table_identifiers": [
          "{{CATALOG}}.{{SCHEMA}}.agents_clean",
          "{{CATALOG}}.{{SCHEMA}}.tickets_clean",
          "{{CATALOG}}.{{SCHEMA}}.sla_clean"
        ],
        "sql_instructions": [
          {
            "title": "Agent performance by tickets closed per month",
            "content": "WITH monthly_ranked_agents AS (SELECT a.`agent_name`, DATE_TRUNC('month', t.`created_time`) AS month, SUM(s.`survey_results`) AS total_survey_results, ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', t.`created_time`) ORDER BY SUM(s.`survey_results`) DESC) AS performance_rank FROM {{CATALOG}}.{{SCHEMA}}.tickets_clean t JOIN {{CATALOG}}.{{SCHEMA}}.agents_clean a ON t.`ticket_id` = a.`ticket_id` JOIN {{CATALOG}}.{{SCHEMA}}.sla_clean s ON t.`ticket_id` = s.`ticket_id` WHERE t.`created_time` IS NOT NULL GROUP BY a.`agent_name`, DATE_TRUNC('month', t.`created_time`)) SELECT `agent_name`, `month`, `total_survey_results` FROM monthly_ranked_agents WHERE performance_rank <= 10 ORDER BY `month`, performance_rank;"
          },
          {
            "title": "Proportion of tickets per month that violate first response SLA",
            "content": "WITH monthly_tickets AS (SELECT DATE_TRUNC('month', t.`created_time`) AS month, COUNT(*) AS total_tickets, SUM(CASE WHEN s.`first_response_time` > s.`expected_sla_to_first_response` THEN 1 ELSE 0 END) AS sla_violations FROM {{CATALOG}}.{{SCHEMA}}.tickets_clean t JOIN {{CATALOG}}.{{SCHEMA}}.sla_clean s ON t.`ticket_id` = s.`ticket_id` WHERE t.`created_time` IS NOT NULL AND s.`first_response_time` IS NOT NULL AND s.`expected_sla_to_first_response` IS NOT NULL GROUP BY DATE_TRUNC('month', t.`created_time`)) SELECT month, ROUND((sla_violations / total_tickets :: decimal) * 100, 2) AS violation_percentage FROM monthly_tickets ORDER BY month;"
          },
          {
            "title": "Which agents violate the resolution SLA most often compared to their number of closed tickets?",
            "content": "SELECT a.`agent_name`, a.`violation_count`, t2.`total_tickets`, ROUND((a.`violation_count` :: decimal / t2.`total_tickets`) * 100, 2) AS violation_percentage FROM (SELECT a.`agent_name`, COUNT(*) AS violation_count FROM {{CATALOG}}.{{SCHEMA}}.tickets_clean t JOIN {{CATALOG}}.{{SCHEMA}}.agents_clean a ON t.`ticket_id` = a.`ticket_id` JOIN {{CATALOG}}.{{SCHEMA}}.sla_clean s ON t.`ticket_id` = s.`ticket_id` WHERE a.`agent_name` IS NOT NULL AND s.`resolution_time` > s.`expected_sla_to_resolve` GROUP BY a.`agent_name`) a JOIN (SELECT a.`agent_name`, COUNT(*) AS total_tickets FROM {{CATALOG}}.{{SCHEMA}}.tickets_clean t JOIN {{CATALOG}}.{{SCHEMA}}.agents_clean a ON t.`ticket_id` = a.`ticket_id` JOIN {{CATALOG}}.{{SCHEMA}}.sla_clean s ON t.`ticket_id` = s.`ticket_id` WHERE a.`agent_name` IS NOT NULL AND s.`close_time` IS NOT NULL GROUP BY a.`agent_name`) t2 ON a.`agent_name` = t2.`agent_name` ORDER BY violation_percentage DESC;"
          }
        ],
        "instructions": "SLA stands for \"Service Level Agreement,\" and violating an SLA (whether for first response time to each customer issue, or for time-to-resolution, is a huge problem for a business.)",
        "function_names": [
          "{{CATALOG}}.{{SCHEMA}}.get_top_agents_by_survey_score"
        ],         
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


