# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "aibi-sales-pipeline-review",
    "category": "AI-BI",
    "title": "AI/BI: Sales Pipeline Review",
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_aibi_sales_pipeline_review",
    "description": "Optimize your sales pipeline visibility and insights with AI/BI Dashboards, and leverage Genie to ask questions about your data in natural language.",
    "bundle": True,
    "notebooks": [
      {
        "path": "AI-BI-Sales-pipeline",
        "pre_run": False,
        "publish_on_website": True,
        "add_cluster_setup_cell": False,
        "title": "AI BI: Sales Pipeline Review",
        "description": "Discover Databricks Intelligence Data Platform capabilities."
      }
    ],
    "init_job": {},
    "serverless_supported": True,
    "cluster": {},
    "pipelines": [],
    "dashboards": [
      {
        "name": "[dbdemos] AIBI - Sales Pipeline Review",
        "id": "sales-pipeline"
      }
    ],
    "data_folders": [
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/raw_data/raw_accounts",
        "source_format": "parquet",
        "target_volume_folder": "raw_accounts",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/raw_data/raw_opportunity",
        "source_format": "parquet",
        "target_volume_folder": "raw_opportunity",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/raw_data/raw_user",
        "source_format": "parquet",
        "target_volume_folder": "raw_user",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/raw_data/raw_dim_country",
        "source_format": "parquet",
        "target_volume_folder": "raw_dim_country",
        "target_format": "delta"
      },
      {
        "source_folder": "aibi/dbdemos_aibi_sales_pipeline/raw_data/raw_employee_hierarchy",
        "source_format": "parquet",
        "target_volume_folder": "raw_employee_hierarchy",
        "target_format": "delta"
      }
    ],
    "sql_queries": [
      ["CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_accounts TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact   = TRUE ) COMMENT 'This is the bronze table for accounts created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_accounts', format => 'parquet', pathGlobFilter => '*.parquet')",
       "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_opportunity TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact   = TRUE ) COMMENT 'This is the bronze table for opportunity created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_opportunity', format => 'parquet', pathGlobFilter => '*.parquet')",
       "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_user TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact   = TRUE ) COMMENT 'This is the bronze table for user created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_user', format => 'parquet', pathGlobFilter => '*.parquet')",
       "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.dim_country TBLPROPERTIES ( delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact   = TRUE ) COMMENT 'This is the reference table for countries created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_dim_country', format => 'parquet', pathGlobFilter => '*.parquet')",
       "CREATE TABLE IF NOT EXISTS `{{CATALOG}}`.`{{SCHEMA}}`.employee_hierarchy TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact   = TRUE ) COMMENT 'This is the employee hierarchy table created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_employee_hierarchy', format => 'parquet', pathGlobFilter => '*.parquet')"
       ]
      ,
      ["CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_accounts COMMENT 'Cleaned version of the raw accounts table' AS SELECT AccountId,  AccountName,  AccountIndustry,  AccountType,  AccountHQCountry,  AccountCountry,  NumberOfEmployees,  AnnualRevenue FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_accounts WHERE AccountId IS NOT NULL AND AccountName IS NOT NULL AND AccountIndustry IS NOT NULL AND NumberOfEmployees > 0",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_opportunity COMMENT 'Cleaned version of the raw opportunity table' AS SELECT  OpportunityId,  OpportunityType,  OpportunityName,  OpportunityDescription,  AccountId,  AgeInDays,  Amount,  OpportunityAmount,  CloseDate,  CreatedDate,  LeadSource,  OwnerId,  Probability,  StageName,  ForecastCategory,  NewExpansionBookingAnnual,  NewExpansionBookingAnnualWtd,  NewRecurringBookingsManual,  NewRecurringBookings,  BusinessType FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_opportunity WHERE OpportunityId IS NOT NULL AND OpportunityType IS NOT NULL AND AccountId IS NOT NULL AND AgeInDays IS NOT NULL AND AgeInDays > 0 AND CloseDate >= CreatedDate AND OwnerId IS NOT NULL AND Probability IS NOT NULL AND Probability >= 0 AND Probability <= 1 AND StageName IS NOT NULL",
      "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_user COMMENT 'Cleaned version of the raw user table' AS SELECT UserId,  UserName,  UserTitle,  UserRole,  UserSegment,  UserRegion FROM `{{CATALOG}}`.`{{SCHEMA}}`.raw_user WHERE UserId IS NOT NULL AND UserName IS NOT NULL AND UserTitle IS NOT NULL AND UserRole IS NOT NULL"
       ],
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.accounts AS SELECT  AccountId AS id,  AccountName AS name,  AccountIndustry AS industry,  AccountType AS type,  hq_c.region AS region_hq__c,  c.region AS region__c,  CASE    WHEN NumberOfEmployees <= 100 THEN 'SMB' WHEN NumberOfEmployees BETWEEN 101 AND 1000 THEN 'MM'    ELSE 'ENT'  END AS company_size_segment__c,  AnnualRevenue AS annualrevenue FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_accounts acc JOIN `{{CATALOG}}`.`{{SCHEMA}}`.dim_country hq_c ON acc.AccountHQCountry = hq_c.country JOIN `{{CATALOG}}`.`{{SCHEMA}}`.dim_country c ON acc.AccountCountry = c.country"
      ],
      [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.opportunity AS SELECT OpportunityId AS opportunityid,  OpportunityType AS type,  OpportunityName AS name,  OpportunityDescription AS description,  AccountId AS accountid,  AgeInDays AS days_to_close,  Amount AS amount,  OpportunityAmount AS opportunity_amount,  CloseDate AS closedate,  CreatedDate AS createddate,  LeadSource AS leadsource,  OwnerId AS ownerid,  Probability AS probability,  Probability * 100 AS probability_per, StageName AS stagename,  ForecastCategory AS forecastcategory,  NewExpansionBookingAnnual AS New_Expansion_Booking_Annual__c,  NewExpansionBookingAnnualWtd AS New_Expansion_Booking_Annual_Wtd__c,  NewRecurringBookingsManual AS New_Recurring_Bookings_Manual__c,  NewRecurringBookings AS New_Recurring_Bookings__c,  BusinessType AS business_type__c,  a.industry AS industry,  a.name AS account_name,  a.region_hq__c AS region_hq__c,  a.region__c AS region,  a.company_size_segment__c,  CONCAT(a.region__c, '-', a.company_size_segment__c) AS region_size,  a.annualrevenue FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_opportunity o JOIN `{{CATALOG}}`.`{{SCHEMA}}`.accounts a ON o.AccountId = a.id"
      ],
      ["CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.opportunity_history AS SELECT createddate AS CreatedDate,  stagename AS DealStage,  region AS Region,  SUM(amount) AS SumDealAmount FROM `{{CATALOG}}`.`{{SCHEMA}}`.opportunity GROUP BY createddate, stagename, region",
       "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.user AS SELECT UserId AS id, UserName AS name,  UserTitle AS title,  e.managerid AS managerid,  UserRole AS role__c,  UserSegment AS segment__c,  UserRegion AS region__c FROM `{{CATALOG}}`.`{{SCHEMA}}`.cleaned_user u LEFT JOIN `{{CATALOG}}`.`{{SCHEMA}}`.employee_hierarchy e ON u.UserId = e.id"]
    ]
    ,
    "genie_rooms": [
      {
        "id": "sales-pipeline",
        "display_name": "DBDemos - AI/BI - Sales Pipeline Review",
        "description": "Analyze your Sales Pipeline performance leveraging this AI/BI Dashboard. Deep dive into your data and metrics.",
        "table_identifiers": [
          "{{CATALOG}}.{{SCHEMA}}.accounts",
          "{{CATALOG}}.{{SCHEMA}}.opportunity",
          "{{CATALOG}}.{{SCHEMA}}.opportunity_history",
          "{{CATALOG}}.{{SCHEMA}}.user"
        ],
        "sql_instructions": [
          {
            "title": "Average active opportunity size",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT AVG(`opportunity_amount`) AS `avg(opportunity_amount)` FROM q GROUP BY GROUPING SETS (())"
          },
          {
            "title": "Distribution of stages across all opportunities",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT COUNT(`stagename`) AS `count(stagename)`, `stagename` FROM q GROUP BY `stagename`"
          },
          {
            "title": "Pipeline Amount by Region",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT `region`, SUM(`pipeline_amount`) AS `sum(pipeline_amount)` FROM q GROUP BY `region`"
          },
          {
            "title": "Average days to close",
            "content": "WITH q AS (SELECT *, CASE WHEN stagename IN ('2. Demo', '3. Validation', '4. Procure') THEN amount ELSE NULL END AS pipeline_amount FROM opportunity WHERE forecastcategory IS NOT NULL) SELECT AVG(`days_to_close`) AS `avg(days_to_close)` FROM q GROUP BY GROUPING SETS (())"
          }
        ],
        "instructions": "- Financial years run from February through to the end of January the following year.\n- For example deals created in FY24 are createddate >= '2024-02-01' AND createddate < '2025-02-01'\n- Deals closed in FY24 are closedate >= '2024-02-01'  AND closedate < '2025-02-01'\n\n- There are 4 financial quarters every fiscal year. Each quarter is 3 months long. Q1 starts in February and ends in April. Q2 starts in may and ends in July. This quarter is Q2. We are currently in Q2.\n\n- Always round metrics in results to 2 decimal places. For example use ROUND(foo,2) where foo is the metric getting returned.\n\n- When someone asks about europe that means EMEA, america and north america mean AMER.\n\n- When someone asks about top sales performance for sales reps respond with the person who generated the most pipeline across all stages.\n\n- a won sale is one with the stagename \"5. Closed Won\" a lost sale is one with the stagename \"X. Closed Lost\"\n\n- Remember to filter out nulls for forecastcategory\n\n- Remember to use pipeline_amount when asked about pipeline\n- When asked about pipeline start by breaking down totals by region\n\n- include user name when asked about sales reps\n\n \n\n- include account name when asked about a specific account\n",
        "curated_questions": [
          "How's my pipeline just in the americas and by segment?",
          "What customers churned in America and how much revenue were they paying us?",
          "How's my pipeline?",
          "Who are my biggest customers globally?",
          "What is my average active opportunity size?"
        ]
      }
    ]
  }

# COMMAND ----------


