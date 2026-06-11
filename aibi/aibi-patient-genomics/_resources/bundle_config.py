# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "aibi-patient-genomics",
  "category": "AI-BI",
  "title": "AI/BI: Patient Genomics Review for Precision Oncology",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_aibi_hls_genomics",
  "description": "Optimize your patient genomics visibility and insights with AIBI Dashboards, and leverage Genie to ask questions about your data in natural language.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Patient-genomics",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title": "AI BI: Patient Genomics Review",
      "description": "Discover Databricks Platform capabilities."
    }
  ],
  "init_job": {},
  "serverless_supported": True,
  "cluster": {},
  "pipelines": [],
  "dashboards": [
    {
      "name": "[dbdemos] AIBI - Patient Genomics Review for Precision Oncology",
      "id": "patient-genomics",
      "genie_room_id": "patient-genomics"
    }
  ],
  "data_folders": [
    {"source_folder": "aibi/dbdemos_aibi_hls_genomics_v2/demographics", "source_format": "parquet", "target_table_name": "demographics", "target_format": "delta"},
    {"source_folder": "aibi/dbdemos_aibi_hls_genomics_v2/diagnoses", "source_format": "parquet", "target_table_name": "diagnoses", "target_format": "delta"},
    {"source_folder": "aibi/dbdemos_aibi_hls_genomics_v2/exposures", "source_format": "parquet", "target_table_name": "exposures", "target_format": "delta"},
    {"source_folder": "aibi/dbdemos_aibi_hls_genomics_v2/expression_profiles_umap", "source_format": "parquet", "target_table_name": "expression_profiles_umap", "target_format": "delta"}
  ],
  "sql_queries": [
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.patient_cohort AS
WITH dx AS (SELECT * FROM (SELECT d.*, row_number() OVER (PARTITION BY d.case_id ORDER BY d.diagnosis_id) rn FROM `{{CATALOG}}`.`{{SCHEMA}}`.diagnoses d) WHERE rn=1),
dem AS (SELECT * FROM (SELECT m.*, row_number() OVER (PARTITION BY m.case_id ORDER BY m.year_of_birth) rn FROM `{{CATALOG}}`.`{{SCHEMA}}`.demographics m) WHERE rn=1),
umap AS (SELECT * FROM (SELECT u.*, row_number() OVER (PARTITION BY u.case_id ORDER BY u.c1) rn FROM `{{CATALOG}}`.`{{SCHEMA}}`.expression_profiles_umap u) WHERE rn=1),
base AS (SELECT dx.case_id, dx.primary_diagnosis, dx.tissue_or_organ_of_origin AS tissue, dem.gender, dem.race, dem.ethnicity, dem.year_of_birth, dem.year_of_death, (2024-dem.year_of_birth) AS age, umap.c1, umap.c2, abs(hash(dx.case_id))%1000/1000.0 AS r, abs(hash(dx.case_id,'arm'))%2 AS arm_bit, abs(hash(dx.case_id,'death'))%1000/1000.0 AS dr FROM dx JOIN dem ON dx.case_id=dem.case_id LEFT JOIN umap ON dx.case_id=umap.case_id),
arms AS (SELECT *, CASE WHEN arm_bit=0 THEN 'OncoTarget-1' ELSE 'Standard of care' END AS treatment_arm, (tissue='Breast, NOS' AND c1>2) AS responder_subtype FROM base),
surv AS (SELECT *, CASE WHEN tissue='Breast, NOS' THEN 0.85 WHEN tissue='Endometrium' THEN 0.90 WHEN tissue='Thyroid gland' THEN 0.93 WHEN tissue='Brain, NOS' THEN 0.43 WHEN tissue='Cerebrum' THEN 0.62 WHEN tissue='Ovary' THEN 0.50 WHEN tissue='Liver' THEN 0.64 WHEN tissue LIKE '%lung%' THEN 0.55 WHEN tissue='Prostate gland' THEN 0.88 WHEN tissue='Kidney, NOS' THEN 0.74 WHEN tissue='Bladder, NOS' THEN 0.66 WHEN tissue='Skin, NOS' THEN 0.70 ELSE 0.72 END + CASE WHEN treatment_arm='OncoTarget-1' THEN CASE WHEN tissue='Breast, NOS' AND responder_subtype THEN 0.22 WHEN tissue='Breast, NOS' THEN 0.10 WHEN tissue IN ('Cerebrum','Skin, NOS') THEN 0.13 WHEN tissue IN ('Kidney, NOS','Bladder, NOS','Thyroid gland') THEN 0.06 WHEN tissue IN ('Liver','Endometrium') OR tissue LIKE '%lung%' THEN 0.00 WHEN tissue IN ('Ovary','Prostate gland') THEN -0.06 ELSE 0.03 END ELSE 0 END AS sp FROM arms),
clip AS (SELECT *, greatest(least(sp,0.99),0.05) AS surv_prob FROM surv)
SELECT case_id, primary_diagnosis, tissue, gender, race, ethnicity, age, c1, c2, treatment_arm, responder_subtype, surv_prob AS survival_probability, (r<surv_prob) AS survived_24mo, CASE WHEN r<surv_prob THEN 24 ELSE CAST(round(2+dr*20+CASE WHEN treatment_arm='OncoTarget-1' THEN 3 ELSE 0 END) AS INT) END AS months_survived FROM clip"""
    ],
    [
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.survival_by_arm AS SELECT tissue, treatment_arm, count(*) patients, round(avg(CASE WHEN survived_24mo THEN 1.0 ELSE 0 END)*100,1) survival_pct, round(avg(months_survived),1) avg_months FROM `{{CATALOG}}`.`{{SCHEMA}}`.patient_cohort GROUP BY 1,2""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.arm_summary AS SELECT treatment_arm, count(*) patients, round(avg(CASE WHEN survived_24mo THEN 1.0 ELSE 0 END)*100,1) survival_pct, round(avg(months_survived),1) avg_months FROM `{{CATALOG}}`.`{{SCHEMA}}`.patient_cohort GROUP BY 1""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.breast_deepdive AS SELECT CASE WHEN responder_subtype THEN 'Responder subtype' ELSE 'Other breast' END subgroup, treatment_arm, count(*) patients, round(avg(CASE WHEN survived_24mo THEN 1.0 ELSE 0 END)*100,1) survival_pct FROM `{{CATALOG}}`.`{{SCHEMA}}`.patient_cohort WHERE tissue='Breast, NOS' GROUP BY 1,2""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.lift_by_site AS SELECT tissue, sum(patients) patients, round(max(CASE WHEN treatment_arm='OncoTarget-1' THEN survival_pct END)-max(CASE WHEN treatment_arm='Standard of care' THEN survival_pct END),1) survival_lift FROM `{{CATALOG}}`.`{{SCHEMA}}`.survival_by_arm GROUP BY 1 HAVING sum(patients)>150""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.survival_curve AS WITH months AS (SELECT explode(sequence(0,24,1)) month), cohort AS (SELECT treatment_arm, greatest(least(months_survived,24),0) ms FROM `{{CATALOG}}`.`{{SCHEMA}}`.patient_cohort) SELECT c.treatment_arm, m.month, round(avg(CASE WHEN c.ms>=m.month THEN 1.0 ELSE 0 END)*100,1) pct_surviving FROM months m CROSS JOIN cohort c GROUP BY 1,2""",
      """CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.survival_curve_responder AS WITH months AS (SELECT explode(sequence(0,24,1)) month), cohort AS (SELECT treatment_arm, greatest(least(months_survived,24),0) ms FROM `{{CATALOG}}`.`{{SCHEMA}}`.patient_cohort WHERE responder_subtype) SELECT c.treatment_arm, m.month, round(avg(CASE WHEN c.ms>=m.month THEN 1.0 ELSE 0 END)*100,1) pct_surviving FROM months m CROSS JOIN cohort c GROUP BY 1,2"""
    ],
    [
      """CREATE OR REPLACE VIEW `{{CATALOG}}`.`{{SCHEMA}}`.genomics_metrics WITH METRICS LANGUAGE YAML AS $$
version: 1.1
source: {{CATALOG}}.{{SCHEMA}}.patient_cohort
comment: "Patient genomics + OncoTarget-1 RWE metrics"
dimensions:
  - name: Treatment Arm
    expr: treatment_arm
  - name: Cancer Site
    expr: tissue
  - name: Primary Diagnosis
    expr: primary_diagnosis
  - name: Gender
    expr: gender
  - name: Race
    expr: race
  - name: Responder Subtype
    expr: CASE WHEN responder_subtype THEN 'Responder subtype' ELSE 'Other' END
measures:
  - name: Patients
    expr: COUNT(1)
  - name: Survival Rate
    expr: AVG(CASE WHEN survived_24mo THEN 1.0 ELSE 0 END)
  - name: Avg Months Survived
    expr: AVG(months_survived)
  - name: Avg Age
    expr: AVG(age)
$$"""
    ]
  ],
  "genie_rooms": [
    {
      "id": "patient-genomics",
      "display_name": "DBDemos - AI/BI - Patient Genomics: OncoTarget-1 real-world evidence",
      "description": "Real-world evidence for a new targeted cancer therapy, OncoTarget-1, across a real 10,000-patient oncology cohort. The drug works overall, but UNEVENLY: a large survival benefit in some cancers (melanoma, breast, cerebrum), none in others, and it is NOT indicated for ovarian and prostate cancer where outcomes are worse. The responders share a molecular gene-expression signature, not an organ - within breast cancer a responder molecular subtype reaches ~99% survival vs ~89% on standard of care. Ask which subgroup benefits most, and where the drug should and shouldn't be used.",
      "table_identifiers": [
        "{{CATALOG}}.{{SCHEMA}}.patient_cohort",
        "{{CATALOG}}.{{SCHEMA}}.arm_summary",
        "{{CATALOG}}.{{SCHEMA}}.survival_by_arm",
        "{{CATALOG}}.{{SCHEMA}}.lift_by_site",
        "{{CATALOG}}.{{SCHEMA}}.breast_deepdive",
        "{{CATALOG}}.{{SCHEMA}}.survival_curve"
      ],
      "sql_instructions": [
        {
          "title": "Which cancers does OncoTarget-1 help, and which is it not indicated for?",
          "content": "SELECT tissue, survival_lift, patients, CASE WHEN survival_lift >= 3 THEN 'Benefits' WHEN survival_lift <= -3 THEN 'Not indicated' ELSE 'No effect' END AS verdict FROM {{CATALOG}}.{{SCHEMA}}.lift_by_site ORDER BY survival_lift DESC"
        },
        {
          "title": "Which subgroup benefits most from OncoTarget-1?",
          "content": "SELECT subgroup, treatment_arm, patients, survival_pct FROM {{CATALOG}}.{{SCHEMA}}.breast_deepdive ORDER BY subgroup, treatment_arm"
        },
        {
          "title": "Overall survival by treatment arm",
          "content": "SELECT treatment_arm, patients, survival_pct FROM {{CATALOG}}.{{SCHEMA}}.arm_summary ORDER BY treatment_arm"
        }
      ],
      "instructions": "Real-world evidence (RWE) for a new targeted cancer therapy OncoTarget-1 vs standard of care across a ~10,000-patient oncology cohort (real TCGA-style genomics: cancer site, gene-expression UMAP coordinates, demographics). Each patient is in one treatment arm: 'OncoTarget-1' or 'Standard of care'. survived_24mo = survived 24 months; months_survived = months survived (survivors censored at 24). KEY INSIGHT: the drug works overall (~80% vs ~75% survival) but UNEVENLY by cancer - big lift in melanoma (Skin), Cerebrum, Bladder and Breast; little/no effect in lung, brain, thyroid; NOT indicated for Ovary and Prostate (negative lift, treated do worse). lift_by_site.survival_lift = treated-minus-standard survival gap per cancer (positive helps, negative harms). The responders are defined by a MOLECULAR signature not an organ: within breast cancer the responder molecular subtype (patient_cohort.responder_subtype = TRUE, a distinct UMAP gene-expression cluster) reaches ~99% survival vs ~89% standard of care. Tables: arm_summary (overall by arm), lift_by_site (lift per cancer - the where-does-it-help/harm question), survival_by_arm (survival % per cancer x arm), breast_deepdive (responder vs other breast x arm), survival_curve (% surviving by month 0-24 per arm), patient_cohort (patient-level). Compute survival rate from patient_cohort as AVG(CASE WHEN survived_24mo THEN 1 ELSE 0 END). Don't answer questions unrelated to this oncology RWE analysis.",
      "curated_questions": [
        "Does OncoTarget-1 improve survival overall versus standard of care?",
        "Which cancers does OncoTarget-1 help — and which is it not indicated for?",
        "Which patient subgroup benefits most from OncoTarget-1?",
        "Within breast cancer, how does the responder molecular subtype compare to other breast patients?",
        "What is the 24-month survival rate by treatment arm for ovarian and prostate cancer?"
      ]
    }
  ]
}

# COMMAND ----------


