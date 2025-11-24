CREATE TEMPORARY VIEW ip_visits AS
(
  WITH CTE_END_DATES AS (
    SELECT
      patient,
      encounterclass,
      DATE_ADD(EVENT_DATE, -1) AS END_DATE
    FROM
      (
        SELECT
          patient,
          encounterclass,
          EVENT_DATE,
          EVENT_TYPE,
          MAX(START_ORDINAL) OVER (
              PARTITION BY patient, encounterclass
              ORDER BY EVENT_DATE, EVENT_TYPE
              ROWS UNBOUNDED PRECEDING
            ) AS START_ORDINAL,
          ROW_NUMBER() OVER (
              PARTITION BY patient, encounterclass
              ORDER BY EVENT_DATE, EVENT_TYPE
            ) AS OVERALL_ORD
        FROM
          (
            SELECT
              patient,
              encounterclass,
              start AS EVENT_DATE,
              -1 AS EVENT_TYPE,
              ROW_NUMBER() OVER (
                  PARTITION BY patient, encounterclass
                  ORDER BY start, stop
                ) AS START_ORDINAL
            FROM
              encounters
            WHERE
              encounterclass = 'inpatient'
            UNION ALL
            SELECT
              patient,
              encounterclass,
              DATE_ADD(stop, 1),
              1 AS EVENT_TYPE,
              NULL
            FROM
              encounters
            WHERE
              encounterclass = 'inpatient'
          ) RAWDATA
      ) E
    WHERE
      (2 * E.START_ORDINAL - E.OVERALL_ORD = 0)
  ),
  CTE_VISIT_ENDS AS (
    SELECT
      MIN(V.id) AS encounter_id,
      V.patient,
      V.encounterclass,
      V.start AS VISIT_START_DATE,
      MIN(E.END_DATE) AS VISIT_END_DATE
    FROM
      encounters V
        INNER JOIN CTE_END_DATES E
          ON V.patient = E.patient
          AND V.encounterclass = E.encounterclass
          AND E.END_DATE >= V.start
    GROUP BY
      V.patient,
      V.encounterclass,
      V.start
  )
  SELECT
    encounter_id,
    patient,
    encounterclass,
    MIN(VISIT_START_DATE) AS VISIT_START_DATE,
    VISIT_END_DATE
  FROM
    CTE_VISIT_ENDS
  GROUP BY
    encounter_id,
    patient,
    encounterclass,
    VISIT_END_DATE
);

CREATE TEMPORARY VIEW ER_VISITS AS
SELECT
  MIN(encounter_id) AS encounter_id,
  patient,
  encounterclass,
  VISIT_START_DATE,
  MAX(VISIT_END_DATE) AS VISIT_END_DATE
FROM
  (
    SELECT
      CL1.id AS encounter_id,
      CL1.patient,
      CL1.encounterclass,
      CL1.start AS VISIT_START_DATE,
      CL2.stop AS VISIT_END_DATE
    FROM
      encounters CL1
        INNER JOIN encounters CL2
          ON CL1.patient = CL2.patient
          AND CL1.start = CL2.start
          AND CL1.encounterclass = CL2.encounterclass
    WHERE
      CL1.encounterclass in ('emergency', 'urgent')
  ) T1
GROUP BY
  patient,
  encounterclass,
  VISIT_START_DATE;

CREATE TEMPORARY VIEW op_visits AS
WITH CTE_VISITS_DISTINCT AS (
  SELECT
    MIN(id) AS encounter_id,
    patient,
    encounterclass,
    start AS VISIT_START_DATE,
    stop AS VISIT_END_DATE
  FROM
    encounters
  WHERE
    encounterclass in ('ambulatory', 'wellness', 'outpatient')
  GROUP BY
    patient,
    encounterclass,
    start,
    stop
)
SELECT
  MIN(encounter_id) AS encounter_id,
  patient,
  encounterclass,
  VISIT_START_DATE,
  MAX(VISIT_END_DATE) AS VISIT_END_DATE
FROM
  CTE_VISITS_DISTINCT
GROUP BY
  patient,
  encounterclass,
  VISIT_START_DATE;

CREATE OR REFRESH MATERIALIZED VIEW assign_all_visit_ids AS
SELECT
  E.id AS encounter_id,
  E.patient as person_source_value,
  E.start AS date_service,
  E.stop AS date_service_end,
  E.encounterclass,
  AV.encounterclass AS VISIT_TYPE,
  AV.VISIT_START_DATE,
  AV.VISIT_END_DATE,
  AV.VISIT_OCCURRENCE_ID,
  CASE
    WHEN
      E.encounterclass = 'inpatient'
      and AV.encounterclass = 'inpatient'
    THEN
      VISIT_OCCURRENCE_ID
    WHEN
      E.encounterclass in ('emergency', 'urgent')
    THEN
      (
        CASE
          WHEN
            AV.encounterclass = 'inpatient'
            AND E.start > AV.VISIT_START_DATE
          THEN
            VISIT_OCCURRENCE_ID
          WHEN
            AV.encounterclass in ('emergency', 'urgent')
            AND E.start = AV.VISIT_START_DATE
          THEN
            VISIT_OCCURRENCE_ID
          ELSE NULL
        END
      )
    WHEN
      E.encounterclass in ('ambulatory', 'wellness', 'outpatient')
    THEN
      (
        CASE
          WHEN
            AV.encounterclass = 'inpatient'
            AND E.start >= AV.VISIT_START_DATE
          THEN
            VISIT_OCCURRENCE_ID
          WHEN
            AV.encounterclass in ('ambulatory', 'wellness', 'outpatient')
          THEN
            VISIT_OCCURRENCE_ID
          ELSE NULL
        END
      )
    ELSE NULL
  END AS VISIT_OCCURRENCE_ID_NEW
FROM
  encounters E
    INNER JOIN all_visits AV
      ON E.patient = AV.patient
      AND E.start >= AV.VISIT_START_DATE
      AND E.start <= AV.VISIT_END_DATE;

CREATE OR REFRESH MATERIALIZED VIEW all_visits AS
SELECT
  *,
  ROW_NUMBER() OVER (ORDER BY patient) as visit_occurrence_id
FROM
  (
    SELECT
      *
    FROM
      ip_visits
    UNION ALL
    SELECT
      *
    FROM
      er_visits
    UNION ALL
    SELECT
      *
    FROM
      op_visits
  );

CREATE OR REFRESH MATERIALIZED VIEW final_visit_ids AS
SELECT
  encounter_id,
  VISIT_OCCURRENCE_ID_NEW
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY PRIORITY) AS RN
    FROM
      (
        SELECT
          *,
          CASE
            WHEN
              encounterclass in ('emergency', 'urgent')
            THEN
              (
                CASE
                  WHEN
                    VISIT_TYPE = 'inpatient'
                    AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                  THEN
                    1
                  WHEN
                    VISIT_TYPE in ('emergency', 'urgent')
                    AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                  THEN
                    2
                  ELSE 99
                END
              )
            WHEN
              encounterclass in ('ambulatory', 'wellness', 'outpatient')
            THEN
              (
                CASE
                  WHEN
                    VISIT_TYPE = 'inpatient'
                    AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                  THEN
                    1
                  WHEN
                    VISIT_TYPE in ('ambulatory', 'wellness', 'outpatient')
                    AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                  THEN
                    2
                  ELSE 99
                END
              )
            WHEN
              encounterclass = 'inpatient'
              AND VISIT_TYPE = 'inpatient'
              AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
            THEN
              1
            ELSE 99
          END AS PRIORITY
        FROM
          assign_all_visit_ids
      ) T1
  ) T2
WHERE
  RN = 1;

CREATE OR REFRESH MATERIALIZED VIEW source_to_standard_vocab_map
  (


    CONSTRAINT source_concept_valid_id EXPECT(SOURCE_CONCEPT_ID IS NOT NULL) ON VIOLATION DROP ROW
  ) AS
SELECT
  c.concept_code AS SOURCE_CODE,
  c.concept_id AS SOURCE_CONCEPT_ID,
  c.concept_name AS SOURCE_CODE_DESCRIPTION,
  c.vocabulary_id AS SOURCE_VOCABULARY_ID,
  c.domain_id AS SOURCE_DOMAIN_ID,
  c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
  c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
  c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
  c.INVALID_REASON AS SOURCE_INVALID_REASON,
  c1.concept_id AS TARGET_CONCEPT_ID,
  c1.concept_name AS TARGET_CONCEPT_NAME,
  c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID,
  c1.domain_id AS TARGET_DOMAIN_ID,
  c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
  c1.INVALID_REASON AS TARGET_INVALID_REASON,
  c1.standard_concept AS TARGET_STANDARD_CONCEPT
FROM
  concept C
    JOIN concept_relationship CR
      ON C.CONCEPT_ID = CR.CONCEPT_ID_1
      AND CR.invalid_reason IS NULL
      AND lower(cr.relationship_id) = 'maps to'
    JOIN concept C1
      ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
      AND C1.INVALID_REASON IS NULL;

CREATE OR REFRESH MATERIALIZED VIEW source_to_source_vocab_map AS
SELECT
  c.concept_code AS SOURCE_CODE,
  c.concept_id AS SOURCE_CONCEPT_ID,
  c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
  c.vocabulary_id AS SOURCE_VOCABULARY_ID,
  c.domain_id AS SOURCE_DOMAIN_ID,
  c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
  c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
  c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
  c.invalid_reason AS SOURCE_INVALID_REASON,
  c.concept_ID as TARGET_CONCEPT_ID,
  c.concept_name AS TARGET_CONCEPT_NAME,
  c.vocabulary_id AS TARGET_VOCABULARY_ID,
  c.domain_id AS TARGET_DOMAIN_ID,
  c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
  c.INVALID_REASON AS TARGET_INVALID_REASON,
  c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
FROM
  CONCEPT c