CREATE OR REFRESH MATERIALIZED VIEW person AS
SELECT
  ROW_NUMBER() OVER (ORDER BY p.id) as PERSON_ID,
  case upper(p.gender)
    when 'M' then 8507
    when 'F' then 8532
  end as GENDER_CONCEPT_ID,
  YEAR(p.birthdate) as YEAR_OF_BIRTH,
  MONTH(p.birthdate) as MONTH_OF_BIRTH,
  DAY(p.birthdate) as DAY_OF_BIRTH,
  p.birthdate as BIRTH_DATETIME,
  case upper(p.race)
    when 'WHITE' then 8527
    when 'BLACK' then 8516
    when 'ASIAN' then 8515
    else 0
  end as RACE_CONCEPT_ID,
  case
    when upper(p.race) = 'HISPANIC' then 38003563
    else 0
  end as ETHNICITY_CONCEPT_ID,
  1 as LOCATION_ID,
  0 as PROVIDER_ID,
  0 as CARE_SITE_ID,
  p.id as PERSON_SOURCE_VALUE,
  p.gender as GENDER_SOURCE_VALUE,
  0 as GENDER_SOURCE_CONCEPT_ID,
  p.race as RACE_SOURCE_VALUE,
  0 as RACE_SOURCE_CONCEPT_ID,
  p.ethnicity as ETHNICITY_SOURCE_VALUE,
  0 as ETHNICITY_SOURCE_CONCEPT_ID
from
  patients p
where
  p.gender is not null;

CREATE OR REFRESH MATERIALIZED VIEW condition_occurrence AS
select
  row_number() over (order by p.person_id) as CONDITION_OCCURRENCE_ID,
  p.person_id as PERSON_ID,
  coalesce(srctostdvm.target_concept_id, 0) AS CONDITION_CONCEPT_ID,
  c.start as CONDITION_START_DATE,
  c.start as CONDITION_START_DATETIME,
  c.stop as CONDITION_END_DATE,
  c.stop as CONDITION_END_DATETIME,
  32020 as CONDITION_TYPE_CONCEPT_ID,
  "" as STOP_REASON,
  0 as PROVIDER_ID,
  fv.visit_occurrence_id_new AS VISIT_OCCURRENCE_ID,
  0 as VISIT_DETAIL_ID,
  c.code as CONDITION_SOURCE_VALUE,
  coalesce(srctosrcvm.source_concept_id, 0) as CONDITION_SOURCE_CONCEPT_ID,
  0 as CONDITION_STATUS_SOURCE_VALUE,
  0 as CONDITION_STATUS_CONCEPT_ID
from
  conditions c
    inner join source_to_standard_vocab_map srctostdvm
      on srctostdvm.source_code = c.code
      and srctostdvm.target_domain_id = 'Condition'
      and srctostdvm.source_vocabulary_id = 'SNOMED'
      and srctostdvm.target_standard_concept = 'S'
      and (
        srctostdvm.target_invalid_reason IS NULL
        OR srctostdvm.target_invalid_reason = ''
      )
    left join source_to_source_vocab_map srctosrcvm
      on srctosrcvm.source_code = c.code
      and srctosrcvm.source_vocabulary_id = 'SNOMED'
    left join final_visit_ids fv
      on fv.encounter_id = c.encounter
    inner join person p
      on c.patient = p.person_source_value;

CREATE OR REFRESH MATERIALIZED VIEW drug_exposure AS
SELECT
  row_number() over (order by person_id) AS drug_exposure_id,
  *
FROM
  (
    SELECT
      p.person_id,
      coalesce(srctostdvm.target_concept_id, 0) AS drug_concept_id,
      c.start AS drug_exposure_start_date,
      c.start AS drug_exposure_start_datetime,
      coalesce(c.stop, c.start) AS drug_exposure_end_date,
      coalesce(c.stop, c.start) AS drug_exposure_end_datetime,
      c.stop AS verbatim_end_date,
      581452 AS drug_type_concept_id,
      '' AS stop_reason, -- Changed from null to empty string
      0 AS refills,
      0 AS quantity,
      coalesce(datediff(c.stop, c.start), 0) AS days_supply,
      '' AS sig, -- Changed from null to empty string
      0 AS route_concept_id,
      0 AS lot_number,
      0 AS provider_id,
      fv.visit_occurrence_id_new AS visit_occurrence_id,
      0 AS visit_detail_id,
      c.code AS drug_source_value,
      coalesce(srctosrcvm.source_concept_id, 0) AS drug_source_concept_id,
      '' AS route_source_value, -- Changed from null to empty string
      '' AS dose_unit_source_value -- Changed from null to empty string
    FROM
      conditions c
        JOIN source_to_standard_vocab_map srctostdvm
          ON srctostdvm.source_code = c.code
          AND srctostdvm.target_domain_id = 'Drug'
          AND srctostdvm.source_vocabulary_id = 'RxNorm'
          AND srctostdvm.target_standard_concept = 'S'
          AND (
            srctostdvm.target_invalid_reason IS NULL
            OR srctostdvm.target_invalid_reason = ''
          )
        LEFT JOIN source_to_source_vocab_map srctosrcvm
          ON srctosrcvm.source_code = c.code
          AND srctosrcvm.source_vocabulary_id = 'RxNorm'
        LEFT JOIN final_visit_ids fv
          ON fv.encounter_id = c.encounter
        JOIN person p
          ON p.person_source_value = c.patient
    UNION ALL
    SELECT
      p.person_id,
      coalesce(srctostdvm.target_concept_id, 0) AS drug_concept_id,
      m.start,
      m.start,
      coalesce(m.stop, m.start),
      coalesce(m.stop, m.start),
      m.stop,
      38000177,
      '' AS stop_reason,
      0,
      0,
      coalesce(datediff(m.stop, m.start), 0),
      '' AS sig,
      0,
      0,
      0,
      fv.visit_occurrence_id_new AS visit_occurrence_id,
      0,
      m.code,
      coalesce(srctosrcvm.source_concept_id, 0),
      '' AS route_source_value,
      '' AS dose_unit_source_value
    FROM
      medications m
        JOIN source_to_standard_vocab_map srctostdvm
          ON srctostdvm.source_code = m.code
          AND srctostdvm.target_domain_id = 'Drug'
          AND srctostdvm.source_vocabulary_id = 'RxNorm'
          AND srctostdvm.target_standard_concept = 'S'
          AND (
            srctostdvm.target_invalid_reason IS NULL
            OR srctostdvm.target_invalid_reason = ''
          )
        LEFT JOIN source_to_source_vocab_map srctosrcvm
          ON srctosrcvm.source_code = m.code
          AND srctosrcvm.source_vocabulary_id = 'RxNorm'
        LEFT JOIN final_visit_ids fv
          ON fv.encounter_id = m.encounter
        JOIN person p
          ON p.person_source_value = m.patient
    UNION ALL
    SELECT
      p.person_id,
      coalesce(srctostdvm.target_concept_id, 0) AS drug_concept_id,
      i.date,
      i.date,
      i.date,
      i.date,
      i.date,
      581452,
      '' AS stop_reason,
      0,
      0,
      0,
      '' AS sig,
      0,
      0,
      0,
      fv.visit_occurrence_id_new AS visit_occurrence_id,
      0,
      i.code,
      coalesce(srctosrcvm.source_concept_id, 0),
      '' AS route_source_value,
      '' AS dose_unit_source_value
    FROM
      immunizations i
        LEFT JOIN source_to_standard_vocab_map srctostdvm
          ON srctostdvm.source_code = i.code
          AND srctostdvm.target_domain_id = 'Drug'
          AND srctostdvm.source_vocabulary_id = 'CVX'
          AND srctostdvm.target_standard_concept = 'S'
          AND (
            srctostdvm.target_invalid_reason IS NULL
            OR srctostdvm.target_invalid_reason = ''
          )
        LEFT JOIN source_to_source_vocab_map srctosrcvm
          ON srctosrcvm.source_code = i.code
          AND srctosrcvm.source_vocabulary_id = 'CVX'
        LEFT JOIN final_visit_ids fv
          ON fv.encounter_id = i.encounter
        JOIN person p
          ON p.person_source_value = i.patient
  ) tmp;