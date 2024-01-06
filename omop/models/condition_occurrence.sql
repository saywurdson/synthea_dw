-- models/condition_occurrence.sql

SELECT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS condition_occurrence_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.code.coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding', 'Observation') }} AS INTEGER) AS condition_concept_id,
    CAST(JSON_EXTRACT(data, '$.onsetDateTime') AS DATE) AS condition_start_date,
    CAST(JSON_EXTRACT(data, '$.onsetDateTime') AS TIMESTAMP) AS condition_start_datetime,
    CAST(JSON_EXTRACT(data, '$.abatementDateTime') AS DATE) AS condition_end_date,
    CAST(JSON_EXTRACT(data, '$.abatementDateTime') AS TIMESTAMP) AS condition_end_datetime,
    32817 AS condition_type_concept_id,
    32893 AS condition_status_concept_id,
    CASE 
        WHEN condition_end_date IS NULL OR condition_end_datetime IS NULL THEN NULL
        ELSE REPLACE(JSON_EXTRACT(data, '$.clinicalStatus.coding[0].code'), '"', '')
    END AS stop_reason,
    NULL AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS condition_source_value,
    CAST({{ get_concept_id('concept_code', 'data', '$.code.coding[0].code', 'SNOMED', 'Condition', 'Observation') }} AS INTEGER) AS condition_source_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.verificationStatus.coding[0].code'), '"', '') AS condition_status_source_value
FROM {{ source('raw', 'Condition') }}