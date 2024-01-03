-- models/condition_occurrence.sql

WITH condition_data AS (
    SELECT DISTINCT
        data AS condition_json
    FROM {{ source('raw', 'Condition') }}
),

parsed_data AS (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(condition_json, '$.code.coding[0].code'), '"', '') AS condition_source_value,
        REPLACE(REPLACE(JSON_EXTRACT(condition_json, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_source_value,
        CAST({{ get_standard_concept_id('concept_code', 'condition_json', '$.code.coding[0].code', 'SNOMED', 'Condition', 'Clinical Finding', 'Observation') }} AS INTEGER) AS condition_concept_id,
        CAST({{ get_concept_id('concept_code', 'condition_json', '$.code.coding[0].code', 'SNOMED', 'Condition', 'Observation') }} AS INTEGER) AS condition_source_concept_id,
        CAST(JSON_EXTRACT(condition_json, '$.onsetDateTime') AS DATE) AS condition_start_date,
        CAST(JSON_EXTRACT(condition_json, '$.onsetDateTime') AS TIMESTAMP) AS condition_start_datetime,
        CAST(JSON_EXTRACT(condition_json, '$.abatementDateTime') AS DATE) AS condition_end_date,
        CAST(JSON_EXTRACT(condition_json, '$.abatementDateTime') AS TIMESTAMP) AS condition_end_datetime,
        CASE 
            WHEN condition_end_date IS NULL OR condition_end_datetime IS NULL THEN NULL
            ELSE REPLACE(JSON_EXTRACT(condition_json, '$.clinicalStatus.coding[0].code'), '"', '')
        END AS stop_reason,
        REPLACE(JSON_EXTRACT(condition_json, '$.code.coding[0].display'), '"', '') AS condition_type_source_value,
        REPLACE(JSON_EXTRACT(condition_json, '$.verificationStatus.coding[0].code'), '"', '') AS condition_status_source_value
    FROM condition_data
),

patient_data AS (
    SELECT DISTINCT
        person_id,
        person_source_value
    FROM {{ ref('person') }}
),

joined_data AS (
    SELECT DISTINCT
        pd.condition_source_value,
        p.person_id AS person_id,
        pd.condition_concept_id,
        pd.condition_start_date,
        pd.condition_start_datetime,
        pd.condition_end_date,
        pd.condition_end_datetime,
        32817 AS condition_type_concept_id, 
        32893 AS condition_status_concept_id,
        pd.stop_reason,
        NULL AS provider_id, 
        NULL AS visit_occurrence_id,  
        NULL AS visit_detail_id,  
        pd.condition_type_source_value,
        pd.condition_source_concept_id, 
        pd.condition_status_source_value
    FROM parsed_data pd
    LEFT JOIN patient_data p ON pd.patient_source_value = p.person_source_value
)

SELECT
    ROW_NUMBER() OVER (ORDER BY condition_source_value) AS condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    32817 AS condition_type_concept_id,
    32893 AS condition_status_concept_id,
    stop_reason,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value
FROM joined_data
WHERE person_id IS NOT NULL