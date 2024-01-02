-- models/intermediate/int_patient.sql

WITH raw_data AS (
    SELECT
        *,
        data AS patient_json 
    FROM {{ source('raw', 'Patient') }}
),

parsed_data AS (
    SELECT
        JSON_EXTRACT(patient_json, '$.gender') AS gender_raw,
        JSON_EXTRACT(patient_json, '$.birthDate') AS birth_date_raw,
        JSON_EXTRACT(patient_json, '$.extension') AS extension_raw,
        JSON_EXTRACT(patient_json, '$.id') AS person_source_value
    FROM raw_data
)

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY person_source_value) AS person_id,
    {{ get_concept_id('concept_code', '$[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_concept_id,
    EXTRACT(YEAR FROM pd.birth_date_raw::DATE) AS year_of_birth,
    EXTRACT(MONTH FROM pd.birth_date_raw::DATE) AS month_of_birth,
    EXTRACT(DAY FROM pd.birth_date_raw::DATE) AS day_of_birth,
    pd.birth_date_raw::TIMESTAMP AS birth_datetime,
    {{ get_concept_id('concept_name', '$[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_concept_id,
    {{ get_concept_id('concept_name', '$[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_concept_id,
    NULL AS location_id,
    NULL AS provider_id,
    NULL AS care_site_id,
    REPLACE(pd.person_source_value, '"', '') AS person_source_value,
    REPLACE(CAST(JSON_EXTRACT(pd.extension_raw, '$[3].valueCode') AS VARCHAR), '"', '') AS gender_source_value,
    0 AS gender_source_concept_id,
    REPLACE(CAST(JSON_EXTRACT(pd.extension_raw, '$[0].extension[0].valueCoding.display') AS VARCHAR), '"', '') AS race_source_value,
    0 AS race_source_concept_id,
    REPLACE(CAST(JSON_EXTRACT(pd.extension_raw, '$[1].extension[0].valueCoding.display') AS VARCHAR), '"', '') AS ethnicity_source_value,
    0 AS ethnicity_source_concept_id
FROM parsed_data pd