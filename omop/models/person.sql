-- models/person.sql

WITH raw_data AS (
    SELECT DISTINCT
        *,
        data AS patient_json 
    FROM {{ source('raw', 'Patient') }}
),

parsed_data AS (
    SELECT DISTINCT
        JSON_EXTRACT(patient_json, '$.extension[3].valueCode') AS gender_raw,
        JSON_EXTRACT(patient_json, '$.birthDate') AS birth_date_raw,
        JSON_EXTRACT(patient_json, '$.extension') AS extension_raw,
        REPLACE(JSON_EXTRACT(patient_json, '$.id'), '"', '') AS person_source_value
    FROM raw_data
),

location_data AS (
    SELECT DISTINCT
        location_id,
        location_source_value
    FROM {{ ref('location') }}
),

joined_data AS (
    SELECT DISTINCT
        pd.*,
        ld.location_id
    FROM parsed_data pd
    LEFT JOIN location_data ld ON pd.person_source_value = ld.location_source_value
)

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY person_source_value) AS person_id,
    {{ get_standard_concept_id('concept_code', 'gender_raw', '$', 'Gender', 'Gender', 'Gender') }} AS gender_concept_id,
    EXTRACT(YEAR FROM CAST(birth_date_raw AS DATE)) AS year_of_birth,
    EXTRACT(MONTH FROM CAST(birth_date_raw AS DATE)) AS month_of_birth,
    EXTRACT(DAY FROM CAST(birth_date_raw AS DATE)) AS day_of_birth,
    CAST(birth_date_raw AS TIMESTAMP) AS birth_datetime,
    {{ get_standard_concept_id('concept_name', 'extension_raw', '$[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_concept_id,
    {{ get_standard_concept_id('concept_name', 'extension_raw', '$[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_concept_id,
    location_id,
    NULL AS provider_id,
    NULL AS care_site_id,
    REPLACE(person_source_value, '"', '') AS person_source_value,
    REPLACE(gender_raw, '"', '') AS gender_source_value,
    {{ get_concept_id('concept_code', 'gender_raw', '$', 'Gender', 'Gender', 'Gender') }} AS gender_source_concept_id,
    extension_raw ->> '$[0].extension[0].valueCoding.display' AS race_source_value,
    {{ get_concept_id('concept_name', 'extension_raw', '$[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_source_concept_id,
    extension_raw ->> '$[1].extension[0].valueCoding.display' AS ethnicity_source_value,
    {{ get_concept_id('concept_name', 'extension_raw', '$[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_source_concept_id
FROM joined_data