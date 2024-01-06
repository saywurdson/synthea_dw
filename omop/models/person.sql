-- models/person.sql

SELECT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS person_id,
    {{ get_standard_concept_id('concept_code', 'data', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_concept_id,
    CAST(EXTRACT(YEAR FROM JSON_EXTRACT(data, '$.birthDate')::DATE) AS INTEGER) AS year_of_birth,
    CAST(EXTRACT(MONTH FROM JSON_EXTRACT(data, '$.birthDate')::DATE) AS INTEGER) AS month_of_birth,
    CAST(EXTRACT(DAY FROM JSON_EXTRACT(data, '$.birthDate')::DATE) AS INTEGER) AS day_of_birth,
    REPLACE(JSON_EXTRACT(data, '$.birthDate'), '"', '')::TIMESTAMP AS birth_datetime,
    {{ get_standard_concept_id('concept_name', 'data', '$.extension[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_concept_id,
    {{ get_standard_concept_id('concept_name', 'data', '$.extension[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS location_id,
    NULL AS provider_id,
    NULL AS care_site_id,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS person_source_value,
    REPLACE(JSON_EXTRACT(data, '$.extension[3].valueCode'), '"', '') AS gender_source_value,
    {{ get_concept_id('concept_code', 'data', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_source_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.extension[0].extension[0].valueCoding.display'), '"', '') AS race_source_value,
    {{ get_concept_id('concept_name', 'data', '$.extension[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_source_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.extension[1].extension[0].valueCoding.display'), '"', '') AS ethnicity_source_value,
    {{ get_concept_id('concept_name', 'data', '$.extension[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_source_concept_id
FROM {{ source('raw', 'Patient') }}