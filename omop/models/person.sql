-- models/person.sql

WITH PatientData AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '')) AS person_id,
        {{ get_standard_concept_id('concept_code', 'p.data', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_concept_id,
        CAST(EXTRACT(YEAR FROM JSON_EXTRACT(p.data, '$.birthDate')::DATE) AS INTEGER) AS year_of_birth,
        CAST(EXTRACT(MONTH FROM JSON_EXTRACT(p.data, '$.birthDate')::DATE) AS INTEGER) AS month_of_birth,
        CAST(EXTRACT(DAY FROM JSON_EXTRACT(p.data, '$.birthDate')::DATE) AS INTEGER) AS day_of_birth,
        REPLACE(JSON_EXTRACT(p.data, '$.birthDate'), '"', '')::TIMESTAMP AS birth_datetime,
        {{ get_standard_concept_id('concept_name', 'p.data', '$.extension[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_concept_id,
        {{ get_standard_concept_id('concept_name', 'p.data', '$.extension[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_concept_id,
        REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '') AS location_id,
        COALESCE(v.care_site_id, NULL) AS care_site_id,
        COALESCE(v.provider_id, NULL) AS provider_id,
        REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '') AS person_source_value,
        REPLACE(JSON_EXTRACT(p.data, '$.extension[3].valueCode'), '"', '') AS gender_source_value,
        {{ get_concept_id('concept_code', 'p.data', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_source_concept_id,
        REPLACE(JSON_EXTRACT(p.data, '$.extension[0].extension[0].valueCoding.display'), '"', '') AS race_source_value,
        {{ get_concept_id('concept_name', 'p.data', '$.extension[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_source_concept_id,
        REPLACE(JSON_EXTRACT(p.data, '$.extension[1].extension[0].valueCoding.display'), '"', '') AS ethnicity_source_value,
        {{ get_concept_id('concept_name', 'p.data', '$.extension[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_source_concept_id,
        ROW_NUMBER() OVER (PARTITION BY REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '') ORDER BY COALESCE(v.care_site_id, '') ASC) as seq_num
    FROM {{ source('raw', 'Patient') }} p
    LEFT JOIN {{ ref('visit_occurrence') }} v ON REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '') = v.person_id
)

SELECT 
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    care_site_id,
    provider_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
FROM PatientData
WHERE seq_num = 1