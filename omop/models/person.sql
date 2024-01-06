-- models/person.sql

WITH PatientData AS (
    SELECT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS person_id,
        data
    FROM {{ source('raw', 'Patient') }}
),
VisitData AS (
    SELECT
        person_id AS visit_person_id,
        care_site_id AS visit_care_site_id,
        provider_id AS visit_provider_id
    FROM {{ ref('visit_occurrence') }}
)

SELECT
    pd.person_id,
    {{ get_standard_concept_id('concept_code', 'pd.data', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_concept_id,
    CAST(EXTRACT(YEAR FROM JSON_EXTRACT(pd.data, '$.birthDate')::DATE) AS INTEGER) AS year_of_birth,
    CAST(EXTRACT(MONTH FROM JSON_EXTRACT(pd.data, '$.birthDate')::DATE) AS INTEGER) AS month_of_birth,
    CAST(EXTRACT(DAY FROM JSON_EXTRACT(pd.data, '$.birthDate')::DATE) AS INTEGER) AS day_of_birth,
    REPLACE(JSON_EXTRACT(pd.data, '$.birthDate'), '"', '')::TIMESTAMP AS birth_datetime,
    {{ get_standard_concept_id('concept_name', 'pd.data', '$.extension[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_concept_id,
    {{ get_standard_concept_id('concept_name', 'pd.data', '$.extension[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_concept_id,
    pd.person_id AS location_id,
    COALESCE(vd.visit_provider_id, NULL) AS provider_id,
    COALESCE(vd.visit_care_site_id, NULL) AS care_site_id,
    pd.person_id AS person_source_value,
    REPLACE(JSON_EXTRACT(pd.data, '$.extension[3].valueCode'), '"', '') AS gender_source_value,
    {{ get_concept_id('concept_code', 'pd.data', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_source_concept_id,
    REPLACE(JSON_EXTRACT(pd.data, '$.extension[0].extension[0].valueCoding.display'), '"', '') AS race_source_value,
    {{ get_concept_id('concept_name', 'pd.data', '$.extension[0].extension[0].valueCoding.display', 'Race', 'Race', 'Race') }} AS race_source_concept_id,
    REPLACE(JSON_EXTRACT(pd.data, '$.extension[1].extension[0].valueCoding.display'), '"', '') AS ethnicity_source_value,
    {{ get_concept_id('concept_name', 'pd.data', '$.extension[1].extension[0].valueCoding.display', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_source_concept_id
FROM PatientData pd
LEFT JOIN VisitData vd ON pd.person_id = vd.visit_person_id