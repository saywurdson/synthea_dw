-- models/person.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS person_id,
    EXTRACT(YEAR FROM CAST(SUBSTRING(JSON_EXTRACT(p, '$.birthDate'), 2, 10) AS DATE)) AS year_of_birth,
    EXTRACT(MONTH FROM CAST(SUBSTRING(JSON_EXTRACT(p, '$.birthDate'), 2, 10) AS DATE)) AS month_of_birth,
    EXTRACT(DAY FROM CAST(SUBSTRING(JSON_EXTRACT(p, '$.birthDate'), 2, 10) AS DATE)) AS day_of_birth,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.birthDate'), 2, 10) AS TIMESTAMP) AS birth_datetime,
    {{ get_standard_concept_id('concept_code', 'p', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_concept_id,
    {{ get_standard_concept_id('concept_name', 'p', '$.extension[0].extension[1].valueString', 'Race', 'Race', 'Race') }} AS race_concept_id,
    {{ get_standard_concept_id('concept_name', 'p', '$.extension[1].extension[1].valueString', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_concept_id,
    vo.care_site_id,
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS location_id,
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS person_source_value,
    REPLACE(JSON_EXTRACT(p, '$.extension[3].valueCode'), '"', '') AS gender_source_value,
    {{ get_concept_id('concept_code', 'p', '$.extension[3].valueCode', 'Gender', 'Gender', 'Gender') }} AS gender_source_concept_id,
    REPLACE(JSON_EXTRACT(p, '$.extension[0].extension[1].valueString'), '"', '') AS race_source_value,
    {{ get_concept_id('concept_name', 'p', '$.extension[0].extension[1].valueString', 'Race', 'Race', 'Race') }} AS race_source_concept_id,
    REPLACE(JSON_EXTRACT(p, '$.extension[1].extension[1].valueString'), '"', '') AS ethnicity_source_value,
    {{ get_concept_id('concept_name', 'p', '$.extension[1].extension[1].valueString', 'Ethnicity', 'Ethnicity', 'Ethnicity') }} AS ethnicity_source_concept_id
FROM {{ source('json', 'Patient') }} p
LEFT JOIN (
    SELECT
        person_id,
        care_site_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = vo.person_id