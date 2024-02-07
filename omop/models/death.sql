-- models/death.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS person_id,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.deceasedDateTime'), 2, 10) AS DATE) AS death_date,
    32817 AS death_type_concept_id,
    0 AS cause_concept_id, 
    NULL AS cause_source_value,
    0 AS cause_source_concept_id
FROM
    {{ source('json', 'Patient') }} p
WHERE
    JSON_EXTRACT(p, '$.deceasedDateTime') IS NOT NULL
