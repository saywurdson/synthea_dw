-- models/episode.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(ct, '$.id'), '"', '') AS episode_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(ct, '$.participant[0].member.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id('concept_code', 'ct', '$.reasonCode[0].coding[0].code', 'SNOMED', 'Condition') }} AS episode_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(ct, '$.period.start'), 2, 10) AS DATE) AS episode_start_date,
    CAST(JSON_EXTRACT(ct, '$.period.start') AS TIMESTAMP) AS episode_start_datetime,
    CAST(SUBSTRING(JSON_EXTRACT(ct, '$.period.end'), 2, 10) AS DATE) AS episode_end_date,
    CAST(JSON_EXTRACT(ct, '$.period.end') AS TIMESTAMP) AS episode_end_datetime,
    NULL AS episode_parent_id,
    1 AS episode_number,
    32533 AS episode_object_concept_id,
    32817 AS episode_type_concept_id,
    REPLACE(JSON_EXTRACT(ct, '$.reasonCode[0].coding[0].code'), '"', '') AS episode_source_value,
    {{ get_concept_id('concept_code', 'ct', '$.reasonCode[0].coding[0].code', 'SNOMED', 'Condition') }} AS episode_source_concept_id
FROM {{ source('json', 'CareTeam') }} ct
WHERE
    {{ get_concept_id('concept_code', 'ct', '$.reasonCode[0].coding[0].code', 'SNOMED', 'Condition') }} IS NOT NULL