-- models/visit_occurrence.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(e, '$.id'), '"', '') AS visit_occurrence_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(e, '$.subject.reference'), '"', ''), '/', -1) AS person_id,
    CASE REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '')
        WHEN 'IMP' THEN 9201
        WHEN 'EMER' THEN 9203
        WHEN 'AMB' THEN 9202
        WHEN 'VR' THEN 5202
        ELSE 0
    END AS visit_concept_id,
    CAST(SUBSTRING(REPLACE(JSON_EXTRACT(e, '$.period.start'), '"', ''), 1, 10) AS DATE) AS visit_start_date,
    CAST(REPLACE(JSON_EXTRACT(e, '$.period.start'), '"', '') AS TIMESTAMP) AS visit_start_datetime,
    CAST(SUBSTRING(COALESCE(REPLACE(JSON_EXTRACT(e, '$.period.end'), '"', ''), REPLACE(JSON_EXTRACT(e, '$.period.start'), '"', '')), 1, 10) AS DATE) AS visit_end_date,
    COALESCE(REPLACE(JSON_EXTRACT(e, '$.period.end'), '"', ''), REPLACE(JSON_EXTRACT(e, '$.period.start'), '"', '')) AS visit_end_datetime,
    32817 AS visit_type_concept_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"', ''), '/', -1) AS provider_id,
    SPLIT_PART(REPLACE(JSON_EXTRACT(e, '$.serviceProvider.reference'), '"', ''), '/', -1) AS care_site_id,
    REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') AS visit_source_value,
    CASE REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '')
        WHEN 'IMP' THEN 9201
        WHEN 'EMER' THEN 9203
        WHEN 'AMB' THEN 9202
        WHEN 'VR' THEN 5202
        ELSE 0
    END AS visit_source_concept_id,
    0 AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    0 AS discharged_to_concept_id,
    NULL AS discharged_to_source_value,
    NULL AS preceding_visit_occurrence_id
FROM {{ source('json', 'Encounter') }} e