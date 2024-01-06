-- models/visit_occurrence.sql

SELECT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS visit_occurrence_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.serviceProvider.reference'), '"Organization/', ''), '"', '') AS care_site_id,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS encounter_source_value,
    CASE 
        WHEN JSON_EXTRACT(data, '$.class.code') = '"AMB"' THEN 9202
        WHEN JSON_EXTRACT(data, '$.class.code') = '"IMP"' THEN 9201
        WHEN JSON_EXTRACT(data, '$.class.code') = '"EMER"' THEN 9203
        WHEN JSON_EXTRACT(data, '$.class.code') = '"VR"' THEN 5083
        WHEN JSON_EXTRACT(data, '$.class.code') = '"HH"' THEN 38004519
        ELSE 0 
    END AS visit_concept_id,
    CAST(JSON_EXTRACT(data, '$.period.start') AS DATE) AS visit_start_date,
    CAST(JSON_EXTRACT(data, '$.period.start') AS TIMESTAMP) AS visit_start_datetime,
    CAST(JSON_EXTRACT(data, '$.period.end') AS DATE) AS visit_end_date,
    CAST(JSON_EXTRACT(data, '$.period.end') AS TIMESTAMP) AS visit_end_datetime,
    32817 AS visit_type_concept_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '') AS provider_id,
    REPLACE(JSON_EXTRACT(data, '$.class.code'), '"', '') AS visit_source_value,
    0 AS visit_source_concept_id,
    0 AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    0 AS discharged_to_concept_id,
    NULL AS discharged_to_source_value,
    0 AS preceding_visit_occurrence_id
FROM {{ source('raw', 'Encounter') }}