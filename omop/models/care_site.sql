-- models/care_site.sql

SELECT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS care_site_id,
    REPLACE(JSON_EXTRACT(data, '$.name'), '"', '') AS care_site_name,
    8717 AS place_of_service_concept_id,
    NULL AS location_id,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS care_site_source_value,
    REPLACE(JSON_EXTRACT(data, '$.type[0].coding[0].display'), '"', '') AS place_of_service_source_value
FROM {{ source('raw', 'Organization') }}