-- models/care_site.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(o.data, '$.id'), '"', '') AS care_site_id,
    REPLACE(JSON_EXTRACT(o.data, '$.name'), '"', '') AS care_site_name,
    8717 AS place_of_service_concept_id,
    REPLACE(JSON_EXTRACT(l.data, '$.id'), '"', '') AS location_id,
    REPLACE(JSON_EXTRACT(o.data, '$.id'), '"', '') AS care_site_source_value,
    REPLACE(JSON_EXTRACT(o.data, '$.type[0].coding[0].display'), '"', '') AS place_of_service_source_value
FROM {{ source('raw', 'Organization') }} o
LEFT JOIN {{ source('raw', 'Location') }} l 
ON REPLACE(JSON_EXTRACT(o.data, '$.id'), '"', '') = REPLACE(JSON_EXTRACT(l.data, '$.managingOrganization.identifier.value'), '"', '')