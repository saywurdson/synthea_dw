-- models/care_site.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS care_site_id,
    REPLACE(JSON_EXTRACT(o, '$.name'), '"', '') AS care_site_name,
    32693 AS place_of_service_concept_id, 
    REPLACE(JSON_EXTRACT(l, '$.id'), '"', '') AS location_id,  
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS care_site_source_value,
    'Healthcare Provider' AS place_of_service_source_value
FROM {{ source('json', 'Organization') }} o
LEFT JOIN {{ source('json', 'Location') }} l 
    ON REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') = REPLACE(JSON_EXTRACT(l, '$.managingOrganization.identifier.value'), '"', '')
