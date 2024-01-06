-- models/care_site.sql

WITH CareSiteData AS (
    SELECT
        REPLACE(JSON_EXTRACT(o.data, '$.id'), '"', '') AS care_site_id,
        REPLACE(JSON_EXTRACT(o.data, '$.name'), '"', '') AS care_site_name,
        REPLACE(JSON_EXTRACT(o.data, '$.id'), '"', '') AS care_site_source_value,
        REPLACE(JSON_EXTRACT(o.data, '$.type[0].coding[0].display'), '"', '') AS place_of_service_source_value
    FROM {{ source('raw', 'Organization') }} o
),
LocationData AS (
    SELECT
        REPLACE(JSON_EXTRACT(l.data, '$.managingOrganization.identifier.value'), '"', '') AS managingOrganization,
        REPLACE(JSON_EXTRACT(l.data, '$.id'), '"', '') AS location_id
    FROM {{ source('raw', 'Location') }} l
)

SELECT
    csd.care_site_id,
    csd.care_site_name,
    8717 AS place_of_service_concept_id,
    COALESCE(ld.location_id, NULL) AS location_id,
    csd.care_site_source_value,
    csd.place_of_service_source_value
FROM CareSiteData csd
LEFT JOIN LocationData ld ON csd.care_site_id = ld.managingOrganization