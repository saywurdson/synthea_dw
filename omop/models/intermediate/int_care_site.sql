-- models/intermediate/int_care_site.sql

WITH raw_data AS (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS id,
        data AS care_site_json 
    FROM {{ source('raw', 'Organization') }}
),

location_data AS (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.managingOrganization.identifier.value'), '"', '') AS managingOrganization,
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS location_id
    FROM {{ source('raw', 'Location') }}
),

int_location_data AS (
    SELECT DISTINCT
        location_source_value,
        location_id AS int_location_id
    FROM {{ ref('int_location') }} 
),

joined_data AS (
    SELECT DISTINCT
        rd.*,
        ld.location_id,
        ild.int_location_id,
        REPLACE(JSON_EXTRACT(rd.care_site_json, '$.id'), '"', '') AS care_site_source_value
    FROM raw_data rd
    JOIN location_data ld ON rd.id = ld.managingOrganization
    JOIN int_location_data ild ON ld.location_id = ild.location_source_value
)

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY care_site_source_value) AS care_site_id,
    REPLACE(JSON_EXTRACT(care_site_json, '$.name'), '"', '') AS care_site_name,
    8717 AS place_of_service_concept_id,
    joined_data.int_location_id AS location_id,
    care_site_source_value,
    REPLACE(JSON_EXTRACT(care_site_json, '$.type[0].coding[0].display'), '"', '') AS place_of_service_source_value
FROM joined_data