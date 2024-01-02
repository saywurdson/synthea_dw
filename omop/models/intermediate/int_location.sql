-- models/intermediate/int_location.sql

WITH raw_data AS (
    SELECT
        *,
        data AS location_json 
    FROM {{ source('raw', 'Location') }}
),

parsed_data AS (
    SELECT
        JSON_EXTRACT(location_json, '$.id') AS location_source_value,
        JSON_EXTRACT(location_json, '$.address.line[0]') AS address_1_raw,
        JSON_EXTRACT(location_json, '$.address.city') AS city_raw,
        JSON_EXTRACT(location_json, '$.address.state') AS state_raw,
        JSON_EXTRACT(location_json, '$.address.postalCode') AS zip_raw,
        JSON_EXTRACT(location_json, '$.address.country') AS country_source_value_raw,
        JSON_EXTRACT(location_json, '$.position.latitude') AS latitude_raw,
        JSON_EXTRACT(location_json, '$.position.longitude') AS longitude_raw
    FROM raw_data
)

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY location_source_value) AS location_id,
    REPLACE(address_1_raw, '"', '') AS address_1,
    NULL AS address_2,
    REPLACE(city_raw, '"', '') AS city,
    REPLACE(state_raw, '"', '') AS state,
    REPLACE(zip_raw, '"', '') AS zip,
    NULL AS county,
    REPLACE(location_source_value, '"', '') AS location_source_value,
    42046186 AS country_concept_id,
    REPLACE(country_source_value_raw, '"', '') AS country_source_value,
    CAST(latitude_raw AS FLOAT) AS latitude,
    CAST(longitude_raw AS FLOAT) AS longitude
FROM parsed_data