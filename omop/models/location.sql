-- models/location.sql

WITH raw_location_data AS (
    SELECT DISTINCT
        *,
        data AS location_json 
    FROM {{ source('raw', 'Location') }}
),

parsed_location_data AS (
    SELECT DISTINCT
        JSON_EXTRACT(location_json, '$.id') AS location_source_value,
        JSON_EXTRACT(location_json, '$.address.line[0]') AS address_1_raw,
        JSON_EXTRACT(location_json, '$.address.city') AS city_raw,
        JSON_EXTRACT(location_json, '$.address.state') AS state_raw,
        JSON_EXTRACT(location_json, '$.address.postalCode') AS zip_raw,
        JSON_EXTRACT(location_json, '$.address.country') AS country_source_value_raw,
        JSON_EXTRACT(location_json, '$.position.latitude') AS latitude_raw,
        JSON_EXTRACT(location_json, '$.position.longitude') AS longitude_raw
    FROM raw_location_data
),

raw_patient_data AS (
    SELECT DISTINCT
        *,
        data AS patient_json 
    FROM {{ source('raw', 'Patient') }}
),

parsed_patient_data AS (
    SELECT DISTINCT
        JSON_EXTRACT(patient_json, '$.id') AS patient_source_value,
        JSON_EXTRACT(patient_json, '$.address[0].line[0]') AS patient_address_1_raw,
        JSON_EXTRACT(patient_json, '$.address[0].city') AS patient_city_raw,
        JSON_EXTRACT(patient_json, '$.address[0].state') AS patient_state_raw,
        JSON_EXTRACT(patient_json, '$.address[0].postalCode') AS patient_zip_raw,
        JSON_EXTRACT(patient_json, '$.address[0].country') AS patient_country_source_value_raw,
        JSON_EXTRACT(patient_json, '$.address[0].extension[0].extension[0].valueDecimal') AS patient_latitude_raw,
        JSON_EXTRACT(patient_json, '$.address[0].extension[0].extension[1].valueDecimal') AS patient_longitude_raw
    FROM raw_patient_data
),

combined_data AS (
    SELECT DISTINCT
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
    FROM parsed_location_data

    UNION

    SELECT DISTINCT
        REPLACE(patient_address_1_raw, '"', '') AS address_1,
        NULL AS address_2,
        REPLACE(patient_city_raw, '"', '') AS city,
        REPLACE(patient_state_raw, '"', '') AS state,
        REPLACE(patient_zip_raw, '"', '') AS zip,
        NULL AS county,
        REPLACE(patient_source_value, '"', '') AS location_source_value,
        42046186 AS country_concept_id,
        REPLACE(patient_country_source_value_raw, '"', '') AS country_source_value,
        CAST(patient_latitude_raw AS FLOAT) AS latitude,
        CAST(patient_longitude_raw AS FLOAT) AS longitude
    FROM parsed_patient_data
)

SELECT
    ROW_NUMBER() OVER (ORDER BY location_source_value) AS location_id,
    address_1,
    address_2,
    city,
    state,
    zip,
    county,
    location_source_value,
    country_concept_id,
    country_source_value,
    latitude,
    longitude
FROM combined_data
WHERE address_1 IS NOT NULL