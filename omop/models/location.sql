-- models/location.sql

SELECT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS location_id,
    REPLACE(JSON_EXTRACT(data, '$.address.line[0]'), '"', '') AS address_1,
    NULL AS address_2,
    REPLACE(JSON_EXTRACT(data, '$.address.city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(data, '$.address.state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(data, '$.address.postalCode'), '"', '') AS zip,
    NULL AS county,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS location_source_value,
    CASE
        WHEN REPLACE(JSON_EXTRACT(data, '$.address.country'), '"', '') = 'US' THEN 42046186
        ELSE 0
    END AS country_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.address.country'), '"', '') AS country_source_value,
    CAST(JSON_EXTRACT(data, '$.position.latitude') AS FLOAT) AS latitude,
    CAST(JSON_EXTRACT(data, '$.position.longitude') AS FLOAT) AS longitude
FROM {{ source('raw', 'Location') }}
WHERE JSON_EXTRACT(data, '$.address.line[0]') IS NOT NULL

UNION ALL

SELECT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS location_id,
    REPLACE(JSON_EXTRACT(data, '$.address[0].line[0]'), '"', '') AS address_1,
    NULL AS address_2,
    REPLACE(JSON_EXTRACT(data, '$.address[0].city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(data, '$.address[0].state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(data, '$.address[0].postalCode'), '"', '') AS zip,
    NULL AS county,
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS location_source_value,
    CASE
        WHEN REPLACE(JSON_EXTRACT(data, '$.address[0].country'), '"', '') = 'US' THEN 42046186
        ELSE 0
    END AS country_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.address[0].country'), '"', '') AS country_source_value,
    CAST(JSON_EXTRACT(data, '$.address[0].extension[0].extension[0].valueDecimal') AS FLOAT) AS latitude,
    CAST(JSON_EXTRACT(data, '$.address[0].extension[0].extension[1].valueDecimal') AS FLOAT) AS longitude
FROM {{ source('raw', 'Patient') }}