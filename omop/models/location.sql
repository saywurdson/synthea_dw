-- models/location.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(l, '$.id'), '"', '') AS location_id,
    REPLACE(JSON_EXTRACT(l, '$.address.line[0]'), '"', '') AS address_1,
    NULL AS address_2,
    REPLACE(JSON_EXTRACT(l, '$.address.city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(l, '$.address.state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(l, '$.address.postalCode'), '"', '') AS zip,
    NULL AS county,
    REPLACE(JSON_EXTRACT(l, '$.name'), '"', '') AS location_source_value,
    CASE
        WHEN REPLACE(JSON_EXTRACT(l, '$.address.country'), '"', '') = 'US' THEN 42046186
        ELSE 0
    END AS country_concept_id,
    REPLACE(JSON_EXTRACT(l, '$.address.country'), '"', '') AS country_source_value,
    CAST(JSON_EXTRACT(l, '$.position.latitude') AS DECIMAL(9,6)) AS latitude,
    CAST(JSON_EXTRACT(l, '$.position.longitude') AS DECIMAL(9,6)) AS longitude
FROM {{ source('json', 'Location') }} l

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS location_id,
    COALESCE(REPLACE(JSON_EXTRACT(p, '$.address[0].line[0]'), '"', ''), NULL) AS address_1,
    NULL AS address_2,
    REPLACE(JSON_EXTRACT(p, '$.address[0].city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(p, '$.address[0].state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(p, '$.address[0].postalCode'), '"', '') AS zip,
    NULL AS county,
    NULL AS location_source_value,
    CASE
        WHEN REPLACE(JSON_EXTRACT(p, '$.address[0].country'), '"', '') = 'US' THEN 42046186
        ELSE 0
    END AS country_concept_id,
    REPLACE(JSON_EXTRACT(p, '$.address[0].country'), '"', '') AS country_source_value,
    CAST(REPLACE(JSON_EXTRACT(JSON_EXTRACT(p, '$.address[0].extension[0].extension[0]'), '$.valueDecimal'), '"', '') AS DECIMAL(9,6)) AS latitude,
    CAST(REPLACE(JSON_EXTRACT(JSON_EXTRACT(p, '$.address[0].extension[0].extension[1]'), '$.valueDecimal'), '"', '') AS DECIMAL(9,6)) AS longitude
FROM {{ source('json', 'Patient') }} p