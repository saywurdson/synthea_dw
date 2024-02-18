-- models/patient.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS patient_id,
    REPLACE(JSON_EXTRACT(p, '$.name[0].given[0]'), '"', '') AS first_name,
    REPLACE(JSON_EXTRACT(p, '$.name[0].family'), '"', '') AS last_name,
    REPLACE(JSON_EXTRACT(p, '$.gender'), '"', '') AS sex,
    REPLACE(JSON_EXTRACT(p, '$.extension[0].extension[0].valueCoding.display'), '"', '') AS race,
    CAST(REPLACE(JSON_EXTRACT(p, '$.birthDate'), '"', '') AS DATE) AS birth_date,
    CAST(REPLACE(JSON_EXTRACT(p, '$.deceasedDateTime'), '"', '') AS DATE) AS death_date,
    CASE
        WHEN JSON_EXTRACT(p, '$.deceasedDateTime') IS NOT NULL THEN 1
        ELSE 0
    END AS death_flag,
    REPLACE(REPLACE(REPLACE(JSON_EXTRACT(p, '$.address[0].line'), '"', ''), '[', ''), ']', '') AS address,
    REPLACE(JSON_EXTRACT(p, '$.address[0].city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(p, '$.address[0].state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(p, '$.address[0].postalCode'), '"', '') AS zip_code,
    NULL AS county,
    REPLACE(JSON_EXTRACT(p, '$.address[0].extension[0].extension[0].valueDecimal'), '"', '') AS latitude,
    REPLACE(JSON_EXTRACT(p, '$.address[0].extension[0].extension[1].valueDecimal'), '"', '') AS longitude,
    'SyntheaFhir' AS data_source
FROM "synthea"."json"."Patient" p