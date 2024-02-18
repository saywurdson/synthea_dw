-- models/eligibility.sql

SELECT DISTINCT
    REPLACE(REPLACE(JSON_EXTRACT(e, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
    NULL AS member_id,
    REPLACE(JSON_EXTRACT(p, '$.extension[3].valueCode'), '"', '') AS gender,
    REPLACE(JSON_EXTRACT(p, '$.extension[0].extension[1].valueString'), '"', '') AS race,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.birthDate'), 2, 10) AS DATE) AS birth_date,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.deceasedDateTime'), 2, 10) AS DATE) AS death_date,
    CASE
        WHEN JSON_EXTRACT(p, '$.deceasedDateTime') IS NOT NULL THEN 1
        ELSE 0
    END AS death_flag,
    CAST(SUBSTRING(JSON_EXTRACT(e, '$.billablePeriod.start'), 2, 10) AS DATE) AS enrollment_start_date,
    CAST(SUBSTRING(JSON_EXTRACT(e, '$.billablePeriod.end'), 2, 10) AS DATE) AS enrollment_end_date,
    REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') AS payer,
    CASE
        WHEN REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') = 'Medicare' THEN 'medicare'
        WHEN REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') = 'Medicaid' THEN 'medicaid'
        WHEN REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') = 'NO_INSURANCE' THEN NULL
        ELSE 'commercial'
    END AS payer_type,
    NULL AS plan,
    NULL AS original_reason_entitlement_code,
    NULL AS dual_status_code,
    NULL AS medicare_status_code,
    REPLACE(REPLACE(REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.name[0].given'), ',', -1), '"', ''), '[', ''), ']', '') AS first_name,
    REPLACE(JSON_EXTRACT(p, '$.name[0].family'), '"', '') AS last_name,
    REPLACE(JSON_EXTRACT(p, '$.address[0].line[0]'), '"', '') AS address,
    REPLACE(JSON_EXTRACT(p, '$.address[0].city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(p, '$.address[0].state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(p, '$.address[0].postalCode'), '"', '') AS zip_code,
    REPLACE(JSON_EXTRACT(p, '$.telecom[0].value'), '"', '') AS phone,
    'SyntheaFhir' AS data_source
FROM "synthea"."json"."ExplanationOfBenefit" e
LEFT JOIN "synthea"."json"."Patient" p
    ON REPLACE(REPLACE(JSON_EXTRACT(e, '$.patient.reference'), '"Patient/', ''), '"', '') = REPLACE(JSON_EXTRACT(p, '$.id'), '"', '')