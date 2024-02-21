-- models/eligibility.sql

SELECT DISTINCT
    MAX(REPLACE(REPLACE(JSON_EXTRACT(e, '$.patient.reference'), '"Patient/', ''), '"', '')) AS patient_id,
    NULL AS member_id,
    MAX(REPLACE(JSON_EXTRACT(p, '$.extension[3].valueCode'), '"', '')) AS gender,
    MAX(REPLACE(JSON_EXTRACT(p, '$.extension[0].extension[1].valueString'), '"', '')) AS race,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(p, '$.birthDate'), 2, 10) AS DATE)) AS birth_date,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(p, '$.deceasedDateTime'), 2, 10) AS DATE)) AS death_date,
    MAX(CASE
        WHEN JSON_EXTRACT(p, '$.deceasedDateTime') IS NOT NULL THEN 1
        ELSE 0
    END) AS death_flag,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(e, '$.billablePeriod.start'), 2, 10) AS DATE)) AS enrollment_start_date,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(e, '$.billablePeriod.end'), 2, 10) AS DATE)) AS enrollment_end_date,
    MAX(REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '')) AS payer,
    MAX(CASE
        WHEN REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') = 'Medicare' THEN 'medicare'
        WHEN REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') = 'Medicaid' THEN 'medicaid'
        WHEN REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') = 'NO_INSURANCE' THEN NULL
        ELSE 'commercial'
    END) AS payer_type,
    NULL AS plan,
    NULL AS original_reason_entitlement_code,
    NULL AS dual_status_code,
    NULL AS medicare_status_code,
    MAX(REPLACE(REPLACE(REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.name[0].given'), ',', -1), '"', ''), '[', ''), ']', '')) AS first_name,
    MAX(REPLACE(JSON_EXTRACT(p, '$.name[0].family'), '"', '')) AS last_name,
    MAX(REPLACE(JSON_EXTRACT(p, '$.address[0].line[0]'), '"', '')) AS address,
    MAX(REPLACE(JSON_EXTRACT(p, '$.address[0].city'), '"', '')) AS city,
    MAX(REPLACE(JSON_EXTRACT(p, '$.address[0].state'), '"', '')) AS state,
    MAX(REPLACE(JSON_EXTRACT(p, '$.address[0].postalCode'), '"', '')) AS zip_code,
    MAX(REPLACE(JSON_EXTRACT(p, '$.telecom[0].value'), '"', '')) AS phone,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'ExplanationOfBenefit') }} e
LEFT JOIN {{ source('json', 'Patient') }} p
    ON REPLACE(REPLACE(JSON_EXTRACT(e, '$.patient.reference'), '"Patient/', ''), '"', '') = REPLACE(JSON_EXTRACT(p, '$.id'), '"', '')
GROUP BY REPLACE(REPLACE(JSON_EXTRACT(e, '$.patient.reference'), '"Patient/', ''), '"', '')