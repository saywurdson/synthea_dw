-- models/pharmacy_claim.sql

SELECT DISTINCT
    REPLACE(REPLACE(JSON_EXTRACT(c, '$.id'), '"Patient/', ''), '"', '') AS claim_id,
    REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') AS claim_line_number,
    REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
    NULL AS member_id,
    REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') AS payer,
    NULL AS prescribing_provider_npi,
    NULL AS dispensing_provider_npi,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.created'), 2, 10) AS DATE) AS dispensing_date,
    NULL AS plan,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Claim') }} c
LEFT JOIN {{ source('json', 'Practitioner') }} pr
    ON REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') = REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '')
WHERE REPLACE(JSON_EXTRACT(c, '$.type.code'), '"', '') = 'pharmacy'