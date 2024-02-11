-- models/medical_claim.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
    REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') AS claim_line_number,
    REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
    REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
    NULL AS member_id,
    REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') AS payer,
    NULL AS plan,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
        THEN CAST(SUBSTRING(JSON_EXTRACT(p, '$.billablePeriod.start'), 2, 10) AS DATE)
        ELSE NULL
    END AS admission_date,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
        THEN CAST(SUBSTRING(JSON_EXTRACT(p, '$.billablePeriod.end'), 2, 10) AS DATE)
        ELSE NULL
    END AS discharge_date,
    3 AS admit_source_code,
    9 AS admit_type_code,
    NULL AS discharge_disposition_code,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
        THEN REPLACE(JSON_EXTRACT(e, '$.item.locationCodeableConcept.coding[0].code'), '"', '')
        ELSE NULL
    END AS place_of_service_code,
    NULL AS bill_type_code,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
    CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
    CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS allowed_amount,
    CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS coinsurance_amount,
    CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS copayment_amount,
    NULL AS deductible_amount,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Claim') }} c
WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'