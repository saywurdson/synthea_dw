-- models/medical_claim.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
    REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') AS claim_line_number,
    REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
    REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
    NULL AS member_id,
    REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', '') AS payer,
    NULL AS plan,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
        THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
        ELSE NULL
    END AS admission_date,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
        THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
        ELSE NULL
    END AS discharge_date,
    '3' AS admit_source_code,
    9 AS admit_type_code,
    CASE
        WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
        ELSE 1
    END AS discharge_disposition_code,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
        THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
        ELSE NULL
    END AS place_of_service_code,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
        ELSE NULL
    END AS bill_type_code,
    msdrg."MS-DRG" AS ms_drg_code,
    aprdrg.apr_drg_code AS apr_drg_code,
    CASE
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
        WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
        ELSE NULL
    END AS revenue_center_code,
    NULL AS service_unit_quantity,
    NULL AS hcpcs_code,
    NULL AS hcpcs_modifier_1,
    NULL AS hcpcs_modifier_2,
    NULL AS hcpcs_modifier_3,
    NULL AS hcpcs_modifier_4,
    NULL AS hcpcs_modifier_5,
    REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', '') AS rendering_npi,
    NULL AS billing_npi,
    NULL AS facility_npi,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
    CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
    NULL AS allowed_amount,
    NULL AS charge_amount,
    ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)) AS coinsurance_amount,
    NULL AS copayment_amount,
    NULL AS deductible_amount,
    CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS total_cost_amount,
    'snomed' AS diagnosis_code_type,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') AS diagnosis_code_1,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[1].code'), '"', '') AS diagnosis_code_2,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[2].code'), '"', '') AS diagnosis_code_3,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[3].code'), '"', '') AS diagnosis_code_4,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[4].code'), '"', '') AS diagnosis_code_5,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[5].code'), '"', '') AS diagnosis_code_6,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[6].code'), '"', '') AS diagnosis_code_7,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[7].code'), '"', '') AS diagnosis_code_8,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[8].code'), '"', '') AS diagnosis_code_9,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[9].code'), '"', '') AS diagnosis_code_10,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[10].code'), '"', '') AS diagnosis_code_11,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[11].code'), '"', '') AS diagnosis_code_12,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[12].code'), '"', '') AS diagnosis_code_13,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[13].code'), '"', '') AS diagnosis_code_14,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[14].code'), '"', '') AS diagnosis_code_15,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[15].code'), '"', '') AS diagnosis_code_16,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[16].code'), '"', '') AS diagnosis_code_17,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[17].code'), '"', '') AS diagnosis_code_18,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[18].code'), '"', '') AS diagnosis_code_19,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[19].code'), '"', '') AS diagnosis_code_20,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[20].code'), '"', '') AS diagnosis_code_21,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[21].code'), '"', '') AS diagnosis_code_22,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[22].code'), '"', '') AS diagnosis_code_23,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[23].code'), '"', '') AS diagnosis_code_24,
    REPLACE(JSON_EXTRACT(co, '$.code.coding[24].code'), '"', '') AS diagnosis_code_25,
    'Y' AS diagnosis_poa_1,
    'U' AS diagnosis_poa_2,
    'U' AS diagnosis_poa_3,
    'U' AS diagnosis_poa_4,
    'U' AS diagnosis_poa_5,
    'U' AS diagnosis_poa_6,
    'U' AS diagnosis_poa_7,
    'U' AS diagnosis_poa_8,
    'U' AS diagnosis_poa_9,
    'U' AS diagnosis_poa_10,
    'U' AS diagnosis_poa_11,
    'U' AS diagnosis_poa_12,
    'U' AS diagnosis_poa_13,
    'U' AS diagnosis_poa_14,
    'U' AS diagnosis_poa_15,
    'U' AS diagnosis_poa_16,
    'U' AS diagnosis_poa_17,
    'U' AS diagnosis_poa_18,
    'U' AS diagnosis_poa_19,
    'U' AS diagnosis_poa_20,
    'U' AS diagnosis_poa_21,
    'U' AS diagnosis_poa_22,
    'U' AS diagnosis_poa_23,
    'U' AS diagnosis_poa_24,
    'U' AS diagnosis_poa_25,
    'snomed' AS procedure_code_type,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code_1,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', '') AS procedure_code_2,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', '') AS procedure_code_3,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', '') AS procedure_code_4,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', '') AS procedure_code_5,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', '') AS procedure_code_6,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', '') AS procedure_code_7,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', '') AS procedure_code_8,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', '') AS procedure_code_9,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', '') AS procedure_code_10,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', '') AS procedure_code_11,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', '') AS procedure_code_12,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', '') AS procedure_code_13,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', '') AS procedure_code_14,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', '') AS procedure_code_15,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', '') AS procedure_code_16,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', '') AS procedure_code_17,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', '') AS procedure_code_18,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', '') AS procedure_code_19,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', '') AS procedure_code_20,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', '') AS procedure_code_21,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', '') AS procedure_code_22,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', '') AS procedure_code_23,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', '') AS procedure_code_24,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', '') AS procedure_code_25,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_1,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_2,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_3,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_4,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_5,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_6,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_7,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_8,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_9,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_10,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_11,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_12,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_13,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_14,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_15,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_16,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_17,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_18,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_19,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_20,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_21,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_22,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_23,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_24,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_25,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Claim') }} c
LEFT JOIN {{ source('json', 'ExplanationOfBenefit') }} e ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
LEFT JOIN {{ source('json', 'Condition') }} co ON REPLACE(JSON_EXTRACT(co, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.diagnosis[0].diagnosisReference.reference'), '"Condition/', ''), '"', '')
LEFT JOIN {{ source('json', 'Procedure') }} p ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.procedure[0].procedureReference.reference'), '"Procedure/', ''), '"', '')
LEFT JOIN {{ source('terminology', 'snomed_icd_10_map') }} map ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
LEFT JOIN {{ source('reference', 'icd10cm_to_msdrg_v41') }} msdrg ON REPLACE(msdrg.ICD10, '.', '') = map.map_target
LEFT JOIN {{ source('terminology', 'apr_drg') }} aprdrg ON aprdrg.mdc_code = msdrg.MDC
LEFT JOIN {{ source('json', 'Practitioner') }} pr ON REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.provider.reference'), '"Practitioner/', ''), '"', '')
LEFT JOIN {{ source('json', 'Patient') }} pat ON REPLACE(JSON_EXTRACT(pat, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '')
WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'