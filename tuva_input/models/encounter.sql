-- models/encounter.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(e, '$.id'), '"', '') AS encounter_id,
    REPLACE(REPLACE(JSON_EXTRACT(e, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
    CASE
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'AMB' THEN 'outpatient'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'EMER' THEN 'emergency department'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'IMP' THEN 'acute inpatient'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'FLD' THEN 'other'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'HH' THEN 'home health'
        ELSE 'other'
    END AS encounter_type,
    CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) AS encounter_start_date,
    CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.end'), 2, 10) AS DATE) AS encounter_end_date,
    CASE
        WHEN 
            CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) IS NOT NULL AND 
            CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.end'), 2, 10) AS DATE) IS NOT NULL AND
            CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) = CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.end'), 2, 10) AS DATE)
        THEN 1
        WHEN 
            CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) IS NOT NULL AND 
            CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.end'), 2, 10) AS DATE) IS NOT NULL 
        THEN
            DATEDIFF(
                'day',
                CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE),
                CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.end'), 2, 10) AS DATE)
            ) + 1
        ELSE 1
    END AS length_of_stay,
    9 AS admit_source_code,
    'Information Not Available' AS admit_source_description,
    9 AS admit_type_code,
    'Unknown' AS admit_type_description,
    00 AS discharge_disposition_code,
    'Unknown Value (but present in data)' AS discharge_disposition_description,
    REPLACE(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '') AS attending_provider_id,
    NULL AS facility_npi,
    'snomed' AS primary_diagnosis_code_type,
    REPLACE(JSON_EXTRACT(e, '$.reasonCode[0].coding[0].code'), '"', '') AS primary_diagnosis_code,
    REPLACE(JSON_EXTRACT(e, '$.reasonCode[0].coding[0].display'), '"', '') AS primary_diagnosis_description,
    NULL AS ms_drg_code,
    NULL AS ms_drg_description,
    NULL AS apr_drug_code,
    NULL AS apr_drug_description,
    REPLACE(JSON_EXTRACT(ex, '$.payment.amount.value'), '"', '') AS paid_amount,
    REPLACE(JSON_EXTRACT(ex, '$.total[0].amount.value'), '"', '') AS allowed_amount,
    REPLACE(JSON_EXTRACT(ex, '$.total[0].amount.value'), '"', '') AS charge_amount,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Encounter') }} e
LEFT JOIN {{ source('json', 'ExplanationOfBenefit') }} ex
    ON REPLACE(JSON_EXTRACT(e, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(ex, '$.item[0].encounter[0].reference'), '"Encounter/', ''), '"', '')
    