-- models/encounter.sql

SELECT DISTINCT
    MAX(REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')) AS encounter_id,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(e, '$.subject.reference'), '"Patient/', ''), '"', '')) AS patient_id,
    MAX(CASE
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'AMB' THEN 'outpatient'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'EMER' THEN 'emergency department'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'IMP' THEN 'acute inpatient'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'FLD' THEN 'other'
        WHEN REPLACE(JSON_EXTRACT(e, '$.class.code'), '"', '') = 'HH' THEN 'home health'
        ELSE 'other'
    END) AS encounter_type,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE)) AS encounter_start_date,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.end'), 2, 10) AS DATE)) AS encounter_end_date,
    MAX(CASE
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
    END) AS length_of_stay,
    9 AS admit_source_code,
    'Information Not Available' AS admit_source_description,
    9 AS admit_type_code,
    'Unknown' AS admit_type_description,
    00 AS discharge_disposition_code,
    'Unknown Value (but present in data)' AS discharge_disposition_description,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '')) AS attending_provider_id,
    NULL AS facility_npi,
    'snomed' AS primary_diagnosis_code_type,
    MAX(REPLACE(JSON_EXTRACT(e, '$.reasonCode[0].coding[0].code'), '"', '')) AS primary_diagnosis_code,
    MAX(REPLACE(JSON_EXTRACT(e, '$.reasonCode[0].coding[0].display'), '"', '')) AS primary_diagnosis_description,
    MAX(icd."MS-DRG") AS ms_drg_code,
    MAX(icd."MS-DRG_description") AS ms_drg_description,
    MAX(apr.apr_drg_code) AS apr_drg_code,
    MAX(apr.apr_drg_description) AS apr_drg_description,
    MAX(REPLACE(JSON_EXTRACT(ex, '$.payment.amount.value'), '"', '')) AS paid_amount,
    MAX(REPLACE(JSON_EXTRACT(ex, '$.total[0].amount.value'), '"', '')) AS allowed_amount,
    MAX(REPLACE(JSON_EXTRACT(ex, '$.total[0].amount.value'), '"', '')) AS charge_amount,
    'SyntheaFhir' AS data_source
FROM "synthea"."json"."Encounter" e
LEFT JOIN "synthea"."json"."ExplanationOfBenefit" ex
    ON REPLACE(JSON_EXTRACT(e, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(ex, '$.item[0].encounter[0].reference'), '"Encounter/', ''), '"', '')
JOIN "synthea"."terminology"."snomed_icd_10_map" sno
    ON REPLACE(JSON_EXTRACT(e, '$.reasonCode[0].coding[0].code'), '"', '') = sno.referenced_component_id
JOIN "synthea"."reference"."icd10cm_to_msdrg_v41" icd
    ON sno.map_target = REPLACE(icd.ICD10, '.', '')
JOIN "synthea"."terminology"."apr_drg" apr
    ON icd.MDC = apr.mdc_code
GROUP BY REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')