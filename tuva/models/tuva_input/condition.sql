SELECT
    MAX(REPLACE(JSON_EXTRACT(co, '$.id'), '"', '')) AS condition_id,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(co, '$.subject.reference'), '"Patient/', ''), '"', '')) AS patient_id,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(co, '$.encounter.reference'), '"Encounter/', ''), '"', '')) AS encounter_id,
    MAX(REPLACE(JSON_EXTRACT(cl, '$.id'), '"', '')) AS claim_id,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(co, '$.recordedDate'), 2, 10) AS DATE)) AS recorded_date,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(co, '$.onsetDateTime'), 2, 10) AS DATE)) AS onset_date,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(co, '$.abatementDateTime'), 2, 10) AS DATE)) AS resolved_date,
    MAX(REPLACE(JSON_EXTRACT(co, '$.clinicalStatus.coding[0].code'), '"', '')) AS status,
    MAX(REPLACE(JSON_EXTRACT(co, '$.category[0].coding[0].display'), '"', '')) AS condition_type,
    'snomed' AS source_code_type,
    MAX(REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '')) AS source_code,
    MAX(REPLACE(JSON_EXTRACT(co, '$.code.coding[0].display'), '"', '')) AS source_description,
    'icd-10-cm' AS normalized_code_type,
    MAX(map.map_target) AS normalized_code,
    MAX(map.map_target_name) AS normalized_description,
    MAX(CASE
        WHEN REPLACE(JSON_EXTRACT(cl, '$.diagnosis[0].sequence'), '"', '') IS NOT NULL THEN CAST(REPLACE(JSON_EXTRACT(cl, '$.diagnosis[0].sequence'), '"', '') AS INTEGER)
        ELSE 1
    END) AS condition_rank,
    MAX(CASE
        WHEN CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) < CAST(SUBSTRING(JSON_EXTRACT(co, '$.onsetDateTime'), 2, 10) AS DATE) THEN 'Y'
        WHEN CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) > CAST(SUBSTRING(JSON_EXTRACT(co, '$.onsetDateTime'), 2, 10) AS DATE) THEN 'N'
        ELSE 'U'
    END) AS present_on_admit_code,
    MAX(CASE
        WHEN CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) < CAST(SUBSTRING(JSON_EXTRACT(co, '$.onsetDateTime'), 2, 10) AS DATE) THEN 'Diagnosis was present at the time of inpatient admission. CMS will pay the CC/MCC DRG for those selected HACs that are coded as Y for the POA Indicator.'
        WHEN CAST(SUBSTRING(JSON_EXTRACT(e, '$.period.start'), 2, 10) AS DATE) > CAST(SUBSTRING(JSON_EXTRACT(co, '$.onsetDateTime'), 2, 10) AS DATE) THEN 'Diagnosis was not present at the time of inpatient admission. CMS will not pay the CC/MCC DRG for those selected HACs that are coded as N for the POA Indicator.'
        ELSE 'Documentation is insufficient to determine if the condition was present at the time of inpatient admission. CMS will not pay the CC/MCC DRG for those selected HACs that are coded as U for the POA Indicator.'
    END) AS present_on_admit_description,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Condition') }} co
LEFT JOIN {{ source('json', 'Claim') }} cl
    ON REPLACE(REPLACE(JSON_EXTRACT(co, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(cl, '$.item[0].encounter[0].reference'), '"Encounter/', ''), '"', '')
LEFT JOIN {{ source('json', 'Encounter') }} e
    ON REPLACE(REPLACE(JSON_EXTRACT(co, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')
JOIN {{ source('terminology', 'snomed_icd_10_map') }} map
    ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
GROUP BY REPLACE(JSON_EXTRACT(co, '$.id'), '"', '')