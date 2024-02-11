-- models/lab_result.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS condition_id,
    REPLACE(REPLACE(JSON_EXTRACT(o, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
    REPLACE(REPLACE(JSON_EXTRACT(o, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS encounter_id,
    NULL AS accession_number,
    'loinc' AS source_code_type,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '') AS source_code,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].display'), '"', '') AS source_description,
    'loinc' AS normalized_code_type,
    l.loinc AS normalized_code,
    l.short_name AS normalized_description,
    l.component AS normalized_component,
    REPLACE(JSON_EXTRACT(o, '$.status'), '"', '') AS status,
    REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') AS result, 
    CAST(SUBSTRING(JSON_EXTRACT(o, '$.effectiveDateTime'), 2, 10) AS DATE) AS result_date,
    CAST(SUBSTRING(JSON_EXTRACT(o, '$.issued'), 2, 10) AS DATE) AS collection_date,
    REPLACE(JSON_EXTRACT(o, '$.valueQuantity.unit'), '"', '') AS source_units,
    REPLACE(JSON_EXTRACT(o, '$.valueQuantity.unit'), '"', '') AS normalized_units,
    NULL AS source_reference_range_low,
    NULL AS source_reference_range_high,
    NULL AS normalized_reference_range_low,
    NULL AS normalized_reference_range_high,
    NULL AS source_abnormal_flag,
    NULL AS normalized_abnormal_flag,
    NULL AS specimen,
    REPLACE(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '') AS ordering_practitioner_id,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Observation') }} o
LEFT JOIN {{ source('terminology', 'loinc') }} l ON REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '') = l.loinc
LEFT JOIN {{ source('json', 'Encounter') }} e ON REPLACE(REPLACE(JSON_EXTRACT(o, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')
WHERE REPLACE(JSON_EXTRACT(o, '$.category[0].coding[0].code'), '"', '') = ('laboratory')
AND REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NOT NULL