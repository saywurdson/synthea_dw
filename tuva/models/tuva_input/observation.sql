-- models/observation.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS observation_id,
    REPLACE(REPLACE(JSON_EXTRACT(o, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
    REPLACE(REPLACE(JSON_EXTRACT(o, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS encounter_id,
    NULL AS panel_id,
    CAST(SUBSTRING(JSON_EXTRACT(o, '$.effectiveDateTime'), 2, 10) AS DATE) AS observation_date,
    REPLACE(JSON_EXTRACT(o, '$.category[0].display'), '"', '') AS observation_type,
    'loinc' AS source_code_type,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '') AS source_code,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].display'), '"', '') AS source_description,
    'loinc' AS normalized_code_type,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '') AS normalized_code,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].display'), '"', '') AS normalized_description,
    CASE
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NOT NULL THEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '')
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NULL AND REPLACE(JSON_EXTRACT(o, '$.component'), '"', '') IS NOT NULL THEN REPLACE(JSON_EXTRACT(o, '$.component[0].code.coding[0].code'), '"', '')
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NULL AND REPLACE(JSON_EXTRACT(o, '$.component'), '"', '') IS NULL THEN REPLACE(JSON_EXTRACT(o, '$.valueCodeableConcept.coding[0].display'), '"', '')
    END AS result,
    CASE
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NOT NULL THEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.unit'), '"', '')
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NULL AND REPLACE(JSON_EXTRACT(o, '$.component'), '"', '') IS NOT NULL THEN REPLACE(JSON_EXTRACT(o, '$.component[0].valueQuantity.unit'), '"', '')
    END AS source_units,
    CASE
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NOT NULL THEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.unit'), '"', '')
        WHEN REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NULL AND REPLACE(JSON_EXTRACT(o, '$.component'), '"', '') IS NOT NULL THEN REPLACE(JSON_EXTRACT(o, '$.component[0].valueQuantity.unit'), '"', '')
    END AS normalized_units,
    NULL AS source_reference_range_low,
    NULL AS source_reference_range_high,
    NULL AS normalized_reference_range_low,
    NULL AS normalized_reference_range_high,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Observation') }} o