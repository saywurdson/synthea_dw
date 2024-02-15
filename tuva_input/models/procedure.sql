-- models/procedure.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS procedure_id,
    REPLACE(REPLACE(JSON_EXTRACT(p, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
    REPLACE(REPLACE(JSON_EXTRACT(p, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS encounter_id,
    REPLACE(JSON_EXTRACT(cl, '$.id'), '"', '') AS claim_id,
    CAST(REPLACE(JSON_EXTRACT(p, '$.performedPeriod.start'), '"', '') AS DATE) AS procedure_date,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code,
    'snomed' AS source_code_type,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS source_code,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].display'), '"', '') AS source_description,
    'snomed' AS normalized_code_type,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS normalized_code,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].display'), '"', '') AS normalized_description,
    NULL AS modifier_1,
    NULL AS modifier_2,
    NULL AS modifier_3,
    NULL AS modifier_4,
    NULL AS modifier_5,
    REPLACE(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '') AS practitioner_id,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Procedure') }} p
LEFT JOIN {{ source('json', 'Encounter') }} e
    ON REPLACE(REPLACE(JSON_EXTRACT(p, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')
LEFT JOIN {{ source('json', 'Claim') }} cl
    ON REPLACE(REPLACE(JSON_EXTRACT(p, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(cl, '$.item[0].encounter[0].reference'), '"Encounter/', ''), '"', '')