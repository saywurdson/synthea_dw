-- models/measurement.sql

SELECT DISTINCT
    po.procedure_occurrence_id AS measurement_id,
    po.person_id,
    po.procedure_concept_id AS measurement_concept_id,
    po.procedure_date AS measurement_date,
    po.procedure_datetime AS measurement_datetime,
    CAST(measurement_datetime AS TIME) AS measurement_time,
    po.procedure_type_concept_id AS measurement_type_concept_id,
    4172703 AS operator_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_concept_id,
    0 AS unit_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    po.provider_id,
    po.visit_occurrence_id,
    po.visit_detail_id,
    po.procedure_source_value AS measurement_source_value,
    po.procedure_source_concept_id AS measurement_source_concept_id,
    NULL AS unit_source_value,
    0 AS unit_source_concept_id,
    NULL AS value_source_value,
    NULL AS measurement_event_id,
    0 AS measurement_event_field_concept_id
FROM {{ ref('procedure_occurrence') }} po
JOIN {{ source('reference', 'concept') }} c
ON po.procedure_source_concept_id = c.concept_id
WHERE c.domain_id = 'Measurement'
/*
UNION ALL

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS measurement_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.code.coding[0].code', 'LOINC', 'Measurement', 'Lab Test') }} AS INTEGER) AS measurement_concept_id,
    CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS DATE) AS measurement_date,
    CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS TIMESTAMP) AS measurement_datetime,
    CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS TIME) AS measurement_time,
    32817 AS measurement_type_concept_id,
    4172703 AS operator_concept_id,
    CAST(JSON_EXTRACT(data, '$.valueQuantity.value') AS FLOAT) AS value_as_number,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.valueCodeableConcept.coding[0].code', 'SNOMED', 'Observation', 'Qualifier Value') }} AS INTEGER) AS value_as_concept_id,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.valueQuantity.unit', 'UCUM', 'Unit', 'Unit') }} AS INTEGER) AS unit_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    vo.provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '') AS measurement_source_value,
    CAST({{ get_concept_id('concept_code', 'data', '$.code.coding[0].code', 'LOINC', 'Measurement') }} AS INTEGER) AS measurement_source_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.valueQuantity.unit'), '"', '') AS unit_source_value,
    CAST({{ get_concept_id('concept_code', 'data', '$.valueQuantity.unit', 'UCUM', 'Unit') }} AS INTEGER) AS unit_source_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.valueQuantity.value'), '"', '') AS value_source_value,
    NULL AS measurement_event_id,
    NULL AS measurement_event_field_concept_id
FROM {{ source('raw', 'Observation') }}
LEFT JOIN {{ ref('visit_occurrence') }} AS vo
ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') = vo.visit_occurrence_id
WHERE 
    REPLACE(JSON_EXTRACT(data, '$.category[0].coding[0].code'), '"', '')  = 'vital-signs'
    OR REPLACE(JSON_EXTRACT(data, '$.category[0].coding[0].code'), '"', '') = 'laboratory'
*/