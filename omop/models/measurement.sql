-- models/measurement.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS measurement_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(o, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'o',
        '$.code.coding[0].code',
        'LOINC',
        'Measurement'
    ) }} AS measurement_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(o, '$.effectiveDateTime'), 2, 10) AS DATE) AS measurement_date,
    CAST(JSON_EXTRACT(o, '$.effectiveDateTime') AS TIMESTAMP) AS measurement_datetime,
    SUBSTRING(JSON_EXTRACT(o, '$.effectiveDateTime'), 13, 8) AS measurement_time,
    32817 AS measurement_type_concept_id,
    4172703 AS operator_concept_id,
    CAST(JSON_EXTRACT(o, '$.valueQuantity.value') AS FLOAT) AS value_as_number,
    0 AS value_as_concept_id,
    {{ get_standard_concept_id(
        'concept_code',
        'o',
        '$.valueQuantity.code',
        'UCUM',
        'Unit'
    ) }} AS unit_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(o, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '') AS measurement_source_value,
    {{ get_concept_id(
        'concept_code',
        'o',
        '$.code.coding[0].code',
        'LOINC',
        'Measurement'
    ) }} AS measurement_source_concept_id,
    REPLACE(JSON_EXTRACT(o, '$.valueQuantity.code'), '"', '') AS unit_source_value,
    {{ get_concept_id(
        'concept_code',
        'o',
        '$.valueQuantity.code',
        'UCUM',
        'Unit'
    ) }} AS unit_source_concept_id,
    REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') AS value_source_value,
    NULL AS measurement_event_id,
    NULL AS meas_event_field_concept_id
FROM {{ source('json', 'Observation') }} o
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(o, '$.subject.reference'), '/', -1), '"', '') = vo.person_id
WHERE {{ get_concept_id(
        'concept_code',
        'o',
        '$.code.coding[0].code',
        'LOINC',
        'Measurement'
) }} IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS measurement_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Measurement'
    ) }} AS measurement_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedPeriod.start'), 2, 10) AS DATE) AS measurement_date,
    CAST(JSON_EXTRACT(p, '$.performedPeriod.start') AS TIMESTAMP) AS measurement_datetime,
    CAST(CAST(JSON_EXTRACT(p, '$.performedPeriod.start') AS TIMESTAMP) AS TIME) AS measurement_time,
    32817 AS measurement_type_concept_id,
    4172703 AS operator_concept_id,
    NULL AS value_as_number,
    0 AS value_as_concept_id,
    0 AS unit_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS measurement_source_value,
    {{ get_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Measurement'
    ) }} AS measurement_source_concept_id,
    NULL AS unit_source_value,
    0 AS unit_source_concept_id,
    NULL AS value_source_value,
    NULL AS measurement_event_id,
    NULL AS meas_event_field_concept_id 
FROM {{ source('json', 'Procedure') }} p
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') = vo.person_id
WHERE {{ get_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Measurement'
) }} IS NOT NULL