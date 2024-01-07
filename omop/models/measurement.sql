-- models/measurement.sql

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY REPLACE(JSON_EXTRACT(data, '$.id'), '"', '')) AS measurement_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '')
        AND (c.vocabulary_id = 'SNOMED')
        AND standard_concept = 'S'
        AND invalid_reason IS NULL
        AND (c.domain_id = 'Measurement')
        AND (c.concept_class_id = 'Procedure' OR c.concept_class_id = 'Observable Entity')
    ), 0) measurement_concept_id,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS DATE) AS measurement_date,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS TIMESTAMP) AS measurement_date,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS TIME) AS measurement_time,
    32817 AS measurement_type_concept_id,
    4172703 AS operator_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_concept_id,
    NULL AS unit_concept_id,
    NULL AS range_low,
    NULL AS range_high,
    vo.provider_id AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '') AS measurement_source_value,
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '')
        AND (c.vocabulary_id = 'SNOMED')
        AND (c.domain_id = 'Measurement')
        AND (c.concept_class_id = 'Procedure' OR c.concept_class_id = 'Observable Entity')
    ), 0) AS measurement_source_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.valueQuantity.unit'), '"', '') AS unit_source_value,
    NULL AS unit_source_concept_id,
    NULL AS value_source_value,
    NULL AS measurement_event_id,
    NULL AS measurement_event_field_concept_id
FROM {{ source('raw', 'Procedure') }}
LEFT JOIN {{ ref('visit_occurrence') }} AS vo
ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') = vo.visit_occurrence_id
WHERE measurement_source_concept_id != 0