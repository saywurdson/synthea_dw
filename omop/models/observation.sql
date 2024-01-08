-- models/observation.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS observation_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '')
        AND (c.vocabulary_id = 'SNOMED')
        AND standard_concept = 'S'
        AND invalid_reason IS NULL
        AND (c.domain_id = 'Observation')
        AND (c.concept_class_id = 'Procedure' OR c.concept_class_id = 'Observable Entity')
    ), 0) observation_concept_id,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS DATE) AS observation_date,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS TIMESTAMP) AS observation_datetime,
    32817 AS observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    NULL AS value_as_concept_id,
    0 AS qualifier_concept_id,
    0 AS unit_concept_id,
    vo.provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '') AS observation_source_value,
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '')
        AND (c.vocabulary_id = 'SNOMED')
        AND (c.domain_id = 'Observation')
        AND (c.concept_class_id = 'Procedure' OR c.concept_class_id = 'Observable Entity')
    ), 0) AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    NULL AS value_source_value,
    NULL AS observation_event_id,
    0 AS observation_event_field_concept_id
FROM {{ source('raw', 'Procedure') }}
LEFT JOIN {{ ref('visit_occurrence') }} AS vo
ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') = vo.encounter_source_value
WHERE 
    observation_source_concept_id != 0
    AND observation_id IS NOT NULL
    AND person_id IS NOT NULL
    AND observation_date IS NOT NULL