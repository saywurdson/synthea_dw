-- models/observation.sql

SELECT DISTINCT
    po.procedure_occurrence_id AS observation_id,
    po.person_id,
    po.procedure_concept_id AS observation_concept_id,
    po.procedure_date AS observation_date,
    po.procedure_datetime AS observation_datetime,
    po.procedure_type_concept_id AS observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    0 AS value_as_concept_id,
    0 AS qualifier_concept_id,
    po.provider_id,
    po.visit_occurrence_id,
    po.visit_detail_id,
    po.procedure_source_value AS observation_source_value,
    po.procedure_source_concept_id AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    NULL AS value_source_value,
    po.procedure_occurrence_id AS observation_event_id,
    0 AS measurement_event_field_concept_id
FROM {{ ref('procedure_occurrence') }} po
JOIN {{ source('reference', 'concept') }} c
ON po.procedure_source_concept_id = c.concept_id
WHERE c.domain_id = 'Observation'