-- models/procedure_occurrence.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS procedure_occurrence_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '')
        AND (c.vocabulary_id = 'SNOMED')
        AND standard_concept = 'S'
        AND invalid_reason IS NULL
        AND (c.domain_id = 'Procedure')
        AND (c.concept_class_id = 'Procedure' OR c.concept_class_id = 'Observable Entity')
    ), 0) procedure_concept_id,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS DATE) AS procedure_date,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.start') AS TIMESTAMP) AS procedure_datetime,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.end') AS DATE) AS procedure_end_date,
    CAST(JSON_EXTRACT(data, '$.performedPeriod.end') AS TIMESTAMP) AS procedure_end_datetime,
    32817 AS procedure_type_concept_id,
    NULL AS modifier_concept_id,
    1 AS quantity,
    vo.provider_id AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '') AS procedure_source_value,
    COALESCE((
        SELECT c.concept_id
        FROM {{ source('reference', 'concept') }} c
        WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.code.coding[0].code'), '"', '')
        AND (c.vocabulary_id = 'SNOMED')
        AND (c.domain_id = 'Procedure')
        AND (c.concept_class_id = 'Procedure' OR c.concept_class_id = 'Observable Entity')
    ), 0) AS procedure_source_concept_id,
    NULL AS modifier_source_value
FROM {{ source('raw', 'Procedure') }}
LEFT JOIN {{ ref('visit_occurrence') }} AS vo
ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') = vo.visit_occurrence_id
WHERE 
    procedure_source_concept_id != 0
    AND procedure_occurrence_id IS NOT NULL
    AND person_id IS NOT NULL
    AND procedure_date IS NOT NULL

UNION ALL

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS procedure_occurrence_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.procedureCode[0].coding[0].code', 'SNOMED', 'Procedure', 'Procedure') }} AS INTEGER) AS procedure_concept_id,
    CAST(JSON_EXTRACT(data, '$.started') AS DATE) AS procedure_date,
    CAST(JSON_EXTRACT(data, '$.started') AS TIMESTAMP) AS procedure_datetime,
    NULL AS procedure_end_date,
    NULL AS procedure_end_datetime,
    32817 AS procedure_type_concept_id,
    NULL AS modifier_concept_id,
    JSON_EXTRACT(data, '$.numberOfInstances') AS quantity,
    vo.provider_id AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.procedureCode[0].coding[0].code'), '"', '') AS procedure_source_value,
    CAST({{ get_concept_id('concept_code', 'data', '$.procedureCode[0].coding[0].code', 'SNOMED', 'Procedure', 'Procedure') }} AS INTEGER) AS procedure_source_concept_id,
    NULL AS modifier_source_value
FROM {{ source('raw', 'ImagingStudy') }}
LEFT JOIN {{ ref('visit_occurrence') }} AS vo
ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') = vo.visit_occurrence_id
WHERE 
    procedure_occurrence_id IS NOT NULL
    AND person_id IS NOT NULL
    AND procedure_date IS NOT NULL