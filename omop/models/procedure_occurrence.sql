-- models/procedure_occurrence.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(cp, '$.id'), '"', '') AS procedure_occurrence_id,
    REPLACE(REPLACE(JSON_EXTRACT(cp, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    c_procedure.concept_id AS procedure_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(cp, '$.period.start'), 2, 10) AS DATE) AS procedure_date,
    CAST(JSON_EXTRACT(cp, '$.period.start') AS TIMESTAMP) AS procedure_datetime,
    CAST(SUBSTRING(JSON_EXTRACT(cp, '$.period.end'), 2, 10) AS DATE) AS procedure_end_date,
    CAST(JSON_EXTRACT(cp, '$.period.end') AS TIMESTAMP) AS procedure_end_datetime,
    32817 AS procedure_type_concept_id,
    NULL AS modifier_concept_id,
    1 AS quantity,
    vo.person_id AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(cp, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    CASE
        WHEN JSON_EXTRACT(cp, '$.category[0].coding[0].display') IS NOT NULL
            THEN REPLACE(JSON_EXTRACT(cp, '$.category[0].coding[0].code'), '"', '')
        WHEN JSON_EXTRACT(cp, '$.category[1].coding[0].display') IS NOT NULL
            THEN REPLACE(JSON_EXTRACT(cp, '$.category[1].coding[0].code'), '"', '')
        ELSE NULL
    END AS procedure_source_value,
    c_source.concept_id AS procedure_source_concept_id,
    NULL AS modifier_source_value
FROM
    {{ source('json', 'CarePlan') }} cp
LEFT JOIN {{ source('reference', 'concept') }} c_procedure ON c_procedure.concept_code = (
    CASE
        WHEN JSON_EXTRACT(cp, '$.category[0].coding[0].display') IS NOT NULL
            THEN REPLACE(JSON_EXTRACT(cp, '$.category[0].coding[0].code'), '"', '')
        WHEN JSON_EXTRACT(cp, '$.category[1].coding[0].display') IS NOT NULL
            THEN REPLACE(JSON_EXTRACT(cp, '$.category[1].coding[0].code'), '"', '')
        ELSE NULL
    END
) AND c_procedure.vocabulary_id = 'SNOMED' AND c_procedure.domain_id = 'Procedure'
LEFT JOIN {{ source('reference', 'concept') }} c_source ON c_source.concept_code = (
    CASE
        WHEN JSON_EXTRACT(cp, '$.category[0].coding[0].display') IS NOT NULL
            THEN REPLACE(JSON_EXTRACT(cp, '$.category[0].coding[0].code'), '"', '')
        WHEN JSON_EXTRACT(cp, '$.category[1].coding[0].display') IS NOT NULL
            THEN REPLACE(JSON_EXTRACT(cp, '$.category[1].coding[0].code'), '"', '')
        ELSE NULL
    END
) AND c_source.vocabulary_id = 'SNOMED' AND c_source.domain_id = 'Procedure'
LEFT JOIN (
    SELECT
        person_id,
        provider_id,
        visit_occurrence_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(REPLACE(JSON_EXTRACT(cp, '$.subject.reference'), '"Patient/', ''), '"', '') = vo.person_id
WHERE c_source.concept_id IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(ims, '$.id'), '"', '') AS procedure_occurrence_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(ims, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'ims',
        '$.procedureCode[0].coding[0].code',
        'SNOMED',
        'Procedure'
    ) }} AS procedure_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(ims, '$.started'), 2, 10) AS DATE) AS procedure_date,
    CAST(JSON_EXTRACT(ims, '$.started') AS TIMESTAMP) AS procedure_datetime,
    NULL AS procedure_end_date,
    NULL AS procedure_end_datetime,
    32817 AS procedure_type_concept_id,
    {{ get_concept_id(
        'concept_code',
        'ims',
        '$.series[0].bodySite.code',
        'SNOMED',
        'Spec Anatomic Site'
    ) }} AS modifier_concept_id,
    CAST(JSON_EXTRACT(ims, '$.numberOfInstances') AS INTEGER) AS quantity,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(ims, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(ims, '$.procedureCode[0].coding[0].code'), '"', '') AS procedure_source_value,
    {{ get_concept_id(
        'concept_code',
        'ims',
        '$.procedureCode[0].coding[0].code',
        'SNOMED',
        'Procedure'
    ) }} AS procedure_source_concept_id,
    REPLACE(JSON_EXTRACT(ims, '$.series[0].bodySite.code'), '"', '') AS modifier_source_value
FROM {{ source('json', 'ImagingStudy') }} ims
LEFT JOIN (
    SELECT
        person_id,
        provider_id,
        visit_occurrence_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(ims, '$.subject.reference'), '/', -1), '"', '') = vo.person_id

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS procedure_occurrence_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    c_procedure.concept_id AS procedure_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedPeriod.start'), 2, 10) AS DATE) AS procedure_date,
    CAST(JSON_EXTRACT(p, '$.performedPeriod.start') AS TIMESTAMP) AS procedure_datetime,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedPeriod.end'), 2, 10) AS DATE) AS procedure_end_date,
    CAST(JSON_EXTRACT(p, '$.performedPeriod.end') AS TIMESTAMP) AS procedure_end_datetime,
    32817 AS procedure_type_concept_id,
    0 AS modifier_concept_id,
    1 AS quantity,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_source_value,
    c_procedure.concept_id AS procedure_source_concept_id,
    NULL AS modifier_source_value
FROM {{ source('json', 'Procedure') }} p
INNER JOIN {{ source('reference', 'concept') }} c_procedure
    ON c_procedure.concept_code = REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '')
    AND c_procedure.vocabulary_id = 'SNOMED'
    AND c_procedure.domain_id = 'Procedure'
LEFT JOIN (
    SELECT
        person_id,
        provider_id,
        visit_occurrence_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') = vo.person_id