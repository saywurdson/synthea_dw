-- models/observation.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(ai, '$.id'), '"', '') AS observation_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(ai, '$.patient.reference'), '/', -1), '"', '') AS person_id,
    4169307 AS observation_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(ai, '$.recordedDate'), 2, 10) AS DATE) AS observation_date,
    CAST(JSON_EXTRACT(ai, '$.recordedDate') AS TIMESTAMP) AS observation_datetime,
    32817 AS observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    {{ get_standard_concept_id('concept_code', 'ai', '$.code.coding[0].code', 'SNOMED', 'Observation') }} AS value_as_concept_id,
    {{ get_standard_concept_id('concept_name', 'ai', '$.criticality', 'SNOMED', 'Meas Value') }} AS qualifier_concept_id,
    0 AS unit_concept_id,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    NULL AS observation_source_value,
    4169307 AS observation_source_concept_id,
    NULL AS unit_source_value,
    REPLACE(JSON_EXTRACT(ai, '$.criticality'), '"', '') AS qualifier_source_value,
    REPLACE(JSON_EXTRACT(ai, '$.code.coding[0].code'), '"', '') AS value_source_value,
    NULL AS observation_event_id, 
    NULL AS obs_event_field_concept_id
FROM
    {{ source('json', 'AllergyIntolerance') }} ai
WHERE
    REPLACE(JSON_EXTRACT(ai, '$.code.coding[0].code'), '"', '') != '419199007'

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(cp, '$.id'), '"', '') AS observation_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(cp, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'cp',
        '$.activity[0].detail.code.coding[0].code',
        'SNOMED',
        'Observation'
    ) }} AS observation_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(cp, '$.period.start'), 2, 10) AS DATE) AS observation_date,
    CAST(JSON_EXTRACT(cp, '$.period.start') AS TIMESTAMP) AS observation_datetime,
    32817 AS observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    0 AS value_as_concept_id,
    0 AS qualifier_concept_id,
    0 AS unit_concept_id,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(cp, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(cp, '$.activity[0].detail.code.coding[0].code'), '"', '') AS observation_source_value,
    {{ get_concept_id(
        'concept_code',
        'cp',
        '$.activity[0].detail.code.coding[0].code',
        'SNOMED',
        'Observation'
    ) }} AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    NULL AS value_source_value,
    REPLACE(JSON_EXTRACT(cp, '$.id'), '"', '') AS observation_event_id,
    NULL AS obs_event_field_concept_id
FROM {{ source('json', 'CarePlan') }} cp
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(cp, '$.subject.reference'), '/', -1), '"', '') = vo.person_id
WHERE {{ get_concept_id(
        'concept_code',
        'cp',
        '$.activity[0].detail.code.coding[0].code',
        'SNOMED',
        'Observation'
) }} IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS observation_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(c, '$.patient.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'c',
        '$.item[0].productOrService.coding[0].code',
        'SNOMED',
        'Observation'
    ) }} AS observation_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS observation_date,
    CAST(JSON_EXTRACT(c, '$.created') AS TIMESTAMP) AS observation_datetime,
    32817 AS observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    0 AS value_as_concept_id,
    0 AS qualifier_concept_id,
    0 AS unit_concept_id,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(c, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(c, '$.item[0].productOrService.coding[0].code'), '"', '') AS observation_source_value,
    {{ get_concept_id(
        'concept_code',
        'c',
        '$.item[0].productOrService.coding[0].code',
        'SNOMED',
        'Observation'
    ) }} AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    NULL AS value_source_value,
    REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS observation_event_id,
    NULL AS obs_event_field_concept_id
FROM
    {{ source('json', 'Claim') }} c
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(c, '$.patient.reference'), '/', -1), '"', '') = vo.person_id
WHERE
    (JSON_EXTRACT(c, '$.type.coding[0].code') IN ('"professional"', '"institutional"')
    OR JSON_EXTRACT(c, '$.type.coding[1].code') IN ('"professional"', '"institutional"'))
AND {{ get_concept_id(
        'concept_code',
        'c',
        '$.item[0].productOrService.coding[0].code',
        'SNOMED',
        'Observation'
) }} IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS observation_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(o, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    COALESCE(
        {{ get_standard_concept_id('concept_code', 'o', '$.component[0].code.coding[0].code', 'LOINC', 'Observation') }},
        {{ get_standard_concept_id('concept_code', 'o', '$.code.coding[0].code', 'LOINC', 'Observation') }}
    ) AS observation_concept_id,
    COALESCE(
        CAST(SUBSTRING(JSON_EXTRACT(o, '$.component[0].effectiveDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(o, '$.effectiveDateTime'), 2, 10) AS DATE)
    ) AS observation_date,
    COALESCE(
        CAST(JSON_EXTRACT(o, '$.component[0].effectiveDateTime') AS TIMESTAMP),
        CAST(JSON_EXTRACT(o, '$.effectiveDateTime') AS TIMESTAMP)
    ) AS observation_datetime,
    32817 AS observation_type_concept_id,
    COALESCE(
        CAST(JSON_EXTRACT(o, '$.component[0].valueQuantity.value') AS FLOAT),
        CAST(JSON_EXTRACT(o, '$.valueQuantity.value') AS FLOAT)
    ) AS value_as_number,
    NULL AS value_as_string,
    COALESCE(
        {{ get_standard_concept_id('concept_code', 'o', '$.component[0].valueCodeableConcept.coding[0].code', 'LOINC', 'Meas Value') }},
        {{ get_standard_concept_id('concept_code', 'o', '$.valueCodeableConcept.coding[0].code', 'LOINC', 'Meas Value') }}
    ) AS value_as_concept_id,
    0 AS qualifier_concept_id,
    COALESCE(
        {{ get_standard_concept_id('concept_code', 'o', '$.component[0].valueQuantity.code', 'UCUM', 'Unit') }},
        {{ get_standard_concept_id('concept_code', 'o', '$.valueQuantity.code', 'UCUM', 'Unit') }}
    ) AS unit_concept_id,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(o, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    COALESCE(
        REPLACE(JSON_EXTRACT(o, '$.component[0].code.coding[0].code'), '"', ''),
        REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '')
    ) AS observation_source_value,
    COALESCE(
        {{ get_concept_id('concept_code', 'o', '$.component[0].code.coding[0].code', 'LOINC', 'Observation') }},
        {{ get_concept_id('concept_code', 'o', '$.code.coding[0].code', 'LOINC', 'Observation') }}
    ) AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    NULL AS value_source_value,
    REPLACE(JSON_EXTRACT(o, '$.id'), '"', '') AS observation_event_id,
    NULL AS obs_event_field_concept_id
FROM {{ source('json', 'Observation') }} o
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(o, '$.subject.reference'), '/', -1), '"', '') = vo.person_id
WHERE COALESCE(
        {{ get_concept_id('concept_code', 'o', '$.component[0].code.coding[0].code', 'LOINC', 'Observation') }},
        {{ get_concept_id('concept_code', 'o', '$.code.coding[0].code', 'LOINC', 'Observation') }}
) IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS observation_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Observation'
    ) }} AS observation_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedPeriod.start'), 2, 10) AS DATE) AS observation_date,
    CAST(JSON_EXTRACT(p, '$.performedPeriod.start') AS TIMESTAMP) AS observation_datetime,
    32817 AS observation_type_concept_id,
    NULL AS value_as_number,
    NULL AS value_as_string,
    0 AS value_as_concept_id, 
    0 AS qualifier_concept_id,
    0 AS unit_concept_id, 
    vo.provider_id AS provider_id, 
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS observation_source_value,
    {{ get_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Observation'
    ) }} AS observation_source_concept_id,
    NULL AS unit_source_value,
    NULL AS qualifier_source_value,
    NULL AS value_source_value,
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS observation_event_id,
    NULL AS obs_event_field_concept_id
FROM {{ source('json', 'Procedure') }} p
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') = vo.person_id
WHERE JSON_EXTRACT(p, '$.code.coding[0].code') IS NOT NULL
AND {{ get_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Observation'
) }} IS NOT NULL