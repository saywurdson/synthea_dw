-- models/note.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(cp, '$.id'), '"', '') AS note_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(cp, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    CAST(SUBSTRING(JSON_EXTRACT(cp, '$.period.start'), 2, 10) AS DATE) AS note_date,
    CAST(JSON_EXTRACT(cp, '$.period.start') AS TIMESTAMP) AS note_datetime,
    32817 AS note_type_concept_id,
    706300 AS note_class_concept_id,
    SUBSTRING(
        JSON_EXTRACT(cp, '$.text.div'),
        POSITION('>' IN JSON_EXTRACT(cp, '$.text.div')) + 1,
        POSITION('<br/>' IN JSON_EXTRACT(cp, '$.text.div')) - POSITION('>' IN JSON_EXTRACT(cp, '$.text.div')) - 1
    ) AS note_title,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSTRING(JSON_EXTRACT(cp, '$.text.div'), POSITION('<br/>' IN JSON_EXTRACT(cp, '$.text.div')) + 5), '<ul>', ''), '<li>', ''), '</li>', ''),  '<br/>', ''),  '</div>', '') AS note_text,
    32678 AS encoding_concept_id,
    4175745 AS language_concept_id,
    vo.provider_id AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(cp, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    NULL AS note_source_value,
    NULL AS note_event_id,
    NULL AS note_event_field_concept_id
FROM
    {{ source('json', 'CarePlan') }} cp
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(SPLIT_PART(JSON_EXTRACT(cp, '$.subject.reference'), '/', -1), '"', '') = vo.person_id
WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSTRING(JSON_EXTRACT(cp, '$.text.div'), POSITION('<br/>' IN JSON_EXTRACT(cp, '$.text.div')) + 5), '<ul>', ''), '<li>', ''), '</li>', ''),  '<br/>', ''),  '</div>', '') IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(dr, '$.id'), '"', '') AS note_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(dr, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    CAST(SUBSTRING(JSON_EXTRACT(dr, '$.issued'), 2, 10) AS DATE) AS note_date,
    CAST(JSON_EXTRACT(dr, '$.issued') AS TIMESTAMP) AS note_datetime,
    32817 AS note_type_concept_id,
    42868493 AS note_class_concept_id,
    REPLACE(JSON_EXTRACT(dr, '$.code.coding[0].display'), '"', '') AS note_title,
    REPLACE(JSON_EXTRACT(dr, '$.presentedForm[0].data'), '"', '') AS note_text,
    32678 AS encoding_concept_id,
    4175745 AS language_concept_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(dr, '$.performer[0].reference'), '/', -1), '"', '') AS provider_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(dr, '$.encounter.reference'), '/', -1), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    NULL AS note_source_value,
    NULL AS note_event_id,
    NULL AS note_event_field_concept_id
FROM
    {{ source('json', 'DiagnosticReport') }} dr
WHERE REPLACE(JSON_EXTRACT(dr, '$.presentedForm[0].data'), '"', '') IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(dref, '$.id'), '"', '') AS note_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(dref, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    CAST(SUBSTRING(JSON_EXTRACT(dref, '$.date'), 2, 10) AS DATE) AS note_date,
    CAST(JSON_EXTRACT(dref, '$.date') AS TIMESTAMP) AS note_datetime,
    32817 AS note_type_concept_id,
    {{ get_concept_id(
        'concept_code',
        'dref',
        '$.type.coding[0].code',
        'LOINC',
        'Note'
    ) }} AS note_class_concept_id,
    REPLACE(JSON_EXTRACT(dref, '$.category[0].coding[0].display'), '"', '') AS note_title,
    REPLACE(JSON_EXTRACT(dref, '$.content[0].attachment.data'), '"', '') AS note_text,
    32678 AS encoding_concept_id,
    4175745 AS language_concept_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(dref, '$.author[0].reference'), '/', -1), '"', '') AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    NULL AS note_source_value,
    NULL AS note_event_id,
    NULL AS note_event_field_concept_id
FROM
    {{ source('json', 'DocumentReference') }} dref