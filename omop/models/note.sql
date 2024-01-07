-- models/note.sql

SELECT 
    ROW_NUMBER() OVER (ORDER BY note_id) AS note_id,
    person_id,
    note_date,
    note_datetime,
    note_type_concept_id,
    note_title,
    note_text,
    encoding_concept_id,
    language_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    note_source_value,
    note_event_id,
    note_event_field_concept_id
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS note_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
        CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS DATE) AS note_date,
        CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS TIMESTAMP) AS note_datetime,
        706391 AS note_type_concept_id,
        REPLACE(JSON_EXTRACT(data, '$.category[0].coding[0].display'), '"', '') AS note_title,
        REPLACE(JSON_EXTRACT(data, '$.presentedForm[0].data'), '"', '') AS note_text,
        32678 AS encoding_concept_id,
        4180061 AS language_concept_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.performer[0].reference'), '"Practitioner/', ''), '"', '') AS provider_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
        NULL AS visit_detail_id,
        NULL AS note_source_value,
        NULL AS note_event_id,
        NULL AS note_event_field_concept_id
    FROM {{ source('raw', 'DiagnosticReport') }}
    WHERE note_title = 'History and physical note'

    UNION

    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS note_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
        CAST(JSON_EXTRACT(data, '$.date') AS DATE) AS note_date,
        CAST(JSON_EXTRACT(data, '$.date') AS TIMESTAMP) AS note_datetime,
        706391 AS note_type_concept_id,
        REPLACE(JSON_EXTRACT(data, '$.category[0].coding[0].display'), '"', '') AS note_title,
        REPLACE(JSON_EXTRACT(data, '$.content[0].attachment.data'), '"', '') AS note_text,
        32678 AS encoding_concept_id,
        4180061 AS language_concept_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.author[0].reference'), '"Practitioner/', ''), '"', '') AS provider_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.context.encounter[0].reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
        NULL AS visit_detail_id,
        NULL AS note_source_value,
        NULL AS note_event_id,
        NULL AS note_event_field_concept_id
    FROM {{ source('raw', 'DocumentReference') }}
) AS note