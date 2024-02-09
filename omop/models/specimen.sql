-- models/specimen.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS specimen_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(p, '$.subject.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Specimen'
    ) }} AS specimen_concept_id,
    32817 AS specimen_type_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedPeriod.start'), 2, 10) AS DATE) AS specimen_date,
    CAST(JSON_EXTRACT(p, '$.performedPeriod.start') AS TIMESTAMP) AS specimen_datetime,
    1 AS quantity,
    0 AS unit_concept_id,
    0 AS anatomic_site_concept_id,
    0 AS disease_status_concept_id,
    {{ get_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Specimen'
    ) }} AS specimen_source_id,
    REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS specimen_source_value,
    NULL AS unit_source_value,
    NULL AS anatomic_site_source_value,
    NULL AS disease_status_source_value
FROM
    {{ source('json', 'Procedure') }} p
WHERE {{ get_concept_id(
        'concept_code',
        'p',
        '$.code.coding[0].code',
        'SNOMED',
        'Specimen'
) }} IS NOT NULL