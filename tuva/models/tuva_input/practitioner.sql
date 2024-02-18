-- models/practitioner.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS practitioner_id,
    REPLACE(JSON_EXTRACT(p, '$.identifier[0].value'), '"', '') AS npi,
    REPLACE(JSON_EXTRACT(p, '$.name[0].given[0]'), '"', '') AS first_name,
    REPLACE(JSON_EXTRACT(p, '$.name[0].family'), '"', '') AS last_name,
    REPLACE(JSON_EXTRACT(pr, '$.location[0].display'), '"', '') AS practice_affiliation,
    REPLACE(JSON_EXTRACT(pr, '$.specialty[0].text'), '"', '') AS specialty,
    NULL AS sub_specialty,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'Practitioner') }} p
JOIN {{ source('json', 'PractitionerRole') }} pr
    ON REPLACE(JSON_EXTRACT(p, '$.identifier[0].value'), '"', '') = REPLACE(JSON_EXTRACT(pr, '$.practitioner.identifier.value'), '"', '')