-- models/provider.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS provider_id,
    REPLACE(JSON_EXTRACT(pr, '$.practitioner.display'), '"', '') AS provider_name,
    REPLACE(JSON_EXTRACT(p, '$.identifier[0].value'), '"', '') AS npi,
    NULL AS dea,
    {{ get_concept_id('concept_code', 'pr', '$.specialty[0].coding[0].code', 'NUCC', 'Provider') }} AS specialty_concept_id,
    REPLACE(JSON_EXTRACT(pr, '$.organization.identifier.value'), '"', '') AS care_site_id,
    NULL AS year_of_birth,
    CASE 
        WHEN REPLACE(JSON_EXTRACT(p, '$.gender'), '"', '') = 'male' THEN 8507
        WHEN REPLACE(JSON_EXTRACT(p, '$.gender'), '"', '') = 'female' THEN 8532
        ELSE 0
    END AS gender_concept_id,
    REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') AS provider_source_value,
    REPLACE(JSON_EXTRACT(pr, '$.specialty[0].coding[0].code'), '"', '') AS specialty_source_value,
    {{ get_concept_id('concept_code', 'pr', '$.specialty[0].coding[0].code', 'NUCC', 'Provider') }} AS specialty_source_concept_id,
    REPLACE(JSON_EXTRACT(p, '$.gender'), '"', '') AS gender_source_value,
    CASE 
        WHEN REPLACE(JSON_EXTRACT(p, '$.gender'), '"', '') = 'male' THEN 8507
        WHEN REPLACE(JSON_EXTRACT(p, '$.gender'), '"', '') = 'female' THEN 8532
        ELSE 0
    END AS gender_source_concept_id
FROM {{ source('json', 'PractitionerRole') }} pr
LEFT JOIN {{ source('json', 'Practitioner') }} p
    ON REPLACE(JSON_EXTRACT(pr, '$.practitioner.identifier.value'), '"', '') = REPLACE(JSON_EXTRACT(p, '$.identifier[0].value'), '"', '')