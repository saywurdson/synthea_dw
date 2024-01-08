-- models/provider.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '') AS provider_id,
    REPLACE(JSON_EXTRACT(pr.data, '$.practitioner.display'), '"', '') AS provider_name,
    REPLACE(JSON_EXTRACT(pr.data, '$.practitioner.identifier.value'), '"', '') AS npi,
    NULL AS dea,
    {{ get_concept_id('concept_code', 'pr.data', '$.specialty[0].coding[0].code', 'NUCC', 'Provider') }} AS specialty_concept_id,
    REPLACE(JSON_EXTRACT(pr.data, '$.organization.identifier.value'), '"', '') AS care_site_id,
    NULL AS year_of_birth,
    CASE
        WHEN REPLACE(JSON_EXTRACT(p.data, '$.gender'), '"', '') = 'male' THEN 8507
        WHEN REPLACE(JSON_EXTRACT(p.data, '$.gender'), '"', '') = 'female' THEN 8532
        ELSE 0
    END AS gender_concept_id,
    REPLACE(JSON_EXTRACT(p.data, '$.id'), '"', '') AS provider_source_value,
    REPLACE(JSON_EXTRACT(pr.data, '$.specialty[0].coding[0].code'), '"', '') AS specialty_source_value,
    {{ get_concept_id('concept_code', 'pr.data', '$.specialty[0].coding[0].code', 'NUCC', 'Provider') }} AS specialty_source_concept_id,
    REPLACE(JSON_EXTRACT(p.data, '$.gender'), '"', '') AS gender_source_value,
    CASE
        WHEN REPLACE(JSON_EXTRACT(p.data, '$.gender'), '"', '') = 'male' THEN 8507
        WHEN REPLACE(JSON_EXTRACT(p.data, '$.gender'), '"', '') = 'female' THEN 8532
        ELSE 0
    END AS gender_source_concept_id
FROM {{ source('raw', 'PractitionerRole') }} pr
LEFT JOIN {{ source('raw', 'Practitioner') }} p
ON REPLACE(JSON_EXTRACT(pr.data, '$.practitioner.identifier.value'), '"', '') = REPLACE(JSON_EXTRACT(p.data, '$.identifier[0].value'), '"', '')