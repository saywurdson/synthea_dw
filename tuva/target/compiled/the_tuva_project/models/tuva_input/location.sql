-- models/location.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(l, '$.id'), '"', '') AS location_id,
    NULL AS npi,
    REPLACE(JSON_EXTRACT(l, '$.name'), '"', '') AS name,
    NULL AS facility_type,
    REPLACE(JSON_EXTRACT(l, '$.managingOrganization.display'), '"', '') AS parent_organization,
    REPLACE(REPLACE(REPLACE(JSON_EXTRACT(l, '$.address.line'), '"', ''), '[', ''), ']', '') AS address,
    REPLACE(JSON_EXTRACT(l, '$.address.city'), '"', '') AS city,
    REPLACE(JSON_EXTRACT(l, '$.address.state'), '"', '') AS state,
    REPLACE(JSON_EXTRACT(l, '$.address.postalCode'), '"', '') AS zip_code,
    CAST(REPLACE(JSON_EXTRACT(l, '$.position.latitude'), '"', '') AS FLOAT) AS latitude,
    CAST(REPLACE(JSON_EXTRACT(l, '$.position.longitude'), '"', '') AS FLOAT) AS longitude,
    'SyntheaFhir' AS data_source
FROM "synthea"."json"."Location" l