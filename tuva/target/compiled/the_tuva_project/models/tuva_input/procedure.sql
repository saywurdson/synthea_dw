-- models/procedure.sql

SELECT
    procedure_id,
    patient_id,
    encounter_id,
    claim_id,
    procedure_date,
    procedure_code,
    source_code_type,
    source_code,
    source_description,
    normalized_code_type,
    normalized_code,
    normalized_description,
    modifier_1,
    modifier_2,
    modifier_3,
    modifier_4,
    modifier_5,
    practitioner_id,
    data_source
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') AS procedure_id,
        REPLACE(REPLACE(JSON_EXTRACT(p, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
        REPLACE(REPLACE(JSON_EXTRACT(p, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS encounter_id,
        REPLACE(JSON_EXTRACT(cl, '$.id'), '"', '') AS claim_id,
        CAST(REPLACE(JSON_EXTRACT(p, '$.performedPeriod.start'), '"', '') AS DATE) AS procedure_date,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code,
        'snomed' AS source_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS source_code,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].display'), '"', '') AS source_description,
        'snomed' AS normalized_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS normalized_code,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].display'), '"', '') AS normalized_description,
        NULL AS modifier_1,
        NULL AS modifier_2,
        NULL AS modifier_3,
        NULL AS modifier_4,
        NULL AS modifier_5,
        REPLACE(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '') AS practitioner_id,
        'SyntheaFhir' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') ORDER BY REPLACE(JSON_EXTRACT(p, '$.id'), '"', '')) AS rn
    FROM "synthea"."json"."Procedure" p
    LEFT JOIN "synthea"."json"."Encounter" e
        ON REPLACE(REPLACE(JSON_EXTRACT(p, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')
    LEFT JOIN "synthea"."json"."Claim" cl
        ON REPLACE(REPLACE(JSON_EXTRACT(p, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(cl, '$.item[0].encounter[0].reference'), '"Encounter/', ''), '"', '')
) AS procedures
WHERE rn = 1