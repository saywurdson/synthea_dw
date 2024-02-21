
  
    
    

    create  table
      "synthea"."tuva_input"."lab_result__dbt_tmp"
  
    as (
      -- models/lab_result.sql

SELECT DISTINCT
    MAX(REPLACE(JSON_EXTRACT(o, '$.id'), '"', '')) AS lab_result_id,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(o, '$.subject.reference'), '"Patient/', ''), '"', '')) AS patient_id,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(o, '$.encounter.reference'), '"Encounter/', ''), '"', '')) AS encounter_id,
    NULL AS accession_number,
    'loinc' AS source_code_type,
    MAX(REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '')) AS source_code,
    MAX(REPLACE(JSON_EXTRACT(o, '$.code.coding[0].display'), '"', '')) AS source_description,
    NULL AS source_component,
    'loinc' AS normalized_code_type,
    MAX(l.loinc) AS normalized_code,
    MAX(l.short_name) AS normalized_description,
    MAX(l.component) AS normalized_component,
    MAX(REPLACE(JSON_EXTRACT(o, '$.status'), '"', '')) AS status,
    MAX(REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '')) AS result, 
    MAX(CAST(SUBSTRING(JSON_EXTRACT(o, '$.effectiveDateTime'), 2, 10) AS DATE)) AS result_date,
    MAX(CAST(SUBSTRING(JSON_EXTRACT(o, '$.issued'), 2, 10) AS DATE)) AS collection_date,
    MAX(REPLACE(JSON_EXTRACT(o, '$.valueQuantity.unit'), '"', '')) AS source_units,
    MAX(REPLACE(JSON_EXTRACT(o, '$.valueQuantity.unit'), '"', '')) AS normalized_units,
    NULL AS source_reference_range_low,
    NULL AS source_reference_range_high,
    NULL AS normalized_reference_range_low,
    NULL AS normalized_reference_range_high,
    NULL AS source_abnormal_flag,
    NULL AS normalized_abnormal_flag,
    NULL AS specimen,
    MAX(REPLACE(REPLACE(JSON_EXTRACT(e, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '')) AS ordering_practitioner_id,
    'SyntheaFhir' AS data_source
FROM "synthea"."json"."Observation" o
LEFT JOIN "synthea"."terminology"."loinc" l ON REPLACE(JSON_EXTRACT(o, '$.code.coding[0].code'), '"', '') = l.loinc
LEFT JOIN "synthea"."json"."Encounter" e ON REPLACE(REPLACE(JSON_EXTRACT(o, '$.encounter.reference'), '"Encounter/', ''), '"', '') = REPLACE(JSON_EXTRACT(e, '$.id'), '"', '')
WHERE REPLACE(JSON_EXTRACT(o, '$.category[0].coding[0].code'), '"', '') = ('laboratory')
AND REPLACE(JSON_EXTRACT(o, '$.valueQuantity.value'), '"', '') IS NOT NULL
GROUP BY REPLACE(JSON_EXTRACT(o, '$.id'), '"', '')
    );
  
  