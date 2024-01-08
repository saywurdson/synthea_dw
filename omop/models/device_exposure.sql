-- models/device_exposure.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS device_exposure_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.patient.reference'), '"Patient/', ''), '"', '') AS person_id,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.type.coding[0].code', 'SNOMED', 'Device', 'Physical Object') }} AS INTEGER) AS device_concept_id,
    NULL AS device_exposure_start_date,
    NULL AS device_exposure_start_datetime,
    NULL AS device_exposure_end_date,
    NULL AS device_exposure_end_datetime,
    32817 AS device_type_concept_id,
    REPLACE(JSON_EXTRACT(data, '$.udiCarrier[0].deviceIdentifier'), '"', '') AS unique_device_id,
    1 AS quantity,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.type.coding[0].code'), '"', '') AS device_source_value,
    CAST({{ get_concept_id('concept_code', 'data', '$.type.coding[0].code', 'SNOMED', 'Device', 'Physical Object') }} AS INTEGER) AS device_source_concept_id,
    NULL AS unit_concept_id,
    NULL AS unit_source_value,
    NULL AS unit_source_concept_id
FROM {{ source('raw', 'Device') }}
WHERE  
    device_exposure_id IS NOT NULL
    AND person_id IS NOT NULL
    AND device_concept_id IS NOT NULL

UNION 

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS device_exposure_id,
    REPLACE(REPLACE(JSON_EXTRACT(data, '$.patient.reference'), '"Patient/', ''), '"', '') AS person_id,
    CAST({{ get_standard_concept_id('concept_code', 'data', '$.suppliedItem.itemCodeableConcept.coding[0].code', 'SNOMED', 'Device', 'Physical Object') }} AS INTEGER) AS device_concept_id,
    CAST(JSON_EXTRACT(data, '$.occurrenceDateTime') AS DATE) AS device_exposure_start_date,
    CAST(JSON_EXTRACT(data, '$.occurrenceDateTime') AS TIMESTAMP) AS device_exposure_start_datetime,
    NULL AS device_exposure_end_date,
    NULL AS device_exposure_end_datetime,
    32817 AS device_type_concept_id,
    NULL AS unique_device_id,
    JSON_EXTRACT(data, '$.suppliedItem.quantity.value') AS quantity,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(data, '$.suppliedItem.itemCodeableConcept.coding[0].code'), '"', '') AS device_source_value,
    CAST({{ get_concept_id('concept_code', 'data', '$.suppliedItem.itemCodeableConcept.coding[0].code', 'SNOMED', 'Device', 'Physical Object') }} AS INTEGER) AS device_source_concept_id,
    NULL AS unit_concept_id,
    NULL AS unit_source_value,
    NULL AS unit_source_concept_id
FROM {{ source('raw', 'SupplyDelivery') }}
WHERE
    device_exposure_id IS NOT NULL
    AND person_id IS NOT NULL
    AND device_concept_id IS NOT NULL
    AND device_exposure_start_date IS NOT NULL