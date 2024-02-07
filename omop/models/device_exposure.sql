-- models/device_exposure.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(d, '$.id'), '"', '') AS device_exposure_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(d, '$.patient.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'd',
        '$.type.coding[0].code',
        'SNOMED',
        'Device'
    ) }} AS device_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(d, '$.manufactureDate'), 2, 10) AS DATE) AS device_exposure_start_date,
    CAST(JSON_EXTRACT(d, '$.manufactureDate') AS TIMESTAMP) AS device_exposure_start_datetime,
    NULL AS device_exposure_end_date,
    NULL AS device_exposure_end_datetime,
    32817 AS device_type_concept_id,
    REPLACE(JSON_EXTRACT(d, '$.distinctIdentifier'), '"', '') AS unique_device_id,
    REPLACE(JSON_EXTRACT(d, '$.udiCarrier[0].carrierHRF'), '"', '') AS production_id,
    1 AS quantity,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(d, '$.type.coding[0].code'), '"', '') AS device_source_value,
    {{ get_concept_id(
        'concept_code',
        'd',
        '$.type.coding[0].code',
        'SNOMED',
        'Device'
    ) }} AS device_source_concept_id,
    NULL AS unit_concept_id,
    NULL AS unit_source_value,
    NULL AS unit_source_concept_id
FROM {{ source('json', 'Device') }} d
WHERE {{ get_concept_id(
        'concept_code',
        'd',
        '$.type.coding[0].code',
        'SNOMED',
        'Device'
) }} IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(sd, '$.id'), '"', '') AS device_exposure_id,
    REPLACE(SPLIT_PART(JSON_EXTRACT(sd, '$.patient.reference'), '/', -1), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'sd',
        '$.suppliedItem.itemCodeableConcept.coding[0].code',
        'SNOMED',
        'Device'
    ) }} AS device_concept_id,
    CAST(SUBSTRING(JSON_EXTRACT(sd, '$.occurrenceDateTime'), 2, 10) AS DATE) AS device_exposure_start_date,
    CAST(JSON_EXTRACT(sd, '$.occurrenceDateTime') AS TIMESTAMP) AS device_exposure_start_datetime,
    CAST(SUBSTRING(JSON_EXTRACT(sd, '$.occurrenceDateTime'), 2, 10) AS DATE) AS device_exposure_end_date,
    CAST(JSON_EXTRACT(sd, '$.occurrenceDateTime') AS TIMESTAMP) AS device_exposure_end_datetime,
    32817 AS device_type_concept_id,
    NULL AS unique_device_id,
    NULL AS production_id,
    CAST(JSON_EXTRACT(sd, '$.suppliedItem.quantity.value') AS INTEGER) AS quantity,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(sd, '$.suppliedItem.itemCodeableConcept.coding[0].code'), '"', '') AS device_source_value,
    {{ get_concept_id(
        'concept_code',
        'sd',
        '$.suppliedItem.itemCodeableConcept.coding[0].code',
        'SNOMED',
        'Device'
    ) }} AS device_source_concept_id,
    NULL AS unit_concept_id,
    NULL AS unit_source_value,
    NULL AS unit_source_concept_id
FROM {{ source('json', 'SupplyDelivery') }} sd
WHERE {{ get_concept_id(
        'concept_code',
        'sd',
        '$.suppliedItem.itemCodeableConcept.coding[0].code',
        'SNOMED',
        'Device'
) }} IS NOT NULL