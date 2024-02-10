-- models/drug_exposure.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(im, '$.id'), '"', '') AS drug_exposure_id,
    REPLACE(REPLACE(JSON_EXTRACT(im, '$.patient.reference'), '"Patient/', ''), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'im',
        '$.vaccineCode.coding[0].code',
        'CVX',
        'Drug'
    ) }} AS drug_concept_id,
    CAST(JSON_EXTRACT(im, '$.occurrenceDateTime') AS DATE) AS drug_exposure_start_date,
    CAST(JSON_EXTRACT(im, '$.occurrenceDateTime') AS TIMESTAMP) AS drug_exposure_start_datetime,
    CAST(JSON_EXTRACT(im, '$.occurrenceDateTime') AS DATE) AS drug_exposure_end_date,
    CAST(JSON_EXTRACT(im, '$.occurrenceDateTime') AS TIMESTAMP) AS drug_exposure_end_datetime,
    CAST(JSON_EXTRACT(im, '$.occurrenceDateTime') AS DATE) AS verbatim_end_date,
    32817 AS drug_type_concept_id,
    CASE WHEN REPLACE(JSON_EXTRACT(im, '$.status'), '"', '') = 'completed' THEN 'completed' ELSE NULL END AS stop_reason,
    0 AS refills,
    CASE
        WHEN ds.amount_value IS NOT NULL THEN ds.amount_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NULL THEN ds.numerator_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NOT NULL THEN ds.numerator_value / ds.denominator_value
        ELSE 1
    END AS quantity,
    1 AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,
    NULL AS lot_number,
    vo.provider_id AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(im, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(im, '$.vaccineCode.coding[0].code'), '"', '') AS drug_source_value,
    {{ get_concept_id(
        'concept_code',
        'im',
        '$.vaccineCode.coding[0].code',
        'CVX'
    ) }} AS drug_source_concept_id,
    NULL AS route_source_value,
    NULL AS dose_unit_source_value
FROM {{ source('json', 'Immunization') }} im
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(REPLACE(JSON_EXTRACT(im, '$.patient.reference'), '"Patient/', ''), '"', '') = vo.person_id
LEFT JOIN {{ source('vocabulary', 'drug_strength') }} ds ON {{ get_concept_id(
    'concept_code',
    'im',
    '$.vaccineCode.coding[0].code',
    'CVX'
) }} = ds.drug_concept_id
LEFT JOIN {{ source('vocabulary', 'concept') }} c ON ds.numerator_unit_concept_id = c.concept_id
WHERE {{ get_concept_id(
    'concept_code',
    'im',
    '$.vaccineCode.coding[0].code',
    'CVX'
) }} IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(ma, '$.id'), '"', '') AS drug_exposure_id,
    REPLACE(REPLACE(JSON_EXTRACT(ma, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'ma',
        '$.medicationCodeableConcept.coding[0].code',
        'RxNorm',
        'Drug'
    ) }} AS drug_concept_id,
    CAST(JSON_EXTRACT(ma, '$.effectiveDateTime') AS DATE) AS drug_exposure_start_date,
    CAST(JSON_EXTRACT(ma, '$.effectiveDateTime') AS TIMESTAMP) AS drug_exposure_start_datetime,
    CAST(JSON_EXTRACT(ma, '$.effectiveDateTime') AS DATE) AS drug_exposure_end_date,
    CAST(JSON_EXTRACT(ma, '$.effectiveDateTime') AS TIMESTAMP) AS drug_exposure_end_datetime,
    CAST(JSON_EXTRACT(ma, '$.effectiveDateTime') AS DATE) AS verbatim_end_date,
    32817 AS drug_type_concept_id,
    CASE
        WHEN REPLACE(JSON_EXTRACT(ma, '$.status'), '"', '') = 'completed' THEN 'completed'
        ELSE NULL
    END AS stop_reason,
    0 AS refills,
    CASE
        WHEN ds.amount_value IS NOT NULL THEN ds.amount_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NULL THEN ds.numerator_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NOT NULL THEN ds.numerator_value / ds.denominator_value
        ELSE 1
    END AS quantity,
    1 AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,
    NULL AS lot_number,
    vo.provider_id AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(JSON_EXTRACT(ma, '$.context'), '$.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(JSON_EXTRACT(ma, '$.medicationCodeableConcept.coding[0]'), '$.code'), '"', '') AS drug_source_value,
    {{ get_concept_id(
        'concept_code',
        'ma',
        '$.medicationCodeableConcept.coding[0].code',
        'RxNorm',
        'Drug'
    ) }} AS drug_source_concept_id,
    NULL AS route_source_value,
    NULL AS dose_unit_source_value 
FROM {{ source('json', 'MedicationAdministration') }} ma
LEFT JOIN (
    SELECT
        person_id,
        provider_id
    FROM {{ ref('visit_occurrence') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY visit_occurrence_id) = 1
) vo ON REPLACE(REPLACE(JSON_EXTRACT(ma, '$.subject.reference'), '"Patient/', ''), '"', '') = vo.person_id
LEFT JOIN {{ source('vocabulary', 'drug_strength') }} ds ON {{ get_concept_id(
    'concept_code',
    'ma',
    '$.medicationCodeableConcept.coding[0].code',
    'RxNorm',
    'Drug'
) }} = ds.drug_concept_id
LEFT JOIN {{ source('vocabulary', 'concept') }} c ON ds.numerator_unit_concept_id = c.concept_id
WHERE {{ get_concept_id(
    'concept_code',
    'ma',
    '$.medicationCodeableConcept.coding[0].code',
    'RxNorm',
    'Drug'
) }} IS NOT NULL

UNION

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(mr, '$.id'), '"', '') AS drug_exposure_id,
    REPLACE(REPLACE(JSON_EXTRACT(mr, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
    {{ get_standard_concept_id(
        'concept_code',
        'mr',
        '$.medicationCodeableConcept.coding[0].code',
        'RxNorm',
        'Drug'
    ) }} AS drug_concept_id,
    CAST(JSON_EXTRACT(mr, '$.authoredOn') AS DATE) AS drug_exposure_start_date,
    CAST(JSON_EXTRACT(mr, '$.authoredOn') AS TIMESTAMP) AS drug_exposure_start_datetime,
    (CAST(JSON_EXTRACT(mr, '$.authoredOn') AS DATE) + INTERVAL '29 days') AS drug_exposure_end_date,
    (CAST(JSON_EXTRACT(mr, '$.authoredOn') AS TIMESTAMP) + INTERVAL '29 days') AS drug_exposure_end_datetime,
    (CAST(JSON_EXTRACT(mr, '$.authoredOn') AS DATE) + INTERVAL '29 days') AS verbatim_end_date,
    32817 AS drug_type_concept_id,
    CASE
        WHEN REPLACE(JSON_EXTRACT(mr, '$.status'), '"', '') = 'stopped' THEN 'stopped'
        ELSE NULL
    END AS stop_reason,
    0 AS refills,
    CASE
        WHEN ds.amount_value IS NOT NULL THEN ds.amount_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NULL THEN ds.numerator_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NOT NULL THEN ds.numerator_value / ds.denominator_value
        ELSE 1
    END AS quantity,
    29 AS days_supply,
    NULL AS sig,
    NULL AS route_concept_id,
    NULL AS lot_number,
    REPLACE(REPLACE(JSON_EXTRACT(mr, '$.requester.reference'), '"Practitioner/', ''), '"', '') AS provider_id,
    REPLACE(REPLACE(JSON_EXTRACT(mr, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
    NULL AS visit_detail_id,
    REPLACE(JSON_EXTRACT(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0]'), '$.code'), '"', '') AS drug_source_value,
    {{ get_concept_id(
        'concept_code',
        'mr',
        '$.medicationCodeableConcept.coding[0].code',
        'RxNorm',
        'Drug'
    ) }} AS drug_source_concept_id,
    NULL AS route_source_value,
    c.concept_name AS dose_unit_source_value
FROM {{ source('json', 'MedicationRequest') }} mr
LEFT JOIN {{ source('vocabulary', 'drug_strength') }} ds ON {{ get_concept_id(
    'concept_code',
    'mr',
    '$.medicationCodeableConcept.coding[0].code',
    'RxNorm',
    'Drug'
) }} = ds.drug_concept_id
LEFT JOIN {{ source('vocabulary', 'concept') }} c ON ds.numerator_unit_concept_id = c.concept_id
WHERE {{ get_concept_id(
    'concept_code',
    'mr',
    '$.medicationCodeableConcept.coding[0].code',
    'RxNorm',
    'Drug'
) }} IS NOT NULL