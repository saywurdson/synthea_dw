-- models/medication.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(m, '$.id'), '"', '') AS medication_id,
    REPLACE(REPLACE(JSON_EXTRACT(m, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
    REPLACE(REPLACE(JSON_EXTRACT(m, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS encounter_id,
    CAST(SUBSTRING(JSON_EXTRACT(m, '$.authoredOn'), 2, 10) AS DATE) AS dispensing_date,
    CAST(SUBSTRING(JSON_EXTRACT(m, '$.authoredOn'), 2, 10) AS DATE) AS prescribing_date,
    'rxnorm' AS source_code_type,
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS source_code,
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].display'), '"', '') AS source_description,
    (
        SELECT c2.concept_code
        FROM {{ source('vocabulary', 'concept_relationship') }} cr
        JOIN {{ source('vocabulary', 'concept') }} c1 ON c1.concept_id = cr.concept_id_1
        JOIN {{ source('vocabulary', 'concept') }} c2 ON c2.concept_id = cr.concept_id_2
        WHERE c1.concept_code = REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '')
        AND cr.relationship_id = 'Mapped from'
        AND c2.vocabulary_id = 'NDC'
        AND c2.domain_id = 'Drug'
        AND c2.invalid_reason IS NULL
        AND c1.concept_class_id in ('Branded Drug', 'Clinical Drug', 'Quant Clinical Drug')
        AND LENGTH(c2.concept_code) = 11
        ORDER BY c2.concept_id
        LIMIT 1
    ) AS ndc_code,
    (
        SELECT c2.concept_name
        FROM {{ source('vocabulary', 'concept_relationship') }} cr
        JOIN {{ source('vocabulary', 'concept') }} c1 ON c1.concept_id = cr.concept_id_1
        JOIN {{ source('vocabulary', 'concept') }} c2 ON c2.concept_id = cr.concept_id_2
        WHERE c1.concept_code = REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '')
        AND cr.relationship_id = 'Mapped from'
        AND c2.vocabulary_id = 'NDC'
        AND c2.domain_id = 'Drug'
        AND c2.invalid_reason IS NULL
        AND c1.concept_class_id in ('Branded Drug', 'Clinical Drug', 'Quant Clinical Drug')
        AND LENGTH(c2.concept_code) = 11
        ORDER BY c2.concept_id
        LIMIT 1
    ) AS ndc_description,
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS rxnorm_code,
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].display'), '"', '') AS rxnorm_description,
    r."atc class id" AS atc_code,
    r.rxstring AS atc_description,
    NULL AS route,
    CASE
        WHEN ds.amount_value IS NOT NULL THEN ds.amount_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NULL THEN ds.numerator_value
        WHEN ds.amount_value IS NULL AND ds.numerator_value IS NOT NULL AND ds.denominator_value IS NOT NULL THEN ds.numerator_value / ds.denominator_value
        ELSE 1
    END AS strength,
    CASE
        WHEN REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
        THEN CAST(REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') AS INTEGER) * 30
        ELSE 1
    END AS quantity,
    NULL AS quantity_unit,
    CASE
        WHEN REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
        THEN 30
        ELSE 1
    END AS days_supply,
    REPLACE(REPLACE(JSON_EXTRACT(m, '$.requester.reference'), '"Practitioner/', ''), '"', '') AS practitioner_id,
    'SyntheaFhir' AS data_source
FROM {{ source('json', 'MedicationRequest') }} m
LEFT JOIN {{ source('vocabulary', 'concept') }} c
    ON REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') = c.concept_code
    AND c.vocabulary_id = 'RxNorm'
    AND c.domain_id = 'Drug'
    AND c.invalid_reason IS NULL
    AND c.standard_concept = 'S'
JOIN {{ source('reference', 'rxcuis_ndcs_atc') }} r
    ON REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') = r.rxcui
JOIN {{ source('vocabulary', 'drug_strength') }} ds
    ON c.concept_id = ds.drug_concept_id
WHERE 
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') IS NOT NULL