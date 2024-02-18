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
        FROM "synthea"."vocabulary"."concept_relationship" cr
        JOIN "synthea"."vocabulary"."concept" c1 ON c1.concept_id = cr.concept_id_1
        JOIN "synthea"."vocabulary"."concept" c2 ON c2.concept_id = cr.concept_id_2
        WHERE c1.concept_code = REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '')
        AND cr.relationship_id = 'Mapped from'
        AND c2.vocabulary_id = 'NDC'
        AND c2.domain_id = 'Drug'
        AND c2.invalid_reason IS NULL
        AND c1.concept_class_id in ('Branded Drug', 'Clinical Drug', 'Quant Clinical Drug', 'Quant Branded Drug')
        AND LENGTH(c2.concept_code) = 11
        ORDER BY c2.concept_id
        LIMIT 1
    ) AS ndc_code,
    (
        SELECT c2.concept_name
        FROM "synthea"."vocabulary"."concept_relationship" cr
        JOIN "synthea"."vocabulary"."concept" c1 ON c1.concept_id = cr.concept_id_1
        JOIN "synthea"."vocabulary"."concept" c2 ON c2.concept_id = cr.concept_id_2
        WHERE c1.concept_code = REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '')
        AND cr.relationship_id = 'Mapped from'
        AND c2.vocabulary_id = 'NDC'
        AND c2.domain_id = 'Drug'
        AND c2.invalid_reason IS NULL
        AND c1.concept_class_id in ('Branded Drug', 'Clinical Drug', 'Quant Clinical Drug', 'Quant Branded Drug')
        AND LENGTH(c2.concept_code) = 11
        ORDER BY c2.concept_id
        LIMIT 1
    ) AS ndc_description,
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS rxnorm_code,
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].display'), '"', '') AS rxnorm_description,
    r."atc class id" AS atc_code,
    (
        SELECT c3.concept_name
        FROM "synthea"."vocabulary"."concept" c3
        WHERE c3.concept_code = atc_code
            AND c3.vocabulary_id = 'ATC'
            AND c3.domain_id = 'Drug'
            AND c3.invalid_reason IS NULL
            AND c3.standard_concept = 'C'
        LIMIT 1
    ) AS atc_description,
    NULL AS route,
    CASE
        WHEN ds.amount_value IS NOT NULL THEN ds.amount_value
        WHEN ds.numerator_value IS NOT NULL THEN
            CASE
                WHEN ds.denominator_value IS NOT NULL AND ds.denominator_value != 0 THEN ds.numerator_value / ds.denominator_value
                ELSE ds.numerator_value
            END
        ELSE NULL
    END AS strength,
    CASE
        WHEN REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
        THEN CAST(REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') AS INTEGER) * 30
        ELSE 1
    END AS quantity,
    (
        SELECT c4.concept_name
        FROM "synthea"."vocabulary"."concept" c1
        JOIN "synthea"."vocabulary"."drug_strength" ds ON c1.concept_id = ds.drug_concept_id
        JOIN "synthea"."vocabulary"."concept" c4 ON c4.concept_id = COALESCE(ds.amount_unit_concept_id, ds.numerator_unit_concept_id)
        WHERE c1.concept_code = REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '')
            AND c1.vocabulary_id = 'RxNorm'
            AND c1.domain_id = 'Drug'
            AND c1.invalid_reason IS NULL
            AND c1.standard_concept = 'S'
            AND c4.concept_id IS NOT NULL
        LIMIT 1
    ) AS quantity_unit,
    CASE
        WHEN REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
        THEN 30
        ELSE 1
    END AS days_supply,
    REPLACE(REPLACE(JSON_EXTRACT(m, '$.requester.reference'), '"Practitioner/', ''), '"', '') AS practitioner_id,
    'SyntheaFhir' AS data_source
FROM "synthea"."json"."MedicationRequest" m
LEFT JOIN "synthea"."vocabulary"."concept" c
    ON REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') = c.concept_code
    AND c.vocabulary_id = 'RxNorm'
    AND c.domain_id = 'Drug'
    AND c.invalid_reason IS NULL
    AND c.standard_concept = 'S'
JOIN "synthea"."reference"."rxcuis_ndcs_atc" r
    ON REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') = r.rxcui
JOIN "synthea"."vocabulary"."drug_strength" ds
    ON c.concept_id = ds.drug_concept_id
WHERE 
    REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') IS NOT NULL