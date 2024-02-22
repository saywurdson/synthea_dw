
  
    
    

    create  table
      "synthea"."tuva_input"."medication__dbt_tmp"
  
    as (
      -- models/medication.sql

WITH MinStrength AS (
    SELECT
        REPLACE(JSON_EXTRACT(m, '$.id'), '"', '') AS medication_id,
        MIN(
            CASE
                WHEN ds.amount_value IS NOT NULL THEN ds.amount_value
                WHEN ds.numerator_value IS NOT NULL THEN
                    CASE
                        WHEN ds.denominator_value IS NOT NULL AND ds.denominator_value != 0 THEN ds.numerator_value / ds.denominator_value
                        ELSE ds.numerator_value
                    END
                ELSE NULL
            END
        ) AS min_strength
    FROM "synthea"."json"."MedicationRequest" m
    JOIN "synthea"."vocabulary"."concept" c
        ON REPLACE(JSON_EXTRACT(m, '$.medicationCodeableConcept.coding[0].code'), '"', '') = c.concept_code
        AND c.vocabulary_id = 'RxNorm'
        AND c.domain_id = 'Drug'
        AND c.invalid_reason IS NULL
        AND c.standard_concept = 'S'
    JOIN "synthea"."vocabulary"."drug_strength" ds
        ON c.concept_id = ds.drug_concept_id
    GROUP BY medication_id
)

SELECT DISTINCT
    ms.medication_id,
    REPLACE(REPLACE(JSON_EXTRACT(mr, '$.subject.reference'), '"Patient/', ''), '"', '') AS patient_id,
    REPLACE(REPLACE(JSON_EXTRACT(mr, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS encounter_id,
    CAST(SUBSTRING(JSON_EXTRACT(mr, '$.authoredOn'), 2, 10) AS DATE) AS dispensing_date,
    CAST(SUBSTRING(JSON_EXTRACT(mr, '$.authoredOn'), 2, 10) AS DATE) AS prescribing_date,
    'rxnorm' AS source_code_type,
    REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS source_code,
    REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].display'), '"', '') AS source_description,
    ndc.ndc_code,
    ndc.ndc_description,
    REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS rxnorm_code,
    REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].display'), '"', '') AS rxnorm_description,
    r."atc class id" AS atc_code,
    atc.atc_description,
    NULL AS route,
    ms.min_strength AS strength,
    CASE
        WHEN REPLACE(JSON_EXTRACT(mr, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
        THEN CAST(REPLACE(JSON_EXTRACT(mr, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') AS INTEGER) * 30
        ELSE 1
    END AS quantity,
    qu.quantity_unit,
    CASE
        WHEN REPLACE(JSON_EXTRACT(mr, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
        THEN 30
        ELSE 1
    END AS days_supply,
    REPLACE(REPLACE(JSON_EXTRACT(mr, '$.requester.reference'), '"Practitioner/', ''), '"', '') AS practitioner_id,
    'SyntheaFhir' AS data_source
FROM MinStrength ms
JOIN "synthea"."json"."MedicationRequest" mr ON ms.medication_id = REPLACE(JSON_EXTRACT(mr, '$.id'), '"', '')
LEFT JOIN (
    SELECT 
        c1.concept_code AS rxnorm_code,
        c2.concept_code AS ndc_code,
        c2.concept_name AS ndc_description
    FROM "synthea"."vocabulary"."concept_relationship" cr
    JOIN "synthea"."vocabulary"."concept" c1 ON c1.concept_id = cr.concept_id_1
    JOIN "synthea"."vocabulary"."concept" c2 ON c2.concept_id = cr.concept_id_2
    WHERE cr.relationship_id = 'Mapped from'
    AND c2.vocabulary_id = 'NDC'
    AND c2.domain_id = 'Drug'
    AND c2.invalid_reason IS NULL
    AND c1.concept_class_id in ('Branded Drug', 'Clinical Drug', 'Quant Clinical Drug', 'Quant Branded Drug')
    AND LENGTH(c2.concept_code) = 11
) ndc ON REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].code'), '"', '') = ndc.rxnorm_code
LEFT JOIN "synthea"."reference"."rxcuis_ndcs_atc" r ON REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].code'), '"', '') = r.rxcui
LEFT JOIN (
    SELECT 
        c3.concept_code AS atc_code,
        c3.concept_name AS atc_description
    FROM "synthea"."vocabulary"."concept" c3
    WHERE c3.vocabulary_id = 'ATC'
    AND c3.domain_id = 'Drug'
    AND c3.invalid_reason IS NULL
    AND c3.standard_concept = 'C'
) atc ON r."atc class id" = atc.atc_code
LEFT JOIN (
    SELECT 
        ds.drug_concept_id,
        c4.concept_name AS quantity_unit
    FROM "synthea"."vocabulary"."drug_strength" ds
    JOIN "synthea"."vocabulary"."concept" c4 ON c4.concept_id = COALESCE(ds.amount_unit_concept_id, ds.numerator_unit_concept_id)
) qu ON REPLACE(JSON_EXTRACT(mr, '$.medicationCodeableConcept.coding[0].code'), '"', '') = qu.drug_concept_id
    );
  
  