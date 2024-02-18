-- models/pharmacy_claim.sql

SELECT *
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
        REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') AS claim_line_number,
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
        NULL AS member_id,
        REPLACE(JSON_EXTRACT(e, '$.insurance[0].coverage.display'), '"', '') AS payer,
        NULL AS plan,
        REPLACE(JSON_EXTRACT(p, '$.identifier[0].value'), '"', '') AS prescribing_provider_npi,
        NULL AS dispensing_provider_npi,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS dispensing_date,
        (
            SELECT c2.concept_code
            FROM "synthea"."vocabulary"."concept_relationship" cr
            JOIN "synthea"."vocabulary"."concept" c1 ON c1.concept_id = cr.concept_id_1
            JOIN "synthea"."vocabulary"."concept" c2 ON c2.concept_id = cr.concept_id_2
            WHERE c1.concept_code = REPLACE(JSON_EXTRACT(c, '$.item[0].productOrService.coding[0].code'), '"', '')
            AND cr.relationship_id = 'Mapped from'
            AND c2.vocabulary_id = 'NDC'
            AND c2.domain_id = 'Drug'
            AND c2.invalid_reason IS NULL
            AND c1.concept_class_id in ('Branded Drug', 'Clinical Drug', 'Quant Clinical Drug')
            AND LENGTH(c2.concept_code) = 11
            ORDER BY c2.concept_code
            LIMIT 1
        ) AS ndc_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
            THEN CAST(REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') AS INTEGER) * 30
            ELSE 1
        END AS quantity,
        CASE
            WHEN REPLACE(JSON_EXTRACT(m, '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value'), '"', '') IS NOT NULL 
            THEN 30
            ELSE 1
        END AS days_supply,
        0 AS refills,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
        CASE
            WHEN REPLACE(JSON_EXTRACT(e, '$.item[0].adjudication[0].category.coding[0].display'), '"', '') = 'Line Allowed Charge Amount'
                THEN CAST(REPLACE(JSON_EXTRACT(e, '$.item[0].adjudication[0].amount.value'), '"', '') AS FLOAT)
            ELSE 0
        END AS allowed_amount,
        CASE
            WHEN REPLACE(JSON_EXTRACT(e, '$.item[0].adjudication[0].category.coding[0].display'), '"', '') = 'Line Beneficiary Coinsurance Amount'
                THEN CAST(REPLACE(JSON_EXTRACT(e, '$.item[0].adjudication[0].amount.value'), '"', '') AS FLOAT)
            ELSE 0
        END AS coinsurance_amount,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS copayment_amount,
        CASE
            WHEN REPLACE(JSON_EXTRACT(e, '$.item[0].adjudication[0].category.coding[0].display'), '"', '') = 'Line Beneficiary Part B Deductible Amount'
                THEN CAST(REPLACE(JSON_EXTRACT(e, '$.item[0].adjudication[0].amount.value'), '"', '') AS FLOAT)
            ELSE 0
        END AS deductible_amount,
        'SyntheaFhir' AS data_source
    FROM "synthea"."json"."Claim" c
    LEFT JOIN "synthea"."json"."MedicationRequest" m
        ON REPLACE(REPLACE(JSON_EXTRACT(c, '$.prescription.reference'), '"MedicationRequest/', ''), '"', '') = REPLACE(JSON_EXTRACT(m, '$.id'), '"', '')
    LEFT JOIN "synthea"."json"."ExplanationOfBenefit" e
        ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Encounter" enc
        ON REPLACE(REPLACE(JSON_EXTRACT(c, '$.item[0].encounter[0].reference'), '"Encounter/', ''), '"', '') = REPLACE(JSON_EXTRACT(enc, '$.id'), '"', '')
    LEFT JOIN "synthea"."json"."Practitioner" p
        ON REPLACE(REPLACE(JSON_EXTRACT(enc, '$.participant[0].individual.reference'), '"Practitioner/', ''), '"', '') = REPLACE(JSON_EXTRACT(p, '$.id'), '"', '')
    LEFT JOIN "synthea"."vocabulary"."drug_strength" ds ON REPLACE(JSON_EXTRACT(c, '$.item[0].productOrService.coding[0].code'), '"', '') = ds.drug_concept_id
    WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'pharmacy'
) AS pharmacy_claim
WHERE pharmacy_claim.ndc_code IS NOT NULL