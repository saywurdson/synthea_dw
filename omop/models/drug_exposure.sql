-- models/drug_exposure.sql

SELECT 
    ROW_NUMBER() OVER (ORDER BY drug_exposure_id) AS drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    verbatim_end_date,
    drug_type_concept_id,
    stop_reason,
    refills,
    quantity,
    days_supply,
    sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS drug_exposure_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
        COALESCE((
            SELECT c.concept_id
            FROM {{ source('reference', 'concept') }} c
            WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.medicationCodeableConcept.coding[0].code'), '"', '')
            AND (c.vocabulary_id = 'RxNorm')
            AND (c.domain_id = 'Drug')
            AND c.invalid_reason IS NULL
            AND c.standard_concept = 'S'
            AND (
                c.concept_class_id = 'Branded Drug' 
                OR c.concept_class_id = 'Branded Pack' 
                OR c.concept_class_id = 'Clinical Drug' 
                OR c.concept_class_id = 'Clinical Pack'
                OR c.concept_class_id = 'Quant Branded Drug'
                OR c.concept_class_id = 'Quant Clinical Drug'
            )
        ), 0) AS drug_concept_id,
        CAST(JSON_EXTRACT(data, '$.authoredOn') AS DATE) AS drug_exposure_start_date,
        CAST(JSON_EXTRACT(data, '$.authoredOn') AS TIMESTAMP) AS drug_exposure_start_datetime,
        NULL AS drug_exposure_end_date,
        NULL AS drug_exposure_end_datetime,
        NULL AS verbatim_end_date,
        32817 AS drug_type_concept_id,
        CASE
            WHEN REPLACE(JSON_EXTRACT(data, '$.status'), '"', '') = 'stopped' THEN REPLACE(JSON_EXTRACT(data, '$.status'), '"', '')
            ELSE NULL
        END AS stop_reason,
        NULL AS refills,
        NULL AS quantity,
        NULL AS days_supply,
        NULL AS sig,
        NULL AS route_concept_id,
        NULL AS lot_number,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.requester.reference'), '"Practitioner/', ''), '"', '') AS provider_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
        NULL AS visit_detail_id,
        REPLACE(JSON_EXTRACT(data, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS drug_source_value,
        COALESCE((
            SELECT c.concept_id
            FROM {{ source('reference', 'concept') }} c
            WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.medicationCodeableConcept.coding[0].code'), '"', '')
            AND (c.vocabulary_id = 'RxNorm')
            AND (c.domain_id = 'Drug')
            AND (
                c.concept_class_id = 'Branded Drug' 
                OR c.concept_class_id = 'Branded Pack' 
                OR c.concept_class_id = 'Clinical Drug' 
                OR c.concept_class_id = 'Clinical Pack'
                OR c.concept_class_id = 'Quant Branded Drug'
                OR c.concept_class_id = 'Quant Clinical Drug'
            )
        ), 0) AS drug_source_concept_id,
        NULL AS route_source_value,
        NULL AS dose_unit_source_value
    FROM {{ source('raw', 'MedicationRequest') }}
    WHERE drug_source_value IS NOT NULL

    UNION

    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS drug_exposure_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.patient.reference'), '"Patient/', ''), '"', '') AS person_id,
        CAST({{ get_standard_concept_id('concept_code', 'data', '$.vaccineCode.coding[0].code', 'CVX', 'Drug', 'CVX') }} AS INTEGER) AS drug_concept_id,
        vo.visit_start_date AS drug_exposure_start_date,
        vo.visit_start_datetime AS drug_exposure_start_datetime,
        vo.visit_end_date AS drug_exposure_end_date,
        vo.visit_end_datetime AS drug_exposure_end_datetime,
        NULL AS verbatim_end_date,
        32817 AS drug_type_concept_id,
        CASE
            WHEN REPLACE(JSON_EXTRACT(data, '$.status'), '"', '') = 'completed' THEN REPLACE(JSON_EXTRACT(data, '$.status'), '"', '')
            ELSE NULL
        END AS stop_reason,
        0 AS refills,
        1 AS quantity,
        1 AS days_supply,
        NULL AS sig,
        NULL AS route_concept_id,
        NULL AS lot_number,
        vo.provider_id AS provider_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
        NULL AS visit_detail_id,
        REPLACE(JSON_EXTRACT(data, '$.vaccineCode.coding[0].code'), '"', '') AS drug_source_value,
        CAST({{ get_concept_id('concept_code', 'data', '$.vaccineCode.coding[0].code', 'CVX', 'Drug') }} AS INTEGER) AS drug_source_concept_id,
        NULL AS route_source_value,
        NULL AS dose_unit_source_value
    FROM {{ source('raw', 'Immunization') }}
    LEFT JOIN {{ ref('visit_occurrence') }} AS vo
    ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.encounter.reference'), '"Encounter/', ''), '"', '') = vo.visit_occurrence_id
    WHERE drug_source_value IS NOT NULL

    UNION

    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(data, '$.id'), '"', '') AS drug_exposure_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.subject.reference'), '"Patient/', ''), '"', '') AS person_id,
        COALESCE((
            SELECT c.concept_id
            FROM {{ source('reference', 'concept') }} c
            WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.medicationCodeableConcept.coding[0].code'), '"', '')
            AND (c.vocabulary_id = 'RxNorm')
            AND (c.domain_id = 'Drug')
            AND c.invalid_reason IS NULL
            AND c.standard_concept = 'S'
            AND (
                c.concept_class_id = 'Branded Drug' 
                OR c.concept_class_id = 'Branded Pack' 
                OR c.concept_class_id = 'Clinical Drug' 
                OR c.concept_class_id = 'Clinical Pack'
                OR c.concept_class_id = 'Quant Branded Drug'
                OR c.concept_class_id = 'Quant Clinical Drug'
            )
        ), 0) AS drug_concept_id,
        CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS DATE) AS drug_exposure_start_date,
        CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS TIMESTAMP) AS drug_exposure_start_datetime,
        CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS DATE)  AS drug_exposure_end_date,
        CAST(JSON_EXTRACT(data, '$.effectiveDateTime') AS TIMESTAMP) AS drug_exposure_end_datetime,
        NULL AS verbatim_end_date,
        32817 AS drug_type_concept_id,
        CASE
            WHEN REPLACE(JSON_EXTRACT(data, '$.status'), '"', '') = 'completed' THEN REPLACE(JSON_EXTRACT(data, '$.status'), '"', '')
            ELSE NULL
        END AS stop_reason,
        0 AS refills,
        1 AS quantity,
        1 AS days_supply,
        NULL AS sig,
        NULL AS route_concept_id,
        NULL AS lot_number,
        vo.provider_id,
        REPLACE(REPLACE(JSON_EXTRACT(data, '$.context.reference'), '"Encounter/', ''), '"', '') AS visit_occurrence_id,
        NULL AS visit_detail_id,
        REPLACE(JSON_EXTRACT(data, '$.medicationCodeableConcept.coding[0].code'), '"', '') AS drug_source_value,
        COALESCE((
            SELECT c.concept_id
            FROM {{ source('reference', 'concept') }} c
            WHERE c.concept_code = REPLACE(JSON_EXTRACT(data, '$.medicationCodeableConcept.coding[0].code'), '"', '')
            AND (c.vocabulary_id = 'RxNorm')
            AND (c.domain_id = 'Drug')
            AND (
                c.concept_class_id = 'Branded Drug' 
                OR c.concept_class_id = 'Branded Pack' 
                OR c.concept_class_id = 'Clinical Drug' 
                OR c.concept_class_id = 'Clinical Pack'
                OR c.concept_class_id = 'Quant Branded Drug'
                OR c.concept_class_id = 'Quant Clinical Drug'
            )
        ), 0) AS drug_source_concept_id,
        NULL AS route_source_value,
        NULL AS dose_unit_source_value
    FROM {{ source('raw', 'MedicationAdministration') }}
    LEFT JOIN {{ ref('visit_occurrence') }} AS vo
    ON REPLACE(REPLACE(JSON_EXTRACT(data, '$.context.reference'), '"Encounter/', ''), '"', '') = vo.visit_occurrence_id
    WHERE drug_source_value IS NOT NULL
) AS drug_exposure