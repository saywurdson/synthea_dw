-- models/cost.sql

SELECT DISTINCT
    REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS cost_id,
    NULL AS cost_event_id,
    32007 AS cost_domain_id,
    5032 AS cost_type_concept_id,
    44818668 AS currency_concept_id,
    NULL AS total_charge,
    CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS DECIMAL) AS total_cost,
    NULL AS total_paid,
    NULL AS paid_by_payer,
    NULL AS paid_by_patient,
    NULL AS paid_patient_copay,
    NULL AS paid_patient_coinsurance,
    NULL AS paid_patient_deductible,
    NULL AS paid_by_primary,
    NULL AS paid_ingredient_cost,
    NULL AS paid_dispensing_fee,
    NULL AS payer_plan_period_id,
    NULL AS amount_allowed,
    38003025 AS revenue_code_concept_id,
    NULL AS revenue_code_source_value,
    NULL AS drg_concept_id,
    NULL AS drg_source_value
FROM
    {{ source('json', 'Claim') }} c