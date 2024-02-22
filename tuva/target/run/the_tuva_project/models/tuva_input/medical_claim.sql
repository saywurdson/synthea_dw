
  
    
    

    create  table
      "synthea"."tuva_input"."medical_claim__dbt_tmp"
  
    as (
      -- models/medical_claim.sql

SELECT DISTINCT
    claim_id,
    claim_line_number,
    claim_type,
    patient_id,
    member_id,
    payer,
    plan,
    claim_start_date,
    claim_end_date,
    claim_line_start_date,
    claim_line_end_date,
    admission_date,
    discharge_date,
    admit_source_code,
    admit_type_code,
    discharge_disposition_code,
    place_of_service_code,
    bill_type_code,
    ms_drg_code,
    apr_drg_code,
    revenue_center_code,
    service_unit_quantity,
    hcpcs_code,
    hcpcs_modifier_1,
    hcpcs_modifier_2,
    hcpcs_modifier_3,
    hcpcs_modifier_4,
    hcpcs_modifier_5,
    rendering_npi,
    billing_npi,
    facility_npi,
    paid_date,
    paid_amount,
    allowed_amount,
    charge_amount,
    coinsurance_amount,
    copayment_amount,
    deductible_amount,
    total_cost_amount,
    diagnosis_code_type,
    diagnosis_code_1,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    diagnosis_code_9,
    diagnosis_code_10,
    diagnosis_code_11,
    diagnosis_code_12,
    diagnosis_code_13,
    diagnosis_code_14,
    diagnosis_code_15,
    diagnosis_code_16,
    diagnosis_code_17,
    diagnosis_code_18,
    diagnosis_code_19,
    diagnosis_code_20,
    diagnosis_code_21,
    diagnosis_code_22,
    diagnosis_code_23,
    diagnosis_code_24,
    diagnosis_code_25,
    diagnosis_poa_1,
    diagnosis_poa_2,
    diagnosis_poa_3,
    diagnosis_poa_4,
    diagnosis_poa_5,
    diagnosis_poa_6,
    diagnosis_poa_7,
    diagnosis_poa_8,
    diagnosis_poa_9,
    diagnosis_poa_10,
    diagnosis_poa_11,
    diagnosis_poa_12,
    diagnosis_poa_13,
    diagnosis_poa_14,
    diagnosis_poa_15,
    diagnosis_poa_16,
    diagnosis_poa_17,
    diagnosis_poa_18,
    diagnosis_poa_19,
    diagnosis_poa_20,
    diagnosis_poa_21,
    diagnosis_poa_22,
    diagnosis_poa_23,
    diagnosis_poa_24,
    diagnosis_poa_25,
    procedure_code_type,
    procedure_code_1,
    procedure_code_2,
    procedure_code_3,
    procedure_code_4,
    procedure_code_5,
    procedure_code_6,
    procedure_code_7,
    procedure_code_8,
    procedure_code_9,
    procedure_code_10,
    procedure_code_11,
    procedure_code_12,
    procedure_code_13,
    procedure_code_14,
    procedure_code_15,
    procedure_code_16,
    procedure_code_17,
    procedure_code_18,
    procedure_code_19,
    procedure_code_20,
    procedure_code_21,
    procedure_code_22,
    procedure_code_23,
    procedure_code_24,
    procedure_code_25,
    procedure_date_1,
    procedure_date_2,
    procedure_date_3,
    procedure_date_4,
    procedure_date_5,
    procedure_date_6,
    procedure_date_7,
    procedure_date_8,
    procedure_date_9,
    procedure_date_10,
    procedure_date_11,
    procedure_date_12,
    procedure_date_13,
    procedure_date_14,
    procedure_date_15,
    procedure_date_16,
    procedure_date_17,
    procedure_date_18,
    procedure_date_19,
    procedure_date_20,
    procedure_date_21,
    procedure_date_22,
    procedure_date_23,
    procedure_date_24,
    procedure_date_25,
    data_source
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
        REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') AS claim_line_number,
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
        NULL AS member_id,
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', '') AS payer,
        NULL AS plan,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END AS admission_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END AS discharge_date,
        '3' AS admit_source_code,
        9 AS admit_type_code,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END AS discharge_disposition_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END AS place_of_service_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END AS bill_type_code,
        msdrg."MS-DRG" AS ms_drg_code,
        MAX(aprdrg.apr_drg_code) AS apr_drg_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END AS revenue_center_code,
        NULL AS service_unit_quantity,
        NULL AS hcpcs_code,
        NULL AS hcpcs_modifier_1,
        NULL AS hcpcs_modifier_2,
        NULL AS hcpcs_modifier_3,
        NULL AS hcpcs_modifier_4,
        NULL AS hcpcs_modifier_5,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', '') AS rendering_npi,
        NULL AS billing_npi,
        NULL AS facility_npi,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
        NULL AS allowed_amount,
        NULL AS charge_amount,
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)) AS coinsurance_amount,
        NULL AS copayment_amount,
        NULL AS deductible_amount,
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS total_cost_amount,
        'icd-10-cm' AS diagnosis_code_type,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_1,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_2,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_3,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_4,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_5,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_6,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_7,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_8,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_9,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_10,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_11,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_12,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_13,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_14,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_15,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_16,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_17,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_18,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_19,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_20,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_21,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_22,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_23,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_24,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_25,
        'Y' AS diagnosis_poa_1,
        'U' AS diagnosis_poa_2,
        'U' AS diagnosis_poa_3,
        'U' AS diagnosis_poa_4,
        'U' AS diagnosis_poa_5,
        'U' AS diagnosis_poa_6,
        'U' AS diagnosis_poa_7,
        'U' AS diagnosis_poa_8,
        'U' AS diagnosis_poa_9,
        'U' AS diagnosis_poa_10,
        'U' AS diagnosis_poa_11,
        'U' AS diagnosis_poa_12,
        'U' AS diagnosis_poa_13,
        'U' AS diagnosis_poa_14,
        'U' AS diagnosis_poa_15,
        'U' AS diagnosis_poa_16,
        'U' AS diagnosis_poa_17,
        'U' AS diagnosis_poa_18,
        'U' AS diagnosis_poa_19,
        'U' AS diagnosis_poa_20,
        'U' AS diagnosis_poa_21,
        'U' AS diagnosis_poa_22,
        'U' AS diagnosis_poa_23,
        'U' AS diagnosis_poa_24,
        'U' AS diagnosis_poa_25,
        'snomed' AS procedure_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code_1,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', '') AS procedure_code_2,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', '') AS procedure_code_3,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', '') AS procedure_code_4,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', '') AS procedure_code_5,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', '') AS procedure_code_6,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', '') AS procedure_code_7,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', '') AS procedure_code_8,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', '') AS procedure_code_9,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', '') AS procedure_code_10,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', '') AS procedure_code_11,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', '') AS procedure_code_12,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', '') AS procedure_code_13,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', '') AS procedure_code_14,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', '') AS procedure_code_15,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', '') AS procedure_code_16,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', '') AS procedure_code_17,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', '') AS procedure_code_18,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', '') AS procedure_code_19,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', '') AS procedure_code_20,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', '') AS procedure_code_21,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', '') AS procedure_code_22,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', '') AS procedure_code_23,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', '') AS procedure_code_24,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', '') AS procedure_code_25,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_1,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_2,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_3,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_4,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_5,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_6,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_7,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_8,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_9,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_10,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_11,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_12,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_13,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_14,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_15,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_16,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_17,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_18,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_19,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_20,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_21,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_22,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_23,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_24,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_25,
        'SyntheaFhir' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''), REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') ORDER BY REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '')) AS rn
    FROM "synthea"."json"."Claim" c
    LEFT JOIN "synthea"."json"."ExplanationOfBenefit" e ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Condition" co ON REPLACE(JSON_EXTRACT(co, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.diagnosis[0].diagnosisReference.reference'), '"Condition/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Procedure" p ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.procedure[0].procedureReference.reference'), '"Procedure/', ''), '"', '')
    LEFT JOIN "synthea"."terminology"."snomed_icd_10_map" map ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
    LEFT JOIN "synthea"."reference"."icd10cm_to_msdrg_v41" msdrg ON REPLACE(msdrg.ICD10, '.', '') = map.map_target
    LEFT JOIN "synthea"."terminology"."apr_drg" aprdrg ON aprdrg.mdc_code = msdrg.MDC
    LEFT JOIN "synthea"."json"."Practitioner" pr ON REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.provider.reference'), '"Practitioner/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Patient" pat ON REPLACE(JSON_EXTRACT(pat, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '')
    WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'
    AND REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', '') IS NOT NULL
    GROUP BY
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.item[0].sequence'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', ''),
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END,
        msdrg."MS-DRG",
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE),
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT),
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)),
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT),
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE)
) AS claims
WHERE rn = 1

UNION

SELECT DISTINCT
    claim_id,
    claim_line_number,
    claim_type,
    patient_id,
    member_id,
    payer,
    plan,
    claim_start_date,
    claim_end_date,
    claim_line_start_date,
    claim_line_end_date,
    admission_date,
    discharge_date,
    admit_source_code,
    admit_type_code,
    discharge_disposition_code,
    place_of_service_code,
    bill_type_code,
    ms_drg_code,
    apr_drg_code,
    revenue_center_code,
    service_unit_quantity,
    hcpcs_code,
    hcpcs_modifier_1,
    hcpcs_modifier_2,
    hcpcs_modifier_3,
    hcpcs_modifier_4,
    hcpcs_modifier_5,
    rendering_npi,
    billing_npi,
    facility_npi,
    paid_date,
    paid_amount,
    allowed_amount,
    charge_amount,
    coinsurance_amount,
    copayment_amount,
    deductible_amount,
    total_cost_amount,
    diagnosis_code_type,
    diagnosis_code_1,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    diagnosis_code_9,
    diagnosis_code_10,
    diagnosis_code_11,
    diagnosis_code_12,
    diagnosis_code_13,
    diagnosis_code_14,
    diagnosis_code_15,
    diagnosis_code_16,
    diagnosis_code_17,
    diagnosis_code_18,
    diagnosis_code_19,
    diagnosis_code_20,
    diagnosis_code_21,
    diagnosis_code_22,
    diagnosis_code_23,
    diagnosis_code_24,
    diagnosis_code_25,
    diagnosis_poa_1,
    diagnosis_poa_2,
    diagnosis_poa_3,
    diagnosis_poa_4,
    diagnosis_poa_5,
    diagnosis_poa_6,
    diagnosis_poa_7,
    diagnosis_poa_8,
    diagnosis_poa_9,
    diagnosis_poa_10,
    diagnosis_poa_11,
    diagnosis_poa_12,
    diagnosis_poa_13,
    diagnosis_poa_14,
    diagnosis_poa_15,
    diagnosis_poa_16,
    diagnosis_poa_17,
    diagnosis_poa_18,
    diagnosis_poa_19,
    diagnosis_poa_20,
    diagnosis_poa_21,
    diagnosis_poa_22,
    diagnosis_poa_23,
    diagnosis_poa_24,
    diagnosis_poa_25,
    procedure_code_type,
    procedure_code_1,
    procedure_code_2,
    procedure_code_3,
    procedure_code_4,
    procedure_code_5,
    procedure_code_6,
    procedure_code_7,
    procedure_code_8,
    procedure_code_9,
    procedure_code_10,
    procedure_code_11,
    procedure_code_12,
    procedure_code_13,
    procedure_code_14,
    procedure_code_15,
    procedure_code_16,
    procedure_code_17,
    procedure_code_18,
    procedure_code_19,
    procedure_code_20,
    procedure_code_21,
    procedure_code_22,
    procedure_code_23,
    procedure_code_24,
    procedure_code_25,
    procedure_date_1,
    procedure_date_2,
    procedure_date_3,
    procedure_date_4,
    procedure_date_5,
    procedure_date_6,
    procedure_date_7,
    procedure_date_8,
    procedure_date_9,
    procedure_date_10,
    procedure_date_11,
    procedure_date_12,
    procedure_date_13,
    procedure_date_14,
    procedure_date_15,
    procedure_date_16,
    procedure_date_17,
    procedure_date_18,
    procedure_date_19,
    procedure_date_20,
    procedure_date_21,
    procedure_date_22,
    procedure_date_23,
    procedure_date_24,
    procedure_date_25,
    data_source
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
        REPLACE(JSON_EXTRACT(c, '$.item[1].sequence'), '"', '') AS claim_line_number,
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
        NULL AS member_id,
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', '') AS payer,
        NULL AS plan,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END AS admission_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END AS discharge_date,
        '3' AS admit_source_code,
        9 AS admit_type_code,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END AS discharge_disposition_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END AS place_of_service_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END AS bill_type_code,
        msdrg."MS-DRG" AS ms_drg_code,
        MAX(aprdrg.apr_drg_code) AS apr_drg_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END AS revenue_center_code,
        NULL AS service_unit_quantity,
        NULL AS hcpcs_code,
        NULL AS hcpcs_modifier_1,
        NULL AS hcpcs_modifier_2,
        NULL AS hcpcs_modifier_3,
        NULL AS hcpcs_modifier_4,
        NULL AS hcpcs_modifier_5,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', '') AS rendering_npi,
        NULL AS billing_npi,
        NULL AS facility_npi,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
        NULL AS allowed_amount,
        NULL AS charge_amount,
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)) AS coinsurance_amount,
        NULL AS copayment_amount,
        NULL AS deductible_amount,
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS total_cost_amount,
        'icd-10-cm' AS diagnosis_code_type,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_1,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_2,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_3,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_4,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_5,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_6,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_7,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_8,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_9,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_10,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_11,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_12,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_13,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_14,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_15,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_16,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_17,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_18,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_19,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_20,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_21,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_22,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_23,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_24,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_25,
        'Y' AS diagnosis_poa_1,
        'U' AS diagnosis_poa_2,
        'U' AS diagnosis_poa_3,
        'U' AS diagnosis_poa_4,
        'U' AS diagnosis_poa_5,
        'U' AS diagnosis_poa_6,
        'U' AS diagnosis_poa_7,
        'U' AS diagnosis_poa_8,
        'U' AS diagnosis_poa_9,
        'U' AS diagnosis_poa_10,
        'U' AS diagnosis_poa_11,
        'U' AS diagnosis_poa_12,
        'U' AS diagnosis_poa_13,
        'U' AS diagnosis_poa_14,
        'U' AS diagnosis_poa_15,
        'U' AS diagnosis_poa_16,
        'U' AS diagnosis_poa_17,
        'U' AS diagnosis_poa_18,
        'U' AS diagnosis_poa_19,
        'U' AS diagnosis_poa_20,
        'U' AS diagnosis_poa_21,
        'U' AS diagnosis_poa_22,
        'U' AS diagnosis_poa_23,
        'U' AS diagnosis_poa_24,
        'U' AS diagnosis_poa_25,
        'snomed' AS procedure_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code_1,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', '') AS procedure_code_2,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', '') AS procedure_code_3,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', '') AS procedure_code_4,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', '') AS procedure_code_5,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', '') AS procedure_code_6,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', '') AS procedure_code_7,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', '') AS procedure_code_8,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', '') AS procedure_code_9,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', '') AS procedure_code_10,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', '') AS procedure_code_11,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', '') AS procedure_code_12,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', '') AS procedure_code_13,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', '') AS procedure_code_14,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', '') AS procedure_code_15,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', '') AS procedure_code_16,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', '') AS procedure_code_17,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', '') AS procedure_code_18,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', '') AS procedure_code_19,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', '') AS procedure_code_20,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', '') AS procedure_code_21,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', '') AS procedure_code_22,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', '') AS procedure_code_23,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', '') AS procedure_code_24,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', '') AS procedure_code_25,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_1,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_2,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_3,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_4,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_5,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_6,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_7,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_8,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_9,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_10,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_11,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_12,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_13,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_14,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_15,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_16,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_17,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_18,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_19,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_20,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_21,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_22,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_23,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_24,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_25,
        'SyntheaFhir' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''), REPLACE(JSON_EXTRACT(c, '$.item[1].sequence'), '"', '') ORDER BY REPLACE(JSON_EXTRACT(c, '$.item[1].sequence'), '"', '')) AS rn
    FROM "synthea"."json"."Claim" c
    LEFT JOIN "synthea"."json"."ExplanationOfBenefit" e ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Condition" co ON REPLACE(JSON_EXTRACT(co, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.diagnosis[1].diagnosisReference.reference'), '"Condition/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Procedure" p ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.procedure[1].procedureReference.reference'), '"Procedure/', ''), '"', '')
    LEFT JOIN "synthea"."terminology"."snomed_icd_10_map" map ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
    LEFT JOIN "synthea"."reference"."icd10cm_to_msdrg_v41" msdrg ON REPLACE(msdrg.ICD10, '.', '') = map.map_target
    LEFT JOIN "synthea"."terminology"."apr_drg" aprdrg ON aprdrg.mdc_code = msdrg.MDC
    LEFT JOIN "synthea"."json"."Practitioner" pr ON REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.provider.reference'), '"Practitioner/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Patient" pat ON REPLACE(JSON_EXTRACT(pat, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '')
    WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'
    AND REPLACE(JSON_EXTRACT(c, '$.item[1].sequence'), '"', '') IS NOT NULL
    GROUP BY
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.item[1].sequence'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', ''),
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END,
        msdrg."MS-DRG",
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE),
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT),
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)),
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT),
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE)
) AS claims
WHERE rn = 1

UNION

SELECT DISTINCT
    claim_id,
    claim_line_number,
    claim_type,
    patient_id,
    member_id,
    payer,
    plan,
    claim_start_date,
    claim_end_date,
    claim_line_start_date,
    claim_line_end_date,
    admission_date,
    discharge_date,
    admit_source_code,
    admit_type_code,
    discharge_disposition_code,
    place_of_service_code,
    bill_type_code,
    ms_drg_code,
    apr_drg_code,
    revenue_center_code,
    service_unit_quantity,
    hcpcs_code,
    hcpcs_modifier_1,
    hcpcs_modifier_2,
    hcpcs_modifier_3,
    hcpcs_modifier_4,
    hcpcs_modifier_5,
    rendering_npi,
    billing_npi,
    facility_npi,
    paid_date,
    paid_amount,
    allowed_amount,
    charge_amount,
    coinsurance_amount,
    copayment_amount,
    deductible_amount,
    total_cost_amount,
    diagnosis_code_type,
    diagnosis_code_1,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    diagnosis_code_9,
    diagnosis_code_10,
    diagnosis_code_11,
    diagnosis_code_12,
    diagnosis_code_13,
    diagnosis_code_14,
    diagnosis_code_15,
    diagnosis_code_16,
    diagnosis_code_17,
    diagnosis_code_18,
    diagnosis_code_19,
    diagnosis_code_20,
    diagnosis_code_21,
    diagnosis_code_22,
    diagnosis_code_23,
    diagnosis_code_24,
    diagnosis_code_25,
    diagnosis_poa_1,
    diagnosis_poa_2,
    diagnosis_poa_3,
    diagnosis_poa_4,
    diagnosis_poa_5,
    diagnosis_poa_6,
    diagnosis_poa_7,
    diagnosis_poa_8,
    diagnosis_poa_9,
    diagnosis_poa_10,
    diagnosis_poa_11,
    diagnosis_poa_12,
    diagnosis_poa_13,
    diagnosis_poa_14,
    diagnosis_poa_15,
    diagnosis_poa_16,
    diagnosis_poa_17,
    diagnosis_poa_18,
    diagnosis_poa_19,
    diagnosis_poa_20,
    diagnosis_poa_21,
    diagnosis_poa_22,
    diagnosis_poa_23,
    diagnosis_poa_24,
    diagnosis_poa_25,
    procedure_code_type,
    procedure_code_1,
    procedure_code_2,
    procedure_code_3,
    procedure_code_4,
    procedure_code_5,
    procedure_code_6,
    procedure_code_7,
    procedure_code_8,
    procedure_code_9,
    procedure_code_10,
    procedure_code_11,
    procedure_code_12,
    procedure_code_13,
    procedure_code_14,
    procedure_code_15,
    procedure_code_16,
    procedure_code_17,
    procedure_code_18,
    procedure_code_19,
    procedure_code_20,
    procedure_code_21,
    procedure_code_22,
    procedure_code_23,
    procedure_code_24,
    procedure_code_25,
    procedure_date_1,
    procedure_date_2,
    procedure_date_3,
    procedure_date_4,
    procedure_date_5,
    procedure_date_6,
    procedure_date_7,
    procedure_date_8,
    procedure_date_9,
    procedure_date_10,
    procedure_date_11,
    procedure_date_12,
    procedure_date_13,
    procedure_date_14,
    procedure_date_15,
    procedure_date_16,
    procedure_date_17,
    procedure_date_18,
    procedure_date_19,
    procedure_date_20,
    procedure_date_21,
    procedure_date_22,
    procedure_date_23,
    procedure_date_24,
    procedure_date_25,
    data_source
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
        REPLACE(JSON_EXTRACT(c, '$.item[2].sequence'), '"', '') AS claim_line_number,
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
        NULL AS member_id,
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', '') AS payer,
        NULL AS plan,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END AS admission_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END AS discharge_date,
        '3' AS admit_source_code,
        9 AS admit_type_code,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END AS discharge_disposition_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END AS place_of_service_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END AS bill_type_code,
        msdrg."MS-DRG" AS ms_drg_code,
        MAX(aprdrg.apr_drg_code) AS apr_drg_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END AS revenue_center_code,
        NULL AS service_unit_quantity,
        NULL AS hcpcs_code,
        NULL AS hcpcs_modifier_1,
        NULL AS hcpcs_modifier_2,
        NULL AS hcpcs_modifier_3,
        NULL AS hcpcs_modifier_4,
        NULL AS hcpcs_modifier_5,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', '') AS rendering_npi,
        NULL AS billing_npi,
        NULL AS facility_npi,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
        NULL AS allowed_amount,
        NULL AS charge_amount,
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)) AS coinsurance_amount,
        NULL AS copayment_amount,
        NULL AS deductible_amount,
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS total_cost_amount,
        'icd-10-cm' AS diagnosis_code_type,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_1,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_2,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_3,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_4,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_5,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_6,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_7,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_8,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_9,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_10,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_11,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_12,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_13,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_14,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_15,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_16,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_17,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_18,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_19,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_20,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_21,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_22,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_23,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_24,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_25,
        'Y' AS diagnosis_poa_1,
        'U' AS diagnosis_poa_2,
        'U' AS diagnosis_poa_3,
        'U' AS diagnosis_poa_4,
        'U' AS diagnosis_poa_5,
        'U' AS diagnosis_poa_6,
        'U' AS diagnosis_poa_7,
        'U' AS diagnosis_poa_8,
        'U' AS diagnosis_poa_9,
        'U' AS diagnosis_poa_10,
        'U' AS diagnosis_poa_11,
        'U' AS diagnosis_poa_12,
        'U' AS diagnosis_poa_13,
        'U' AS diagnosis_poa_14,
        'U' AS diagnosis_poa_15,
        'U' AS diagnosis_poa_16,
        'U' AS diagnosis_poa_17,
        'U' AS diagnosis_poa_18,
        'U' AS diagnosis_poa_19,
        'U' AS diagnosis_poa_20,
        'U' AS diagnosis_poa_21,
        'U' AS diagnosis_poa_22,
        'U' AS diagnosis_poa_23,
        'U' AS diagnosis_poa_24,
        'U' AS diagnosis_poa_25,
        'snomed' AS procedure_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code_1,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', '') AS procedure_code_2,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', '') AS procedure_code_3,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', '') AS procedure_code_4,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', '') AS procedure_code_5,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', '') AS procedure_code_6,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', '') AS procedure_code_7,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', '') AS procedure_code_8,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', '') AS procedure_code_9,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', '') AS procedure_code_10,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', '') AS procedure_code_11,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', '') AS procedure_code_12,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', '') AS procedure_code_13,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', '') AS procedure_code_14,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', '') AS procedure_code_15,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', '') AS procedure_code_16,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', '') AS procedure_code_17,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', '') AS procedure_code_18,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', '') AS procedure_code_19,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', '') AS procedure_code_20,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', '') AS procedure_code_21,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', '') AS procedure_code_22,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', '') AS procedure_code_23,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', '') AS procedure_code_24,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', '') AS procedure_code_25,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_1,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_2,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_3,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_4,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_5,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_6,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_7,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_8,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_9,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_10,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_11,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_12,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_13,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_14,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_15,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_16,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_17,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_18,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_19,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_20,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_21,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_22,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_23,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_24,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_25,
        'SyntheaFhir' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''), REPLACE(JSON_EXTRACT(c, '$.item[2].sequence'), '"', '') ORDER BY REPLACE(JSON_EXTRACT(c, '$.item[2].sequence'), '"', '')) AS rn
    FROM "synthea"."json"."Claim" c
    LEFT JOIN "synthea"."json"."ExplanationOfBenefit" e ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Condition" co ON REPLACE(JSON_EXTRACT(co, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.diagnosis[2].diagnosisReference.reference'), '"Condition/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Procedure" p ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.procedure[2].procedureReference.reference'), '"Procedure/', ''), '"', '')
    LEFT JOIN "synthea"."terminology"."snomed_icd_10_map" map ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
    LEFT JOIN "synthea"."reference"."icd10cm_to_msdrg_v41" msdrg ON REPLACE(msdrg.ICD10, '.', '') = map.map_target
    LEFT JOIN "synthea"."terminology"."apr_drg" aprdrg ON aprdrg.mdc_code = msdrg.MDC
    LEFT JOIN "synthea"."json"."Practitioner" pr ON REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.provider.reference'), '"Practitioner/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Patient" pat ON REPLACE(JSON_EXTRACT(pat, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '')
    WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'
    AND REPLACE(JSON_EXTRACT(c, '$.item[2].sequence'), '"', '') IS NOT NULL
    GROUP BY
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.item[2].sequence'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', ''),
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END,
        msdrg."MS-DRG",
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE),
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT),
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)),
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT),
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE)
) AS claims
WHERE rn = 1

UNION

SELECT DISTINCT
    claim_id,
    claim_line_number,
    claim_type,
    patient_id,
    member_id,
    payer,
    plan,
    claim_start_date,
    claim_end_date,
    claim_line_start_date,
    claim_line_end_date,
    admission_date,
    discharge_date,
    admit_source_code,
    admit_type_code,
    discharge_disposition_code,
    place_of_service_code,
    bill_type_code,
    ms_drg_code,
    apr_drg_code,
    revenue_center_code,
    service_unit_quantity,
    hcpcs_code,
    hcpcs_modifier_1,
    hcpcs_modifier_2,
    hcpcs_modifier_3,
    hcpcs_modifier_4,
    hcpcs_modifier_5,
    rendering_npi,
    billing_npi,
    facility_npi,
    paid_date,
    paid_amount,
    allowed_amount,
    charge_amount,
    coinsurance_amount,
    copayment_amount,
    deductible_amount,
    total_cost_amount,
    diagnosis_code_type,
    diagnosis_code_1,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    diagnosis_code_9,
    diagnosis_code_10,
    diagnosis_code_11,
    diagnosis_code_12,
    diagnosis_code_13,
    diagnosis_code_14,
    diagnosis_code_15,
    diagnosis_code_16,
    diagnosis_code_17,
    diagnosis_code_18,
    diagnosis_code_19,
    diagnosis_code_20,
    diagnosis_code_21,
    diagnosis_code_22,
    diagnosis_code_23,
    diagnosis_code_24,
    diagnosis_code_25,
    diagnosis_poa_1,
    diagnosis_poa_2,
    diagnosis_poa_3,
    diagnosis_poa_4,
    diagnosis_poa_5,
    diagnosis_poa_6,
    diagnosis_poa_7,
    diagnosis_poa_8,
    diagnosis_poa_9,
    diagnosis_poa_10,
    diagnosis_poa_11,
    diagnosis_poa_12,
    diagnosis_poa_13,
    diagnosis_poa_14,
    diagnosis_poa_15,
    diagnosis_poa_16,
    diagnosis_poa_17,
    diagnosis_poa_18,
    diagnosis_poa_19,
    diagnosis_poa_20,
    diagnosis_poa_21,
    diagnosis_poa_22,
    diagnosis_poa_23,
    diagnosis_poa_24,
    diagnosis_poa_25,
    procedure_code_type,
    procedure_code_1,
    procedure_code_2,
    procedure_code_3,
    procedure_code_4,
    procedure_code_5,
    procedure_code_6,
    procedure_code_7,
    procedure_code_8,
    procedure_code_9,
    procedure_code_10,
    procedure_code_11,
    procedure_code_12,
    procedure_code_13,
    procedure_code_14,
    procedure_code_15,
    procedure_code_16,
    procedure_code_17,
    procedure_code_18,
    procedure_code_19,
    procedure_code_20,
    procedure_code_21,
    procedure_code_22,
    procedure_code_23,
    procedure_code_24,
    procedure_code_25,
    procedure_date_1,
    procedure_date_2,
    procedure_date_3,
    procedure_date_4,
    procedure_date_5,
    procedure_date_6,
    procedure_date_7,
    procedure_date_8,
    procedure_date_9,
    procedure_date_10,
    procedure_date_11,
    procedure_date_12,
    procedure_date_13,
    procedure_date_14,
    procedure_date_15,
    procedure_date_16,
    procedure_date_17,
    procedure_date_18,
    procedure_date_19,
    procedure_date_20,
    procedure_date_21,
    procedure_date_22,
    procedure_date_23,
    procedure_date_24,
    procedure_date_25,
    data_source
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
        REPLACE(JSON_EXTRACT(c, '$.item[3].sequence'), '"', '') AS claim_line_number,
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
        NULL AS member_id,
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', '') AS payer,
        NULL AS plan,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END AS admission_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END AS discharge_date,
        '3' AS admit_source_code,
        9 AS admit_type_code,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END AS discharge_disposition_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END AS place_of_service_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END AS bill_type_code,
        msdrg."MS-DRG" AS ms_drg_code,
        MAX(aprdrg.apr_drg_code) AS apr_drg_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END AS revenue_center_code,
        NULL AS service_unit_quantity,
        NULL AS hcpcs_code,
        NULL AS hcpcs_modifier_1,
        NULL AS hcpcs_modifier_2,
        NULL AS hcpcs_modifier_3,
        NULL AS hcpcs_modifier_4,
        NULL AS hcpcs_modifier_5,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', '') AS rendering_npi,
        NULL AS billing_npi,
        NULL AS facility_npi,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
        NULL AS allowed_amount,
        NULL AS charge_amount,
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)) AS coinsurance_amount,
        NULL AS copayment_amount,
        NULL AS deductible_amount,
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS total_cost_amount,
        'icd-10-cm' AS diagnosis_code_type,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_1,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_2,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_3,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_4,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_5,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_6,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_7,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_8,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_9,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_10,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_11,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_12,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_13,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_14,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_15,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_16,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_17,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_18,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_19,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_20,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_21,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_22,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_23,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_24,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_25,
        'Y' AS diagnosis_poa_1,
        'U' AS diagnosis_poa_2,
        'U' AS diagnosis_poa_3,
        'U' AS diagnosis_poa_4,
        'U' AS diagnosis_poa_5,
        'U' AS diagnosis_poa_6,
        'U' AS diagnosis_poa_7,
        'U' AS diagnosis_poa_8,
        'U' AS diagnosis_poa_9,
        'U' AS diagnosis_poa_10,
        'U' AS diagnosis_poa_11,
        'U' AS diagnosis_poa_12,
        'U' AS diagnosis_poa_13,
        'U' AS diagnosis_poa_14,
        'U' AS diagnosis_poa_15,
        'U' AS diagnosis_poa_16,
        'U' AS diagnosis_poa_17,
        'U' AS diagnosis_poa_18,
        'U' AS diagnosis_poa_19,
        'U' AS diagnosis_poa_20,
        'U' AS diagnosis_poa_21,
        'U' AS diagnosis_poa_22,
        'U' AS diagnosis_poa_23,
        'U' AS diagnosis_poa_24,
        'U' AS diagnosis_poa_25,
        'snomed' AS procedure_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code_1,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', '') AS procedure_code_2,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', '') AS procedure_code_3,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', '') AS procedure_code_4,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', '') AS procedure_code_5,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', '') AS procedure_code_6,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', '') AS procedure_code_7,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', '') AS procedure_code_8,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', '') AS procedure_code_9,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', '') AS procedure_code_10,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', '') AS procedure_code_11,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', '') AS procedure_code_12,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', '') AS procedure_code_13,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', '') AS procedure_code_14,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', '') AS procedure_code_15,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', '') AS procedure_code_16,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', '') AS procedure_code_17,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', '') AS procedure_code_18,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', '') AS procedure_code_19,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', '') AS procedure_code_20,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', '') AS procedure_code_21,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', '') AS procedure_code_22,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', '') AS procedure_code_23,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', '') AS procedure_code_24,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', '') AS procedure_code_25,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_1,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_2,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_3,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_4,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_5,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_6,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_7,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_8,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_9,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_10,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_11,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_12,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_13,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_14,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_15,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_16,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_17,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_18,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_19,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_20,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_21,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_22,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_23,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_24,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_25,
        'SyntheaFhir' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''), REPLACE(JSON_EXTRACT(c, '$.item[3].sequence'), '"', '') ORDER BY REPLACE(JSON_EXTRACT(c, '$.item[3].sequence'), '"', '')) AS rn
    FROM "synthea"."json"."Claim" c
    LEFT JOIN "synthea"."json"."ExplanationOfBenefit" e ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Condition" co ON REPLACE(JSON_EXTRACT(co, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.diagnosis[3].diagnosisReference.reference'), '"Condition/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Procedure" p ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.procedure[3].procedureReference.reference'), '"Procedure/', ''), '"', '')
    LEFT JOIN "synthea"."terminology"."snomed_icd_10_map" map ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
    LEFT JOIN "synthea"."reference"."icd10cm_to_msdrg_v41" msdrg ON REPLACE(msdrg.ICD10, '.', '') = map.map_target
    LEFT JOIN "synthea"."terminology"."apr_drg" aprdrg ON aprdrg.mdc_code = msdrg.MDC
    LEFT JOIN "synthea"."json"."Practitioner" pr ON REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.provider.reference'), '"Practitioner/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Patient" pat ON REPLACE(JSON_EXTRACT(pat, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '')
    WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'
    AND REPLACE(JSON_EXTRACT(c, '$.item[3].sequence'), '"', '') IS NOT NULL
    GROUP BY
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.item[3].sequence'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', ''),
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END,
        msdrg."MS-DRG",
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE),
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT),
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)),
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT),
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE)
) AS claims
WHERE rn = 1

UNION

SELECT DISTINCT
    claim_id,
    claim_line_number,
    claim_type,
    patient_id,
    member_id,
    payer,
    plan,
    claim_start_date,
    claim_end_date,
    claim_line_start_date,
    claim_line_end_date,
    admission_date,
    discharge_date,
    admit_source_code,
    admit_type_code,
    discharge_disposition_code,
    place_of_service_code,
    bill_type_code,
    ms_drg_code,
    apr_drg_code,
    revenue_center_code,
    service_unit_quantity,
    hcpcs_code,
    hcpcs_modifier_1,
    hcpcs_modifier_2,
    hcpcs_modifier_3,
    hcpcs_modifier_4,
    hcpcs_modifier_5,
    rendering_npi,
    billing_npi,
    facility_npi,
    paid_date,
    paid_amount,
    allowed_amount,
    charge_amount,
    coinsurance_amount,
    copayment_amount,
    deductible_amount,
    total_cost_amount,
    diagnosis_code_type,
    diagnosis_code_1,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    diagnosis_code_9,
    diagnosis_code_10,
    diagnosis_code_11,
    diagnosis_code_12,
    diagnosis_code_13,
    diagnosis_code_14,
    diagnosis_code_15,
    diagnosis_code_16,
    diagnosis_code_17,
    diagnosis_code_18,
    diagnosis_code_19,
    diagnosis_code_20,
    diagnosis_code_21,
    diagnosis_code_22,
    diagnosis_code_23,
    diagnosis_code_24,
    diagnosis_code_25,
    diagnosis_poa_1,
    diagnosis_poa_2,
    diagnosis_poa_3,
    diagnosis_poa_4,
    diagnosis_poa_5,
    diagnosis_poa_6,
    diagnosis_poa_7,
    diagnosis_poa_8,
    diagnosis_poa_9,
    diagnosis_poa_10,
    diagnosis_poa_11,
    diagnosis_poa_12,
    diagnosis_poa_13,
    diagnosis_poa_14,
    diagnosis_poa_15,
    diagnosis_poa_16,
    diagnosis_poa_17,
    diagnosis_poa_18,
    diagnosis_poa_19,
    diagnosis_poa_20,
    diagnosis_poa_21,
    diagnosis_poa_22,
    diagnosis_poa_23,
    diagnosis_poa_24,
    diagnosis_poa_25,
    procedure_code_type,
    procedure_code_1,
    procedure_code_2,
    procedure_code_3,
    procedure_code_4,
    procedure_code_5,
    procedure_code_6,
    procedure_code_7,
    procedure_code_8,
    procedure_code_9,
    procedure_code_10,
    procedure_code_11,
    procedure_code_12,
    procedure_code_13,
    procedure_code_14,
    procedure_code_15,
    procedure_code_16,
    procedure_code_17,
    procedure_code_18,
    procedure_code_19,
    procedure_code_20,
    procedure_code_21,
    procedure_code_22,
    procedure_code_23,
    procedure_code_24,
    procedure_code_25,
    procedure_date_1,
    procedure_date_2,
    procedure_date_3,
    procedure_date_4,
    procedure_date_5,
    procedure_date_6,
    procedure_date_7,
    procedure_date_8,
    procedure_date_9,
    procedure_date_10,
    procedure_date_11,
    procedure_date_12,
    procedure_date_13,
    procedure_date_14,
    procedure_date_15,
    procedure_date_16,
    procedure_date_17,
    procedure_date_18,
    procedure_date_19,
    procedure_date_20,
    procedure_date_21,
    procedure_date_22,
    procedure_date_23,
    procedure_date_24,
    procedure_date_25,
    data_source
FROM (
    SELECT DISTINCT
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') AS claim_id,
        REPLACE(JSON_EXTRACT(c, '$.item[4].sequence'), '"', '') AS claim_line_number,
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') AS claim_type,
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '') AS patient_id,
        NULL AS member_id,
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', '') AS payer,
        NULL AS plan,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_end_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE) AS claim_line_start_date,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE) AS claim_line_end_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END AS admission_date,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END AS discharge_date,
        '3' AS admit_source_code,
        9 AS admit_type_code,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END AS discharge_disposition_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END AS place_of_service_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END AS bill_type_code,
        msdrg."MS-DRG" AS ms_drg_code,
        MAX(aprdrg.apr_drg_code) AS apr_drg_code,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END AS revenue_center_code,
        NULL AS service_unit_quantity,
        NULL AS hcpcs_code,
        NULL AS hcpcs_modifier_1,
        NULL AS hcpcs_modifier_2,
        NULL AS hcpcs_modifier_3,
        NULL AS hcpcs_modifier_4,
        NULL AS hcpcs_modifier_5,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', '') AS rendering_npi,
        NULL AS billing_npi,
        NULL AS facility_npi,
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE) AS paid_date,
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT) AS paid_amount,
        NULL AS allowed_amount,
        NULL AS charge_amount,
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)) AS coinsurance_amount,
        NULL AS copayment_amount,
        NULL AS deductible_amount,
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) AS total_cost_amount,
        'icd-10-cm' AS diagnosis_code_type,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_1,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_2,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_3,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_4,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_5,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_6,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_7,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_8,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_9,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_10,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_11,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_12,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_13,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_14,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_15,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_16,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_17,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_18,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_19,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_20,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_21,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_22,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_23,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_24,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END AS diagnosis_code_25,
        'Y' AS diagnosis_poa_1,
        'U' AS diagnosis_poa_2,
        'U' AS diagnosis_poa_3,
        'U' AS diagnosis_poa_4,
        'U' AS diagnosis_poa_5,
        'U' AS diagnosis_poa_6,
        'U' AS diagnosis_poa_7,
        'U' AS diagnosis_poa_8,
        'U' AS diagnosis_poa_9,
        'U' AS diagnosis_poa_10,
        'U' AS diagnosis_poa_11,
        'U' AS diagnosis_poa_12,
        'U' AS diagnosis_poa_13,
        'U' AS diagnosis_poa_14,
        'U' AS diagnosis_poa_15,
        'U' AS diagnosis_poa_16,
        'U' AS diagnosis_poa_17,
        'U' AS diagnosis_poa_18,
        'U' AS diagnosis_poa_19,
        'U' AS diagnosis_poa_20,
        'U' AS diagnosis_poa_21,
        'U' AS diagnosis_poa_22,
        'U' AS diagnosis_poa_23,
        'U' AS diagnosis_poa_24,
        'U' AS diagnosis_poa_25,
        'snomed' AS procedure_code_type,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', '') AS procedure_code_1,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', '') AS procedure_code_2,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', '') AS procedure_code_3,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', '') AS procedure_code_4,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', '') AS procedure_code_5,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', '') AS procedure_code_6,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', '') AS procedure_code_7,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', '') AS procedure_code_8,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', '') AS procedure_code_9,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', '') AS procedure_code_10,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', '') AS procedure_code_11,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', '') AS procedure_code_12,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', '') AS procedure_code_13,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', '') AS procedure_code_14,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', '') AS procedure_code_15,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', '') AS procedure_code_16,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', '') AS procedure_code_17,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', '') AS procedure_code_18,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', '') AS procedure_code_19,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', '') AS procedure_code_20,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', '') AS procedure_code_21,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', '') AS procedure_code_22,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', '') AS procedure_code_23,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', '') AS procedure_code_24,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', '') AS procedure_code_25,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_1,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_2,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_3,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_4,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_5,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_6,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_7,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_8,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_9,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_10,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_11,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_12,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_13,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_14,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_15,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_16,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_17,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_18,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_19,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_20,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_21,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_22,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_23,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_24,
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE) AS procedure_date_25,
        'SyntheaFhir' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''), REPLACE(JSON_EXTRACT(c, '$.item[4].sequence'), '"', '') ORDER BY REPLACE(JSON_EXTRACT(c, '$.item[4].sequence'), '"', '')) AS rn
    FROM "synthea"."json"."Claim" c
    LEFT JOIN "synthea"."json"."ExplanationOfBenefit" e ON REPLACE(JSON_EXTRACT(c, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.claim.reference'), '"Claim/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Condition" co ON REPLACE(JSON_EXTRACT(co, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.diagnosis[4].diagnosisReference.reference'), '"Condition/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Procedure" p ON REPLACE(JSON_EXTRACT(p, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.procedure[4].procedureReference.reference'), '"Procedure/', ''), '"', '')
    LEFT JOIN "synthea"."terminology"."snomed_icd_10_map" map ON REPLACE(JSON_EXTRACT(co, '$.code.coding[0].code'), '"', '') = map.referenced_component_id
    LEFT JOIN "synthea"."reference"."icd10cm_to_msdrg_v41" msdrg ON REPLACE(msdrg.ICD10, '.', '') = map.map_target
    LEFT JOIN "synthea"."terminology"."apr_drg" aprdrg ON aprdrg.mdc_code = msdrg.MDC
    LEFT JOIN "synthea"."json"."Practitioner" pr ON REPLACE(JSON_EXTRACT(pr, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(e, '$.provider.reference'), '"Practitioner/', ''), '"', '')
    LEFT JOIN "synthea"."json"."Patient" pat ON REPLACE(JSON_EXTRACT(pat, '$.id'), '"', '') = REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', '')
    WHERE REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') != 'pharmacy'
    AND REPLACE(JSON_EXTRACT(c, '$.item[4].sequence'), '"', '') IS NOT NULL
    GROUP BY
        REPLACE(JSON_EXTRACT(c, '$.id'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.item[4].sequence'), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', ''),
        REPLACE(REPLACE(JSON_EXTRACT(c, '$.patient.reference'), '"Patient/', ''), '"', ''),
        REPLACE(JSON_EXTRACT(c, '$.insurance[0].coverage.display'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE),
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.start'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional'
            THEN CAST(SUBSTRING(JSON_EXTRACT(c, '$.billablePeriod.end'), 2, 10) AS DATE)
            ELSE NULL
        END,
        CASE
            WHEN JSON_EXTRACT(pat, '$.deceasedDateTime') IS NOT NULL THEN 20
            ELSE 1
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional'
            THEN REPLACE(JSON_EXTRACT(e, '$.item[0].locationCodeableConcept.coding[0].code'), '"', '')
            ELSE NULL
        END,
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN '111'
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN '791'
            ELSE NULL
        END,
        msdrg."MS-DRG",
        CASE
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'institutional' THEN 0202
            WHEN REPLACE(JSON_EXTRACT(c, '$.type.coding[0].code'), '"', '') = 'professional' THEN 0500
            ELSE NULL
        END,
        REPLACE(JSON_EXTRACT(pr, '$.identifier[0].value'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(c, '$.created'), 2, 10) AS DATE),
        CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT),
        ABS(CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT) - CAST(REPLACE(JSON_EXTRACT(e, '$.payment.amount.value'), '"', '') AS FLOAT)),
        CAST(REPLACE(JSON_EXTRACT(c, '$.total.value'), '"', '') AS FLOAT),
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[0].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[1].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[2].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[3].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[4].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[5].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[6].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[7].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[8].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[9].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[10].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[11].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[12].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[13].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[14].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[15].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[16].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[17].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[18].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[19].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[20].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[21].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[22].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[23].code') AS VARCHAR), '"', '')
            )
        END,
        CASE
            WHEN (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            ) = 'nan' THEN NULL
            ELSE (
                SELECT REGEXP_REPLACE(map.map_target, '[^a-zA-Z0-9]', '', 'g')
                FROM "synthea"."terminology"."snomed_icd_10_map" AS map
                WHERE map.referenced_component_id = REPLACE(CAST(JSON_EXTRACT(co, '$.code.coding[24].code') AS VARCHAR), '"', '')
            )
        END,
        REPLACE(JSON_EXTRACT(p, '$.code.coding[0].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[1].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[2].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[3].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[4].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[5].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[6].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[7].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[8].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[9].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[10].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[11].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[12].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[13].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[14].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[15].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[16].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[17].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[18].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[19].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[20].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[21].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[22].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[23].code'), '"', ''),
        REPLACE(JSON_EXTRACT(p, '$.code.coding[24].code'), '"', ''),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE),
        CAST(SUBSTRING(JSON_EXTRACT(p, '$.performedDateTime'), 2, 10) AS DATE)
) AS claims
WHERE rn = 1
    );
  
  