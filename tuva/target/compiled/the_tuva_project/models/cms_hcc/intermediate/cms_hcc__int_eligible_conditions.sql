
/*
Steps for staging condition data:
    1) Filter to risk-adjustable claims per claim type for the collection year.
    2) Gather diagnosis codes from condition for the eligible claims.
    3) Map and filter diagnosis codes to HCCs

Claims filtering logic:
 - Professional:
    - CPT/HCPCS in CPT/HCPCS seed file from CMS
 - Inpatient:
    - Bill type code in (11X, 41X)
 - Outpatient:
    - Bill type code in (12X, 13X, 43X, 71X, 73X, 76X, 77X, 85X)
    - CPT/HCPCS in CPT/HCPCS seed file from CMS

Jinja is used to set payment year variable.
 - The payment_year var has been set here so it gets compiled.
 - The collection year is one year prior to the payment year.
*/

with  __dbt__cte__cms_hcc__stg_core__medical_claim as (

select
      claim_id
    , claim_line_number
    , claim_type
    , patient_id
    , claim_start_date
    , claim_end_date
    , bill_type_code
    , hcpcs_code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"
),  __dbt__cte__cms_hcc__stg_core__condition as (

select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."condition"
), medical_claims as (

    select
          claim_id
        , claim_line_number
        , claim_type
        , patient_id
        , claim_start_date
        , claim_end_date
        , bill_type_code
        , hcpcs_code
    from __dbt__cte__cms_hcc__stg_core__medical_claim

)

, conditions as (

    select
          claim_id
        , patient_id
        , code
    from __dbt__cte__cms_hcc__stg_core__condition
    where code_type = 'icd-10-cm'

)

, cpt_hcpcs_list as (

    select
          payment_year
        , hcpcs_cpt_code
    from "synthea"."cms_hcc"."_value_set_cpt_hcpcs"

)

, professional_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.patient_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
    where claim_type = 'professional'
        and extract(year from claim_end_date) = 2023
        and cpt_hcpcs_list.payment_year = 2024

)

, inpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.patient_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
    from medical_claims
    where claim_type = 'institutional'
        and extract(year from claim_end_date) = 2023
        and left(bill_type_code,2) in ('11','41')

)

, outpatient_claims as (

    select
          medical_claims.claim_id
        , medical_claims.claim_line_number
        , medical_claims.claim_type
        , medical_claims.patient_id
        , medical_claims.claim_start_date
        , medical_claims.claim_end_date
        , medical_claims.bill_type_code
        , medical_claims.hcpcs_code
    from medical_claims
        inner join cpt_hcpcs_list
            on medical_claims.hcpcs_code = cpt_hcpcs_list.hcpcs_cpt_code
    where claim_type = 'institutional'
        and extract(year from claim_end_date) = 2023
        and cpt_hcpcs_list.payment_year = 2024
        and left(bill_type_code,2) in ('12','13','43','71','73','76','77','85')

)

, eligible_claims as (

    select * from professional_claims
    union all
    select * from inpatient_claims
    union all
    select * from outpatient_claims

)

, eligible_conditions as (

    select distinct
          eligible_claims.claim_id
        , eligible_claims.patient_id
        , conditions.code
    from eligible_claims
        inner join conditions
            on eligible_claims.claim_id = conditions.claim_id
            and eligible_claims.patient_id = conditions.patient_id

)

, add_data_types as (

    select distinct
          cast(patient_id as TEXT) as patient_id
        , cast(code as TEXT) as condition_code
        , cast('2024' as integer) as payment_year
    from eligible_conditions

)

select
      patient_id
    , condition_code
    , payment_year
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from add_data_types