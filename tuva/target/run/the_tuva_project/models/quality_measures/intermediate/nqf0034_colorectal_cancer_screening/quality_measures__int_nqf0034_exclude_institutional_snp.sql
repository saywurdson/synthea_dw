
  
    
    

    create  table
      "synthea"."quality_measures"."_int_nqf0034_exclude_institutional_snp__dbt_tmp"
  
    as (
      

/*
    Patients greater than or equal to 66 in Institutional Special Needs Plans (SNP)
    or residing in long term care

    Future enhancement: group claims into encounters
*/

with  __dbt__cte__quality_measures__stg_medical_claim as (


select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"


), aged_patients as (
    select distinct patient_id
    from "synthea"."quality_measures"."_int_nqf0034_denominator"
    where max_age >=66

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from __dbt__cte__quality_measures__stg_medical_claim

)

, exclusions as (

    select
          aged_patients.patient_id
        , coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) as exclusion_date
        , 'Institutional or Long Term Care' as exclusion_reason
    from aged_patients
         inner join medical_claim
         on aged_patients.patient_id = medical_claim.patient_id

    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" pp
        on coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) between pp.performance_period_begin and pp.performance_period_end

    where place_of_service_code in ('32', '33', '34', '54', '56')
    and 
        ((medical_claim.claim_end_date)::date - (medical_claim.claim_start_date)::date)
     >= 90
)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from exclusions
    );
  
  