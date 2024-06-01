

/*
    Hospice services used by patient any time during the measurement period
*/

with  __dbt__cte__quality_measures__stg_core__observation as (


select
      patient_id
    , observation_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."observation"


),  __dbt__cte__quality_measures__stg_medical_claim as (


select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"


),  __dbt__cte__quality_measures__stg_core__procedure as (

select
      patient_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."procedure"
), denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
    from "synthea"."quality_measures"."_int_nqf2372_denominator"

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from "synthea"."quality_measures"."_value_set_value_sets"
    where concept_name in (
          'Hospice Care Ambulatory'
        , 'Hospice Encounter'
    )

)

, observations as (

    select
          patient_id
        , observation_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__observation

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

, procedures as (

    select
          patient_id
        , procedure_date
        , normalized_code_type
        , normalized_code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name
    from observations
         inner join exclusion_codes
             on observations.code = exclusion_codes.code
             and observations.code_type = exclusion_codes.code_system

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    where exclusion_codes.code_system = 'hcpcs'

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.normalized_code = exclusion_codes.code
             and procedures.normalized_code_type = exclusion_codes.code_system

)

, hospice as (

    select
          denominator.patient_id
        , observation_exclusions.observation_date as exclusion_date
        , observation_exclusions.concept_name as exclusion_reason
    from denominator
         inner join observation_exclusions
            on denominator.patient_id = observation_exclusions.patient_id
    where observation_exclusions.observation_date
        between denominator.performance_period_begin
        and denominator.performance_period_end

    union all

    select
          denominator.patient_id
        , coalesce (
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , med_claim_exclusions.concept_name as exclusion_reason
    from denominator
         inner join med_claim_exclusions
            on denominator.patient_id = med_claim_exclusions.patient_id
    where (
        med_claim_exclusions.claim_start_date
            between denominator.performance_period_begin
            and denominator.performance_period_end
        or med_claim_exclusions.claim_end_date
            between denominator.performance_period_begin
            and denominator.performance_period_end
    )

    union all

    select
          denominator.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from denominator
         inner join procedure_exclusions
            on denominator.patient_id = procedure_exclusions.patient_id
    where procedure_exclusions.procedure_date
        between denominator.performance_period_begin
        and denominator.performance_period_end

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from hospice