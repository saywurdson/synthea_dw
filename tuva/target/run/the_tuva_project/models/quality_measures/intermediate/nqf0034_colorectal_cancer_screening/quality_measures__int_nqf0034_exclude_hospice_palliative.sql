
  
    
    

    create  table
      "synthea"."quality_measures"."_int_nqf0034_exclude_hospice_palliative__dbt_tmp"
  
    as (
      

/*
DENOMINATOR EXCLUSIONS:
Patient was provided hospice services any time during the measurement period: G9710
*/


with  __dbt__cte__quality_measures__stg_core__condition as (

select
      patient_id
    , claim_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."condition"
),  __dbt__cte__quality_measures__stg_medical_claim as (


select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"


),  __dbt__cte__quality_measures__stg_core__observation as (


select
      patient_id
    , observation_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."observation"


),  __dbt__cte__quality_measures__stg_core__procedure as (

select
      patient_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."procedure"
), exclusion_codes as (
    select
          code
        , case code_system
            when 'SNOMEDCT' then 'snomed-ct'
            when 'ICD9CM' then 'icd-9-cm'
            when 'ICD10CM' then 'icd-10-cm'
            when 'CPT' then 'hcpcs'
            when 'ICD10PCS' then 'icd-10-pcs'
          else lower(code_system) end as code_system
        , concept_name
    From "synthea"."quality_measures"."_value_set_value_sets"
    where concept_name in  (
          'Hospice Care Ambulatory'
        , 'Hospice Encounter'
        , 'Palliative Care Encounter'
        , 'Palliative Care Intervention'
    )


)

, conditions as (

    select
          patient_id
        , claim_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__condition )

, medical_claim as (

    select
          patient_id
        , claim_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from __dbt__cte__quality_measures__stg_medical_claim

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
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__observation

)

, procedures as (

    select
          patient_id
        , procedure_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.concept_name as concept_name
    from conditions
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp
        on recorded_date between pp.performance_period_begin and pp.performance_period_end

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name as concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp on 1=1
    where exclusion_codes.code_system = 'hcpcs'
    and claim_start_date between pp.performance_period_begin and pp.performance_period_end
)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name as concept_name
    from observations
    inner join exclusion_codes
        on observations.code = exclusion_codes.code
        and observations.code_type = exclusion_codes.code_system
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp on 1=1
    where observation_date between pp.performance_period_begin and pp.performance_period_end

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name as concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp on 1=1
    where procedure_date between pp.performance_period_begin and pp.performance_period_end

)

, patients_with_exclusions as(
    select patient_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select patient_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , concept_name as exclusion_reason
    from med_claim_exclusions

    union all

    select patient_id
        , observation_date as exclusion_date
        , concept_name as exclusion_reason
    from observation_exclusions

    union all

    select patient_id
        , procedure_date as exclusion_date
        , concept_name as exclusion_reason
    from procedure_exclusions

)



select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from patients_with_exclusions
    );
  
  