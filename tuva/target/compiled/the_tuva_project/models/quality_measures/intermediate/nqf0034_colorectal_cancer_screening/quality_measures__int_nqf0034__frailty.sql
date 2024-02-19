

/*
DENOMINATOR EXCLUSIONS:
Patients 66 years of age and older with at least one claim/encounter
for frailty during the measurement period (not full exclusion, used
in conjunction with dementia medication or
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
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."condition"
),  __dbt__cte__quality_measures__stg_medical_claim as (


select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"


),  __dbt__cte__quality_measures__stg_core__observation as (


select
      patient_id
    , observation_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."observation"


),  __dbt__cte__quality_measures__stg_core__procedure as (

select
      patient_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."procedure"
), aged_patients as (
    select distinct patient_id
    from "synthea"."quality_measures"."_int_nqf0034_denominator"
    where max_age >=66

)

, exclusion_codes as (
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
        , case when code in ('G2100','G2101') then 1 else  0 end as meets_all_criteria
    From "synthea"."quality_measures"."_value_set_value_sets"
    where concept_name in  (
          'Frailty Device'
        , 'Frailty Diagnosis'
        , 'Frailty Encounter'
        , 'Frailty Symptom'
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
        , exclusion_codes.concept_name
    from conditions
        inner join aged_patients
            on conditions.patient_id = aged_patients.patient_id
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp on
        recorded_date between pp.performance_period_begin and pp.performance_period_end

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name
    from medical_claim
        inner join aged_patients
            on medical_claim.patient_id = aged_patients.patient_id
        inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
        inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp
            on claim_start_date between pp.performance_period_begin and pp.performance_period_end
    where exclusion_codes.code_system = 'hcpcs'


)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name
    from observations
    inner join aged_patients
        on observations.patient_id = aged_patients.patient_id
    inner join exclusion_codes
        on observations.code = exclusion_codes.code
        and observations.code_type = exclusion_codes.code_system
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp
        on observation_date between pp.performance_period_begin and pp.performance_period_end

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
    inner join aged_patients
        on procedures.patient_id = aged_patients.patient_id
    inner join exclusion_codes
        on procedures.code = exclusion_codes.code
        and procedures.code_type = exclusion_codes.code_system
    inner join "synthea"."quality_measures"."_int_nqf0034__performance_period" as pp on
       procedure_date between pp.performance_period_begin and pp.performance_period_end

)

, patients_with_exclusions as(
    select patient_id
      , recorded_date as exclusion_date
      , concept_name as concept_name
    from condition_exclusions

    union distinct

    select patient_id
      , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
      , concept_name as concept_name
    from med_claim_exclusions

    union distinct

    select patient_id
      , observation_date as exclusion_date
      , concept_name as concept_name
    from observation_exclusions

    union distinct

    select patient_id
      , procedure_date as exclusion_date
      , concept_name as concept_name
    from procedure_exclusions

)
, ordered_exclusions as (
    select patient_id
        , exclusion_date
        , concept_name
        , row_number() over (partition by patient_id order by exclusion_date) as exclusion_row
    from patients_with_exclusions
    )

select  patient_id
    , exclusion_date
    , concept_name
from ordered_exclusions