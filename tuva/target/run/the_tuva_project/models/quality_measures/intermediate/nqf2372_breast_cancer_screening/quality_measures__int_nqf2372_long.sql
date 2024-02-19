
  
    
    

    create  table
      "synthea"."quality_measures"."_int_nqf2372_long__dbt_tmp"
  
    as (
      

/* selecting the full patient population as the grain of this table */
with  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."patient"
), patient as (

    select distinct patient_id
    from __dbt__cte__quality_measures__stg_core__patient

)

, denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , denominator_flag
    from "synthea"."quality_measures"."_int_nqf2372_denominator"

)

, numerator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , evidence_date
        , numerator_flag
    from "synthea"."quality_measures"."_int_nqf2372_numerator"

)

, exclusions as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , exclusion_date
        , exclusion_reason
        , exclusion_flag
    from "synthea"."quality_measures"."_int_nqf2372_exclusions"

)

, measure_flags as (

    select
          patient.patient_id
        , case
            when denominator.patient_id is not null
            then denominator.denominator_flag
            else null
          end as denominator_flag
        , case
            when numerator.patient_id is not null
            then numerator.numerator_flag
            else null
          end as numerator_flag
        , case
            when exclusions.patient_id is not null
            then exclusions.exclusion_flag
            else null
          end as exclusion_flag
        , numerator.evidence_date
        , exclusions.exclusion_date
        , exclusions.exclusion_reason
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
    from patient
        left join denominator
            on patient.patient_id = denominator.patient_id
        left join numerator
            on patient.patient_id = numerator.patient_id
        left join exclusions
            on patient.patient_id = exclusions.patient_id

)

/*
    Deduplicate measure rows by latest evidence date or exclusion date
*/
, add_rownum as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , row_number() over(
            partition by
                  patient_id
                , performance_period_begin
                , performance_period_end
                , measure_id
                , measure_name
            order by
                  evidence_date desc nulls last
                , exclusion_date desc nulls last
          ) as row_num
    from measure_flags

)

, deduped as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from add_rownum
    where row_num = 1

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(evidence_date as date) as evidence_date
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
    from deduped

)

select
      patient_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , evidence_date
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from add_data_types
    );
  
  