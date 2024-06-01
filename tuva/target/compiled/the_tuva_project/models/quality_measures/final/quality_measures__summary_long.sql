

/* measures should already be at the full eligibility population grain */
with measures_unioned as (

    select * from "synthea"."quality_measures"."_int_nqf2372_long"
    union all

    select * from "synthea"."quality_measures"."_int_nqf0034_long"
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
    from measures_unioned

)

select
      patient_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , evidence_date
    , case
        when exclusion_flag = 1 then null
        when numerator_flag = 1 then 1
        when denominator_flag = 1 then 0
        else null end as performance_flag
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from add_data_types