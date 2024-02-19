
  
    
    

    create  table
      "synthea"."quality_measures"."summary_counts__dbt_tmp"
  
    as (
      

with summary_long as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , performance_flag
    from "synthea"."quality_measures"."summary_long"
    where measure_id is not null

)

, calculate_performance_rate  as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , sum(denominator_flag) as denominator_sum
        , sum(numerator_flag) as numerator_sum
        , sum(exclusion_flag) as exclusion_sum
        , (
            cast(sum(performance_flag) as numeric(28,6)) /
                (cast(count(performance_flag) as numeric(28,6)) )
          )*100 as performance_rate
    from summary_long
    group by
          measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end

)

, add_data_types as (

    select
          cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(denominator_sum as integer) as denominator_sum
        , cast(numerator_sum as integer) as numerator_sum
        , cast(exclusion_sum as integer) as exclusion_sum
        , round(cast(performance_rate as numeric(28,6)),3) as performance_rate
    from calculate_performance_rate

)

select
      measure_id
    , measure_name
    , measure_version
    , performance_period_begin
    , performance_period_end
    , denominator_sum
    , numerator_sum
    , exclusion_sum
    , performance_rate
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from add_data_types
    );
  
  