
  
  create view "synthea"."quality_measures"."_int_nqf2372__performance_period__dbt_tmp" as (
    /*
    set performance period end to the end of the current calendar year
    or use the quality_measures_period_end variable if provided
*/
with period_end as (

    select
        cast(
        

    

    date_trunc('year', now()) + ((interval '1 year') * (1))

 + ((interval '1 day') * (-1))


        as date)
        
         as performance_period_end
)

/*
    set performance period begin to a year and a day prior
    for a complete calendar year
*/
, period_begin as (

    select
          performance_period_end
        , 

    

    performance_period_end + ((interval '1 year') * (-1))

 + ((interval '1 day') * (1))

 as performance_period_begin
    from period_end

)

/*
    set performance lookback period to 27 months prior to the end of the
    performance period
*/
, period_lookback as (

    select
          performance_period_end
        , performance_period_begin
        , 

    performance_period_end + ((interval '1 month') * (-27))

 as performance_period_lookback
    from period_begin


)

select
      cast((select id
from "synthea"."quality_measures"."_value_set_measures"
where id = 'NQF2372') as TEXT) as measure_id
    , cast((select name
from "synthea"."quality_measures"."_value_set_measures"
where id = 'NQF2372') as TEXT) as measure_name
    , cast((select version
from "synthea"."quality_measures"."_value_set_measures"
where id = 'NQF2372') as TEXT) as measure_version
    , cast(performance_period_end as date) as performance_period_end
    , cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_lookback as date) as performance_period_lookback
from period_lookback
  );