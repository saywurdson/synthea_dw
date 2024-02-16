
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
    set performance lookback periods for each type of test

    during the measurement period:
    - Fecal occult blood test (FOBT) during the measurement period

    during the measurement period or the two years prior:
    - Fecal immunochemical DNA test (FIT-DNA)

    during the measurement period or the four years prior:
    - Flexible sigmoidoscopy
    - Computed tomography (CT) colonography

    during the measurement period or the nine years prior:
    - Colonoscopy

*/
, period_lookback as (

    select
          performance_period_end
        , performance_period_begin
        , 

    performance_period_end + ((interval '1 year') * (-2))

 as performance_period_begin_1yp
        , 

    performance_period_end + ((interval '1 year') * (-3))

 as performance_period_begin_2yp
        , 

    performance_period_end + ((interval '1 year') * (-5))

 as performance_period_begin_4yp
        , 

    performance_period_end + ((interval '1 year') * (-10))

 as performance_period_begin_9yp
    from period_begin

)

select
      cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
    , cast(performance_period_begin_1yp as date) as performance_period_begin_1yp
    , cast(performance_period_begin_2yp as date) as performance_period_begin_2yp
    , cast(performance_period_begin_4yp as date) as performance_period_begin_4yp
    , cast(performance_period_begin_9yp as date) as performance_period_begin_9yp
from period_lookback