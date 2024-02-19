
  
    
    

    create  table
      "synthea"."quality_measures"."summary_wide__dbt_tmp"
  
    as (
      

/*
    Each measure is pivoted into a boolean column by evaluating the
    denominator, numerator, and exclusion flags.
*/
with measures_long as (

        select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , performance_flag
        , measure_id
    from "synthea"."quality_measures"."summary_long"

)

, nqf_2372 as (

    select
          patient_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF2372'

)
, nqf_0034 as (

    select
          patient_id
        , performance_flag
    from measures_long
    where measure_id = 'NQF0034'

)
, joined as (

    select
          measures_long.patient_id
        , nqf_2372.performance_flag as nqf_2372
        , nqf_0034.performance_flag as nqf_0034
    from measures_long
    left join nqf_2372
         on measures_long.patient_id = nqf_2372.patient_id
    left join nqf_0034
         on measures_long.patient_id = nqf_0034.patient_id

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(nqf_2372 as integer) as nqf_2372
        , cast(nqf_0034 as integer) as nqf_0034
    from joined

)

select
      patient_id
    , nqf_2372
    , nqf_0034
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from add_data_types
    );
  
  