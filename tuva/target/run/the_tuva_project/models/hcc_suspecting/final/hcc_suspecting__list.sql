
  
    
    

    create  table
      "synthea"."hcc_suspecting"."list__dbt_tmp"
  
    as (
      

with hcc_history_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , 'Prior coding history' as reason
        , icd_10_cm_code
            || case
                when last_billed is not null then ' last billed on ' || last_billed
                when last_billed is null and last_recorded is not null then ' last recorded on ' || last_recorded
                else ' (missing recorded and billing dates) '
          end as contributing_factor
    from "synthea"."hcc_suspecting"."_int_patient_hcc_history"
    where current_year_billed = false

)

, unioned as (

    select * from hcc_history_suspects

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(data_source as TEXT) as data_source
        , cast(hcc_code as TEXT) as hcc_code
        , cast(hcc_description as TEXT) as hcc_description
        , cast(reason as TEXT) as reason
        , cast(contributing_factor as TEXT) as contributing_factor
    from unioned

)

select
      patient_id
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from add_data_types
    );
  
  