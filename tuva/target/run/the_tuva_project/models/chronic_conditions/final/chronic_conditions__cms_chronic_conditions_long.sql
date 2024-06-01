
  
    
    

    create  table
      "synthea"."chronic_conditions"."cms_chronic_conditions_long__dbt_tmp"
  
    as (
      

with conditions_unioned as (

    select * from "synthea"."chronic_conditions"."_int_cms_chronic_condition_all"
    union distinct
    select * from "synthea"."chronic_conditions"."_int_cms_chronic_condition_hiv_aids"
    union distinct
    select * from "synthea"."chronic_conditions"."_int_cms_chronic_condition_oud"

)

select
      patient_id
    , claim_id
    , start_date
    , chronic_condition_type
    , condition_category
    , condition
    , data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from conditions_unioned
    );
  
  