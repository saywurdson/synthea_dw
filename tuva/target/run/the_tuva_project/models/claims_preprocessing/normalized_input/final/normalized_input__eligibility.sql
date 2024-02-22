
  
    
    

    create  table
      "synthea"."claims_preprocessing"."normalized_input_eligibility__dbt_tmp"
  
    as (
      


with __dbt__cte__normalized_input__stg_eligibility as (



select
      patient_id
    , patient_id||data_source||payer||plan||enrollment_start_date||enrollment_end_date as patient_id_key
    , member_id
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , plan
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , first_name
    , last_name
    , address
    , city
    , state
    , zip_code
    , phone
    , data_source
from "synthea"."tuva_input"."eligibility"
) select
    cast(elig.patient_id as TEXT ) as patient_id
    , cast(elig.member_id as TEXT ) as member_id
    , cast(elig.gender as TEXT ) as gender
    , cast(elig.race as TEXT ) as race
    , cast(date_norm.normalized_birth_date as date ) as birth_date
    , cast(date_norm.normalized_death_date as date ) as death_date
    , cast(elig.death_flag as int ) as death_flag
    , cast(date_norm.normalized_enrollment_start_date as date ) as enrollment_start_date
    , cast(date_norm.normalized_enrollment_end_date as date ) as enrollment_end_date
    , cast(elig.payer as TEXT ) as payer
    , cast(elig.payer_type as TEXT ) as payer_type
    , cast(elig.plan as TEXT ) as plan
    , cast(elig.original_reason_entitlement_code as TEXT ) as original_reason_entitlement_code
    , cast(elig.dual_status_code as TEXT ) as dual_status_code
    , cast(elig.medicare_status_code as TEXT ) as medicare_status_code
    , cast(elig.first_name as TEXT ) as first_name
    , cast(elig.last_name as TEXT ) as last_name
    , cast(elig.address as TEXT ) as address
    , cast(elig.city as TEXT ) as city
    , cast(elig.state as TEXT ) as state
    , cast(elig.zip_code as TEXT ) as zip_code
    , cast(elig.phone as TEXT ) as phone
    , cast(elig.data_source as TEXT ) as data_source
    , cast('2024-02-22 00:26:23.471542+00:00'  as TEXT ) as tuva_last_run
from __dbt__cte__normalized_input__stg_eligibility elig
left join "synthea"."claims_preprocessing"."_int_normalized_input_eligibility_dates_normalize" date_norm
    on elig.patient_id_key = date_norm.patient_id_key
    );
  
  