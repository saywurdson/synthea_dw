
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_normalized_input_eligibility_dates_normalize__dbt_tmp"
  
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
) select distinct
  elig.patient_id
  , elig.patient_id||elig.data_source||elig.payer||elig.plan||elig.enrollment_start_date||elig.enrollment_end_date as patient_id_key
  , cal_dob.full_date as normalized_birth_date
  , cal_death.full_date as normalized_death_date
  , cal_enroll_start.full_date as normalized_enrollment_start_date
  , cal_enroll_end.full_date as normalized_enrollment_end_date
  , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from __dbt__cte__normalized_input__stg_eligibility elig
left join "synthea"."terminology"."calendar" cal_dob
    on elig.birth_date = cal_dob.full_date
left join "synthea"."terminology"."calendar" cal_death
    on elig.death_date = cal_death.full_date
left join "synthea"."terminology"."calendar" cal_enroll_start
    on elig.enrollment_start_date = cal_enroll_start.full_date
left join "synthea"."terminology"."calendar" cal_enroll_end
    on elig.enrollment_end_date = cal_enroll_end.full_date
    );
  
  