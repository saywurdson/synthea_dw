
  
    
    

    create  table
      "synthea"."core"."_stg_claims_eligibility__dbt_tmp"
  
    as (
      -- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the eligibility table in core.
-- *************************************************




select
         cast(patient_id as TEXT ) as patient_id
       , cast(member_id as TEXT ) as member_id
       , cast(birth_date as date) as birth_date
       , cast(death_date as date) as death_date
       , cast(enrollment_start_date as date ) as enrollment_start_date
       , cast(enrollment_end_date as date ) as enrollment_end_date
       , cast(payer as TEXT ) as payer
       , cast(payer_type as TEXT ) as payer_type
       , cast(plan as TEXT ) as plan
       , cast(original_reason_entitlement_code as TEXT ) as original_reason_entitlement_code
       , cast(dual_status_code as TEXT ) as dual_status_code
       , cast(medicare_status_code as TEXT ) as medicare_status_code
       , cast(data_source as TEXT ) as data_source
       , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"
    );
  
  