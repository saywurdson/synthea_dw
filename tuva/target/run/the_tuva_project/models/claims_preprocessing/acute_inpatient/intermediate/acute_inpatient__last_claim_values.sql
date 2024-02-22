
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_acute_inpatient_last_claim_values__dbt_tmp"
  
    as (
      

with  __dbt__cte__acute_inpatient__stg_medical_claim as (


select 
    claim_id
    , claim_line_number
    , patient_id
    , claim_type
    , claim_start_date
    , claim_end_date
    , admission_date
    , discharge_date
    , facility_npi
    , ms_drg_code
    , apr_drg_code
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , paid_amount
    , allowed_amount
    , charge_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , data_source
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
), first_claim_values as(
    select distinct
        e.encounter_id
        , coalesce(claim_start_date, admission_date) as claim_start
        , discharge_disposition_code
    from "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id" e
    inner join __dbt__cte__acute_inpatient__stg_medical_claim m
        on e.claim_id = m.claim_id
    where claim_type = 'institutional'
)

select
    encounter_id
    , claim_start
    , discharge_disposition_code
    , row_number() over (partition by encounter_id order by claim_start desc) as claim_row
from first_claim_values
    );
  
  