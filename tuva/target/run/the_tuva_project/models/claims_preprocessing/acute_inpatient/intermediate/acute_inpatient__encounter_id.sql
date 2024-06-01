
  
  create view "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id__dbt_tmp" as (
    

with __dbt__cte__acute_inpatient__stg_medical_claim as (


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
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
) -- *************************************************
-- This dbt model assigns an encounter_id to each
-- institutional or professional acute inpatient claim
-- that is eligible to be part of an encounter.
-- Professional acute inpatient claims that are
-- orphan claims (don't overlap with an institutional
-- acute inpatient claim) or that have
-- encounter_count > 1 (overlap with more than one different
-- acute inpatient encounter) are not included here.
-- It returns a table with these 3 columns:
--      patient_id
--      claim_id
--      encounter_id
-- *************************************************




select
  inst.patient_id,
  inst.claim_id,
  med.claim_line_number,
  inst.encounter_id,
  '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_acute_inpatient_institutional_encounter_id" inst
left join __dbt__cte__acute_inpatient__stg_medical_claim med
    on inst.claim_id = med.claim_id

union distinct

select
  patient_id,
  claim_id,
  claim_line_number,
  encounter_id,
  '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_acute_inpatient_professional_encounter_id"
where (orphan_claim_flag = 0) and (encounter_count = 1)
  );
