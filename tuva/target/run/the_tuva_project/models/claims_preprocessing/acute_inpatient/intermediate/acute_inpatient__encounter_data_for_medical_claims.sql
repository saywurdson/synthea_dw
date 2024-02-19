
  
  create view "synthea"."claims_preprocessing"."_int_encounter_data_for_medical_claims__dbt_tmp" as (
    

-- *************************************************
-- This dbt model returns all the columns with relevant
-- encounter fields that we will append to the medical_claim
-- table.

-- It returns a table with these columns:
--      patient_id
--      claim_id
--      encounter_type
--      encounter_id
--      start_date (date used for merging claims into encounters)
--      end_date (date used for merging claims into encounters)
--      encounter_start_date,
--      encounter_end_date,
--      encounter_admit_source_code,
--      encounter_admit_type_code,
--      encounter_discharge_disposition_code
--      orphan_claim_flag (always 0 or 1) (never null)
--      encounter_count (could be 0,1,2,3,...) (never null)
-- *************************************************




with acute_inpatient_claims_with_encounter_id as (
select
  patient_id,
  claim_id,
  start_date,
  end_date,  
-- Relevant encounter-level fields for
-- professional and institutional acute inpatient
-- claims that are assigned to an encounter:
  'acute inpatient' as encounter_type,
  encounter_id,
  encounter_start_date,
  encounter_end_date,
  encounter_admit_source_code,
  encounter_admit_type_code,
  encounter_discharge_disposition_code,
-- Fields that are only relevant for professional
-- acute inpatient claims that are not assigned to
-- an encounter because they are orphan claims or because
-- they overlap with more than one encounter:
  0 as orphan_claim_flag,
  1 as encounter_count
from "synthea"."claims_preprocessing"."_int_acute_inpatient_claims_with_encounter_data"
),


acute_inpatient_claims_without_encounter_id as (
select
  patient_id,
  claim_id,
  start_date,
  end_date,  
-- Relevant encounter-level fields for
-- professional and institutional acute inpatient
-- claims that are assigned to an encounter:
  'acute inpatient' encounter_type,
  cast(null as TEXT) as encounter_id,
  cast(null as date) as encounter_start_date,
  cast(null as date) as encounter_end_date,
  cast(null as TEXT) as encounter_admit_source_code,
  cast(null as TEXT) as encounter_admit_type_code,
  cast(null as TEXT) as encounter_discharge_disposition_code,
-- Fields that are only relevant for professional
-- acute inpatient claims that are not assigned to
-- an encounter because they are orphan claims or because
-- they overlap with more than one encounter:
  orphan_claim_flag,
  encounter_count
  
from "synthea"."claims_preprocessing"."_int_acute_inpatient_professional_encounter_id"
where (orphan_claim_flag = 1) or (encounter_count > 1)
)


select *, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from acute_inpatient_claims_with_encounter_id

union all

select *, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from acute_inpatient_claims_without_encounter_id
  );
