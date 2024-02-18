
  
  create view "synthea"."readmissions"."_int_encounter__dbt_tmp" as (
    

with __dbt__cte__readmissions__stg_core__encounter as (


select
    encounter_id,
    patient_id,
    encounter_start_date,
    encounter_end_date,
    discharge_disposition_code,
    facility_npi,
    ms_drg_code,
    paid_amount,
    primary_diagnosis_code,
    '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."encounter"
where encounter_type = 'acute inpatient'
) -- Staging model for the input layer:
-- stg_encounter input layer model.
-- This contains one row for every unique encounter in the dataset.

select
    cast(encounter_id as TEXT) as encounter_id,
    cast(patient_id as TEXT) as patient_id,
    cast(encounter_start_date as date) as admit_date,
    cast(encounter_end_date as date) as discharge_date,
    cast(discharge_disposition_code as TEXT) as discharge_disposition_code,
    cast(facility_npi as TEXT) as facility_npi,
    cast(ms_drg_code as TEXT) as ms_drg_code,
    cast(paid_amount as numeric) as paid_amount,
    cast(primary_diagnosis_code as TEXT) as primary_diagnosis_code,
    '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from __dbt__cte__readmissions__stg_core__encounter
  );
