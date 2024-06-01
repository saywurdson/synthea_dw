

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
    '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."encounter"
where encounter_type = 'acute inpatient'