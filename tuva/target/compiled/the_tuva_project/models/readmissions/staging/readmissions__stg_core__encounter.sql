

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
    '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."encounter"
where encounter_type = 'acute inpatient'