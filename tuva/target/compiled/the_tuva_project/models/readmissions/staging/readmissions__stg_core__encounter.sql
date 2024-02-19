

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
    '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."encounter"
where encounter_type = 'acute inpatient'