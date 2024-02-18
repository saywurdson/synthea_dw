

select
    encounter_id
    , encounter_type
    , patient_id
    , encounter_end_date
    , facility_npi
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , primary_diagnosis_description
    , paid_amount
    , allowed_amount
    , charge_amount
from "synthea"."core"."encounter"