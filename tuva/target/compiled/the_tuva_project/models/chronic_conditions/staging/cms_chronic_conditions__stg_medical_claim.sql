

select
      claim_id
    , patient_id
    , claim_start_date
    , ms_drg_code
    , data_source
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."tuva_input"."medical_claim"