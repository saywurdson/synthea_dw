

select
      claim_id
    , patient_id
    , claim_start_date
    , ms_drg_code
    , data_source
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."tuva_input"."medical_claim"