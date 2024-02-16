

select
      claim_id
    , patient_id
    , claim_start_date
    , ms_drg_code
    , data_source
    , '2024-02-16 00:16:32.331507+00:00' as tuva_last_run
from "synthea"."tuva_input"."medical_claim"