

select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"

