

select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."tuva_input"."pharmacy_claim"