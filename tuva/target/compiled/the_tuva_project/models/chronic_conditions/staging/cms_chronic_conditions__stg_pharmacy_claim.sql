

select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."tuva_input"."pharmacy_claim"