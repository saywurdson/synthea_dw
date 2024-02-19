

select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."tuva_input"."pharmacy_claim"