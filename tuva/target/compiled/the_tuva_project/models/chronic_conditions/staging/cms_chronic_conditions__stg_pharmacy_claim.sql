

select
      claim_id
    , patient_id
    , paid_date
    , ndc_code
    , data_source
    , '2024-02-16 00:16:32.331507+00:00' as tuva_last_run
from "synthea"."tuva_input"."pharmacy_claim"