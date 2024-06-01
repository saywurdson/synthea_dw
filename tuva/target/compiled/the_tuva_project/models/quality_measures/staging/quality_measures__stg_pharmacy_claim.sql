

select
      patient_id
    , dispensing_date
    , ndc_code
    , paid_date
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."pharmacy_claim"

