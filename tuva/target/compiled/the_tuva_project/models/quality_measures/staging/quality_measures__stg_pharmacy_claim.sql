

select
      patient_id
    , dispensing_date
    , ndc_code
    , paid_date
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."pharmacy_claim"

