


SELECT
  patient_id
, dispensing_date
, paid_amount
, allowed_amount
, payer
, plan
, data_source
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."pharmacy_claim"