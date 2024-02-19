


SELECT
  patient_id
, dispensing_date
, paid_amount
, allowed_amount
, payer
, plan
, data_source
, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."pharmacy_claim"