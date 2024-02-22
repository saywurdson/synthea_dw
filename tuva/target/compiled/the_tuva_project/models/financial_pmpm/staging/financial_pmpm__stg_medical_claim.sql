


SELECT
  patient_id
, claim_id
, claim_line_number
, claim_start_date
, claim_end_date
, service_category_1
, service_category_2
, paid_amount
, allowed_amount
, payer
, plan
, data_source
, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."medical_claim"