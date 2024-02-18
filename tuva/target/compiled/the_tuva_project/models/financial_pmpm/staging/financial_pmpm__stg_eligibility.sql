

select
  patient_id
, enrollment_start_date
, enrollment_end_date
, payer
, plan
, data_source
, '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."eligibility"