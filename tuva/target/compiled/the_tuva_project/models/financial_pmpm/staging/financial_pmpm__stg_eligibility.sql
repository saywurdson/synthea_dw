

select
  patient_id
, enrollment_start_date
, enrollment_end_date
, payer
, plan
, data_source
, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."eligibility"