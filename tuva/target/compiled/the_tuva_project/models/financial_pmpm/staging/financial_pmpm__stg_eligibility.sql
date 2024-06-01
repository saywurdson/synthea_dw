

select
  patient_id
, enrollment_start_date
, enrollment_end_date
, payer
, plan
, data_source
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."eligibility"