
select
      patient_id
    , enrollment_start_date
    , enrollment_end_date
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."eligibility"