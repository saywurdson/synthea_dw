

select
      patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."encounter"

