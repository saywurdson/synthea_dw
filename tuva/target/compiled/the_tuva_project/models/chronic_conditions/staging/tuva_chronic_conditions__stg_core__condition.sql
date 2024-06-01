

select 
      patient_id
    , normalized_code
    , recorded_date
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."condition"