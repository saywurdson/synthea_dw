

select 
      patient_id
    , normalized_code
    , recorded_date
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."condition"