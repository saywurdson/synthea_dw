

select 
      patient_id
    , normalized_code
    , recorded_date
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."condition"