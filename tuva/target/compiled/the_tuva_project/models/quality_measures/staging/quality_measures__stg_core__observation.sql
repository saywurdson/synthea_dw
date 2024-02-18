

select
      patient_id
    , observation_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."observation"

