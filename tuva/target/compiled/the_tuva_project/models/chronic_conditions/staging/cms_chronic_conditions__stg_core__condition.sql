

select
      claim_id
    , patient_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."condition"