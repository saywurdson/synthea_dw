
select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , data_source
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."core"."condition"