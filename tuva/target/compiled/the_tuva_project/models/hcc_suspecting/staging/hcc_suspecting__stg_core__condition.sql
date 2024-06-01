
select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , data_source
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."condition"