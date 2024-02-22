
select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."condition"