

select
      claim_id
    , patient_id
    , procedure_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."procedure"