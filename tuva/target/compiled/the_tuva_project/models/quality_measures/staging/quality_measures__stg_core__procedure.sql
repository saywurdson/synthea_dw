
select
      patient_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."procedure"