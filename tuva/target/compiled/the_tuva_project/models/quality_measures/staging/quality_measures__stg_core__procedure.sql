
select
      patient_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."procedure"