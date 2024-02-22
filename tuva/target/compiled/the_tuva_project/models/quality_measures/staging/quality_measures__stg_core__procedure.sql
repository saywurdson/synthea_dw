
select
      patient_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."procedure"