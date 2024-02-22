

select
      claim_id
    , patient_id
    , procedure_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."procedure"