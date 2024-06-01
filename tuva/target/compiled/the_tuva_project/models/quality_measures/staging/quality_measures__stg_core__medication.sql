

select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."medication"

