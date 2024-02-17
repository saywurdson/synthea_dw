

select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-02-17 06:16:59.503923+00:00' as tuva_last_run
from "synthea"."core"."medication"

