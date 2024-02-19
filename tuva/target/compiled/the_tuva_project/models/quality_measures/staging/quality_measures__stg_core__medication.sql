

select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
from "synthea"."core"."medication"

