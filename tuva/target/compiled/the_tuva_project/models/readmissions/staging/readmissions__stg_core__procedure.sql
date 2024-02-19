

select
  encounter_id
, normalized_code
, normalized_code_type
, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."core"."procedure"