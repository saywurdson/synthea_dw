

select
  encounter_id
, normalized_code
, condition_rank
, normalized_code_type
, claim_id
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."condition"