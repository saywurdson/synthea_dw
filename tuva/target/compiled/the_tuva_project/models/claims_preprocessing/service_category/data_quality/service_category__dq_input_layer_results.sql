

select
  dq_problem
, count(distinct claim_id) as distinct_claims
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_dq_input_layer_tests"
group by 1