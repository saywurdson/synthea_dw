

select
  dq_problem
, count(distinct claim_id) as distinct_claims
, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_dq_input_layer_tests"
group by 1