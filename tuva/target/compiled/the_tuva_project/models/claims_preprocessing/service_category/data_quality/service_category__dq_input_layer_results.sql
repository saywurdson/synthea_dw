

select
  dq_problem
, count(distinct claim_id) as distinct_claims
, '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_dq_input_layer_tests"
group by 1