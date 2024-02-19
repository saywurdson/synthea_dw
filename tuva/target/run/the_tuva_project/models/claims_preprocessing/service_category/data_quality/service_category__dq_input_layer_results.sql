
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_dq_input_layer_results__dbt_tmp"
  
    as (
      

select
  dq_problem
, count(distinct claim_id) as distinct_claims
, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_dq_input_layer_tests"
group by 1
    );
  
  