
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_dq_multiple_service_categories_professional__dbt_tmp"
  
    as (
      

select
  claim_id
, claim_line_number
, count(distinct service_category_2) as distinct_service_category_count
, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_combined_professional"
group by 1,2
having count(distinct service_category_2) > 1
    );
  
  