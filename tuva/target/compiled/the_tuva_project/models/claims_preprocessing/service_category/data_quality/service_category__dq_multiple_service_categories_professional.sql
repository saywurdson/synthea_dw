

select
  claim_id
, claim_line_number
, count(distinct service_category_2) as distinct_service_category_count
, '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_combined_professional"
group by 1,2
having count(distinct service_category_2) > 1