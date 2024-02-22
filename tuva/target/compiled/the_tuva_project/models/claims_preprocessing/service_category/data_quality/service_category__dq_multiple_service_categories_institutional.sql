

select
  claim_id
, count(distinct service_category_2) as distinct_service_category_count
, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_combined_institutional"
group by 1
having count(distinct service_category_2) > 1