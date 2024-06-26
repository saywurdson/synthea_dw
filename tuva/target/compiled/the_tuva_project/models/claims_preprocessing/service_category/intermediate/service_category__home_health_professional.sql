

with __dbt__cte__service_category__stg_medical_claim as (


select
APR_DRG_CODE,
BILL_TYPE_CODE,
CLAIM_ID,
CLAIM_LINE_NUMBER,
CLAIM_TYPE,
HCPCS_CODE,
MS_DRG_CODE,
PLACE_OF_SERVICE_CODE,
REVENUE_CENTER_CODE,
'2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
) select distinct 
  a.claim_id
, a.claim_line_number
, 'Home Health' as service_category_2
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim a
left join "synthea"."claims_preprocessing"."_int_dme_professional" b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where a.claim_type = 'professional'
  and a.place_of_service_code in ('12')
  and (b.claim_id is null and b.claim_line_number is null)