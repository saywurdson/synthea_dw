

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
'2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
) select distinct
  claim_id
, count(distinct bill_type_code) as cnt
from __dbt__cte__service_category__stg_medical_claim
group by 1
having count(distinct bill_type_code) > 1