

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
'2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
) select distinct 
  a.claim_id
, 'Urgent Care' as service_category_2
, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim a
left join "synthea"."claims_preprocessing"."_int_emergency_department_institutional" b
  on a.claim_id = b.claim_id
where claim_type = 'institutional'
  and revenue_center_code = '0456'
  and left(bill_type_code,2) in ('13','71','73')
  and b.claim_id is null