

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
'2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
) select distinct 
  claim_id
, 'Hospice' as service_category_2
, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim
where claim_type = 'institutional'
  and left(bill_type_code,2) in ('81','82')