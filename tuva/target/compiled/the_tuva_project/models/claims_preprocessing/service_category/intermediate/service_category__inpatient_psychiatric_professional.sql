

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
  claim_id
, claim_line_number
, 'Inpatient Psychiatric' as service_category_2
, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim
where claim_type = 'professional'
  and place_of_service_code in ('51','55','56')