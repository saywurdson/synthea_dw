
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_skilled_nursing_institutional__dbt_tmp"
  
    as (
      

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
  claim_id
, 'Skilled Nursing' as service_category_2
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim
where claim_type = 'institutional'
  and left(bill_type_code,2) in ('21','22')
    );
  
  