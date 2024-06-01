

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
    med.claim_id
    , 'Emergency Department' as service_category_2
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim med
left join "synthea"."claims_preprocessing"."_int_acute_inpatient_institutional" inpatient
    on med.claim_id = inpatient.claim_id
where claim_type = 'institutional'
and revenue_center_code in ('0450','0451','0452','0459','0981')
and inpatient.claim_id is null
-- 0456, urgent care, is included in most published definitions
-- that also include a requirement of a bill type code for
-- inpatient or outpatient hospital.