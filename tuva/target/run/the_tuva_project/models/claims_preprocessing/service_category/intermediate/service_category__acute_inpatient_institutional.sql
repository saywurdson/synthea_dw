
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_acute_inpatient_institutional__dbt_tmp"
  
    as (
      

with  __dbt__cte__service_category__stg_medical_claim as (


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
'2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
), room_and_board_requirement as (
select distinct 
  claim_id
from __dbt__cte__service_category__stg_medical_claim
where claim_type = 'institutional'
  and revenue_center_code in
  ('0100','0101',
   '0110','0111','0112','0113','0114','0116','0117','0118','0119',
   '0120','0121','0122','0123','0124','0126','0127','0128','0129',
   '0130','0131','0132','0133','0134','0136','0137','0138','0139',
   '0140','0141','0142','0143','0144','0146','0147','0148','0149',
   '0150','0151','0152','0153','0154','0156','0157','0158','0159',
   '0160','0164','0167','0169',
   '0170','0171','0172','0173','0174','0179',
   '0190','0191','0192','0193','0194','0199',
   '0200','0201','0202','0203','0204','0206','0207','0208','0209',
   '0210','0211','0212','0213','0214','0219',
   '1000','1001','1002')
)

, drg_requirement as (
select distinct 
  mc.claim_id
from __dbt__cte__service_category__stg_medical_claim mc
left join "synthea"."terminology"."ms_drg" msdrg
  on mc.ms_drg_code = msdrg.ms_drg_code
left join "synthea"."terminology"."apr_drg" aprdrg
  on mc.apr_drg_code = aprdrg.apr_drg_code
where claim_type = 'institutional'
  and (msdrg.ms_drg_code is not null or aprdrg.apr_drg_code is not null)
)

, bill_type_requirement as (
select distinct 
  claim_id
from __dbt__cte__service_category__stg_medical_claim
where claim_type = 'institutional'
  and left(bill_type_code,2) in ('11','12') 
)

select distinct 
  a.claim_id
, 'Acute Inpatient' as service_category_2
, '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from __dbt__cte__service_category__stg_medical_claim a
inner join room_and_board_requirement b
  on a.claim_id = b.claim_id
inner join drg_requirement c
  on a.claim_id = c.claim_id
inner join bill_type_requirement d
  on a.claim_id = d.claim_id
    );
  
  