
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_combined_professional__dbt_tmp"
  
    as (
      

with combined as (
select *
from "synthea"."claims_preprocessing"."_int_acute_inpatient_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_ambulatory_surgery_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_dialysis_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_emergency_department_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_home_health_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_hospice_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_inpatient_psychiatric_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_inpatient_rehab_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_lab_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_office_visit_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_outpatient_hospital_or_clinic_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_outpatient_psychiatric_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_outpatient_rehab_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_skilled_nursing_professional"

union all

select *
from "synthea"."claims_preprocessing"."_int_urgent_care_professional"
)

select 
  claim_id
, claim_line_number
, service_category_2
, tuva_last_run
from "synthea"."claims_preprocessing"."_int_dme_professional"

union all

select 
  a.claim_id
, a.claim_line_number
, a.service_category_2
, a.tuva_last_run
from "synthea"."claims_preprocessing"."_int_ambulance_professional" a
left join "synthea"."claims_preprocessing"."_int_dme_professional" b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where (b.claim_id is null and b.claim_line_number is null)

union all

select 
  a.claim_id
, a.claim_line_number
, a.service_category_2
, a.tuva_last_run
from combined a
left join "synthea"."claims_preprocessing"."_int_dme_professional" b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
left join "synthea"."claims_preprocessing"."_int_ambulance_professional" c
  on a.claim_id = c.claim_id
  and a.claim_line_number = c.claim_line_number
where (b.claim_id is null and b.claim_line_number is null)
  and (c.claim_id is null and c.claim_line_number is null)
    );
  
  