

with duplicate_bill_types as (
select distinct
  claim_id
, 'Other' as service_category_2
from "synthea"."claims_preprocessing"."_int_duplicate_bill_types"
)

, combine as (
select *
from "synthea"."claims_preprocessing"."_int_acute_inpatient_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_dialysis_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_emergency_department_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_home_health_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_hospice_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_lab_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_outpatient_hospital_or_clinic_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_outpatient_psychiatric_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_skilled_nursing_institutional"

union all

select *
from "synthea"."claims_preprocessing"."_int_urgent_care_institutional"
)

select
  claim_id
, service_category_2
from duplicate_bill_types

union all

select
  a.claim_id
, a.service_category_2
from combine a
left join duplicate_bill_types b
  on a.claim_id = b.claim_id
where b.claim_id is null