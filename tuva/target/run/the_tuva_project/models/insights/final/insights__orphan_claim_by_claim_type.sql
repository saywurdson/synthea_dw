
  
    
    

    create  table
      "synthea"."insights"."orphan_claim_by_claim_type__dbt_tmp"
  
    as (
      

with medical_claim_orphan as(
select
    'medical_claim' as claim_category
    , claim_id
    , patient_id
    , cast(date_part('year', claim_end_date) as TEXT) || right('0'||cast(date_part('month', claim_end_date) as TEXT),2) as year_month
from "synthea"."core"."medical_claim"
)

, pharmacy_claim_orphan as(
select
    'pharmacy_claim' as claim_category
    , claim_id
    , patient_id
    , cast(date_part('year', dispensing_date) as TEXT) || right('0'||cast(date_part('month', dispensing_date) as TEXT),2) as year_month
from "synthea"."core"."pharmacy_claim"
)
, union_orphans as(
  select 
    med.* 
  from medical_claim_orphan med
  left join "synthea"."financial_pmpm"."member_months" months
      on med.patient_id = months.patient_id
      and med.year_month = months.year_month
  where months.patient_id is null

  union all

  select 
    med.* 
  from pharmacy_claim_orphan med
  left join "synthea"."financial_pmpm"."member_months" months
      on med.patient_id = months.patient_id
      and med.year_month = months.year_month
  where months.patient_id is null
)

select
    claim_category
    , count(distinct claim_id) as distinct_claim_count
from union_orphans
group by claim_category
    );
  
  