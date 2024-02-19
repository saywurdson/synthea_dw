
  
    
    

    create  table
      "synthea"."insights"."sum_amount_by_claim_type__dbt_tmp"
  
    as (
      

select 
    claim_type
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
from "synthea"."core"."medical_claim"
group by claim_type

union all

select 
    'pharmacy'
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , null as total_charge_amount
from "synthea"."core"."pharmacy_claim"
    );
  
  