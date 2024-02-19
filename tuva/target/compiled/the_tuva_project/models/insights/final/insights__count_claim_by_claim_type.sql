


select 
    claim_type
    , count(distinct claim_id) as distinct_claim_count
from "synthea"."core"."medical_claim"
group by claim_type
union all
select 
    'pharmacy'
    , count(distinct claim_id) as distinct_claim_count
from "synthea"."core"."pharmacy_claim"