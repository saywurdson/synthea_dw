

select 
    'service_category_1' as service_category_type
    , service_category_1 as service_category
    , count(distinct claim_id) as distinct_claim_count
from "synthea"."core"."medical_claim"
group by service_category_1

union all

select 
    'service_category_2' as service_category_type
    , service_category_2 as service_category
    , count(distinct claim_id) as distinct_claim_count
from "synthea"."core"."medical_claim"
group by service_category_2