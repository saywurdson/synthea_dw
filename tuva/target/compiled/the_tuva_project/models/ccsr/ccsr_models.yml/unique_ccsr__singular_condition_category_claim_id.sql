
    
    

select
    claim_id as unique_field,
    count(*) as n_records

from "synthea"."ccsr"."singular_condition_category"
where claim_id is not null
group by claim_id
having count(*) > 1


