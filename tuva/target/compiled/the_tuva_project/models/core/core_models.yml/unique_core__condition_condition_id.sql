
    
    

select
    condition_id as unique_field,
    count(*) as n_records

from "synthea"."core"."condition"
where condition_id is not null
group by condition_id
having count(*) > 1


