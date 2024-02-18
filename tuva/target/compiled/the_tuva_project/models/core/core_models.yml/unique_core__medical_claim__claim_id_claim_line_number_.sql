
    
    

select
    (claim_id||'-'||claim_line_number) as unique_field,
    count(*) as n_records

from "synthea"."core"."medical_claim"
where (claim_id||'-'||claim_line_number) is not null
group by (claim_id||'-'||claim_line_number)
having count(*) > 1


