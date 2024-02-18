select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    (claim_id||'-'||claim_line_number) as unique_field,
    count(*) as n_records

from "synthea"."core"."medical_claim"
where (claim_id||'-'||claim_line_number) is not null
group by (claim_id||'-'||claim_line_number)
having count(*) > 1



      
    ) dbt_internal_test