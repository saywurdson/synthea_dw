select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select claim_id
from "synthea"."chronic_conditions"."_int_cms_chronic_condition_all"
where claim_id is null



      
    ) dbt_internal_test