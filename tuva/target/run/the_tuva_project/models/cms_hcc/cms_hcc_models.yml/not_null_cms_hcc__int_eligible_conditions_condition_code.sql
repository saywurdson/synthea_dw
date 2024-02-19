select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition_code
from "synthea"."cms_hcc"."_int_eligible_conditions"
where condition_code is null



      
    ) dbt_internal_test