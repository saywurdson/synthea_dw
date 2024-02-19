select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition
from "synthea"."chronic_conditions"."cms_chronic_conditions_long"
where condition is null



      
    ) dbt_internal_test