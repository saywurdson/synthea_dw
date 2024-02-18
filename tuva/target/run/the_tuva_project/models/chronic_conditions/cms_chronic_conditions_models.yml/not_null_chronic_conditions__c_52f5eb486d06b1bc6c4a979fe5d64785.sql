select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition
from "synthea"."chronic_conditions"."_int_cms_chronic_condition_hiv_aids"
where condition is null



      
    ) dbt_internal_test