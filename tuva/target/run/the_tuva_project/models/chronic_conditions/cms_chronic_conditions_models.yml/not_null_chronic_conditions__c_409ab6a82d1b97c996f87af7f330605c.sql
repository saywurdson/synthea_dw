select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select patient_id
from "synthea"."chronic_conditions"."_int_cms_chronic_condition_all"
where patient_id is null



      
    ) dbt_internal_test