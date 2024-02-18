select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select use_case
from "synthea"."data_quality"."claims_preprocessing_use_case"
where use_case is null



      
    ) dbt_internal_test