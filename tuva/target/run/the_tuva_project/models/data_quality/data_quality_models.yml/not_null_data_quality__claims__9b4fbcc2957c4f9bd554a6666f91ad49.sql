select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select test_name
from "synthea"."data_quality"."claims_preprocessing_test_result"
where test_name is null



      
    ) dbt_internal_test