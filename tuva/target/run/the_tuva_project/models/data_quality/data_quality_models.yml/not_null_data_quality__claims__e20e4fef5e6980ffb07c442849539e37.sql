select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select source_table
from "synthea"."data_quality"."claims_preprocessing_use_case"
where source_table is null



      
    ) dbt_internal_test