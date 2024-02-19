select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__quality_measures__stg_core__condition as (

select
      patient_id
    , claim_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."condition"
) select patient_id
from __dbt__cte__quality_measures__stg_core__condition
where patient_id is null



      
    ) dbt_internal_test