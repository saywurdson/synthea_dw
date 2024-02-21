select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__quality_measures__stg_core__medication as (


select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-02-21 14:30:54.308435+00:00' as tuva_last_run
from "synthea"."core"."medication"


) select patient_id
from __dbt__cte__quality_measures__stg_core__medication
where patient_id is null



      
    ) dbt_internal_test