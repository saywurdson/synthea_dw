select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."patient"
) select birth_date
from __dbt__cte__quality_measures__stg_core__patient
where birth_date is null



      
    ) dbt_internal_test