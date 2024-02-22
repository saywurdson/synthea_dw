
    
    



with __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."patient"
) select patient_id
from __dbt__cte__quality_measures__stg_core__patient
where patient_id is null


