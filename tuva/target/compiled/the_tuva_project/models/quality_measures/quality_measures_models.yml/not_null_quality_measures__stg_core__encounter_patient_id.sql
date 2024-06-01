
    
    



with __dbt__cte__quality_measures__stg_core__encounter as (


select
      patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."core"."encounter"


) select patient_id
from __dbt__cte__quality_measures__stg_core__encounter
where patient_id is null


