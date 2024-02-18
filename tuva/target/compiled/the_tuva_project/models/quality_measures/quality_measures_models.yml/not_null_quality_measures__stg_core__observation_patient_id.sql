
    
    



with __dbt__cte__quality_measures__stg_core__observation as (


select
      patient_id
    , observation_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."core"."observation"


) select patient_id
from __dbt__cte__quality_measures__stg_core__observation
where patient_id is null


