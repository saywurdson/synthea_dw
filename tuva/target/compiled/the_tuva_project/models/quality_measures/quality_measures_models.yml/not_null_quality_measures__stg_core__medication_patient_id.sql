
    
    



with __dbt__cte__quality_measures__stg_core__medication as (


select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-02-16 00:16:32.331507+00:00' as tuva_last_run
from "synthea"."core"."medication"


) select patient_id
from __dbt__cte__quality_measures__stg_core__medication
where patient_id is null


