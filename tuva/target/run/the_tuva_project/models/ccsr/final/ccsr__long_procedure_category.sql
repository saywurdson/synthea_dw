
  
    
    

    create  table
      "synthea"."ccsr"."long_procedure_category__dbt_tmp"
  
    as (
      

with  __dbt__cte__ccsr__stg_core__procedure as (


select *, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."core"."procedure"
), procedure as (
    
    select * from __dbt__cte__ccsr__stg_core__procedure

), ccsr__procedure_category_map as (

    select * from "synthea"."ccsr"."procedure_category_map"

)

select distinct
    procedure.encounter_id,
    procedure.patient_id,
    procedure.normalized_code,
    ccsr__procedure_category_map.code_description,
    ccsr__procedure_category_map.ccsr_parent_category,
    ccsr__procedure_category_map.ccsr_category,
    ccsr__procedure_category_map.ccsr_category_description,
    ccsr__procedure_category_map.clinical_domain,
    '2023.1' as prccsr_version,
    '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from procedure
left join ccsr__procedure_category_map
    on procedure.normalized_code = ccsr__procedure_category_map.code
    );
  
  