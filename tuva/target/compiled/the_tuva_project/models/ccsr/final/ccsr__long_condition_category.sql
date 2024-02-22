

with  __dbt__cte__ccsr__stg_core__condition as (


select *, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."condition"
where normalized_code_type = 'icd-10-cm'
), ccsr__dx_vertical_pivot as (
    
    select * from "synthea"."ccsr"."dx_vertical_pivot" 

), condition as (
    
    select * from __dbt__cte__ccsr__stg_core__condition

), dxccsr_body_systems as (

    select * from "synthea"."ccsr"."_value_set_dxccsr_v2023_1_body_systems"

)

select 
    condition.encounter_id,
    condition.claim_id,
    condition.patient_id,
    condition.normalized_code,
    ccsr__dx_vertical_pivot.code_description,
    condition.condition_rank,
    ccsr__dx_vertical_pivot.ccsr_parent_category,
    dxccsr_body_systems.body_system,
    dxccsr_body_systems.parent_category_description,
    ccsr__dx_vertical_pivot.ccsr_category,
    ccsr__dx_vertical_pivot.ccsr_category_description,
    ccsr__dx_vertical_pivot.ccsr_category_rank,
    ccsr__dx_vertical_pivot.is_ip_default_category,
    ccsr__dx_vertical_pivot.is_op_default_category,
    '2023.1' as dxccsr_version,
    '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from condition
left join ccsr__dx_vertical_pivot
    on condition.normalized_code = ccsr__dx_vertical_pivot.code
left join dxccsr_body_systems using(ccsr_parent_category)