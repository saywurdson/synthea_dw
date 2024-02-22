
  
  create view "synthea"."readmissions"."_int_procedure_ccs__dbt_tmp" as (
    

with __dbt__cte__readmissions__stg_core__procedure as (


select
  encounter_id
, normalized_code
, normalized_code_type
, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."core"."procedure"
) -- Here we map every procedure code to its corresponding
-- CCS procedure category.
-- This model may list more than one CCS procedure category
-- per encounter_id because different procedures associated with the
-- encounter (different rows on the stg_procedure model) may have
-- different associated CCS procedure categories.



select
    aa.encounter_id,
    aa.normalized_code as procedure_code,
    case
        when bb.icd_10_pcs is null then 0
	else 1
    end as valid_icd_10_pcs_flag,
    cc.ccs_procedure_category,
    '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from
    __dbt__cte__readmissions__stg_core__procedure aa
    left join "synthea"."terminology"."icd_10_pcs" bb
    on aa.normalized_code = bb.icd_10_pcs
    left join "synthea"."readmissions"."_value_set_icd_10_pcs_to_ccs" cc
    on aa.normalized_code = cc.icd_10_pcs
where aa.normalized_code_type = 'icd-10-pcs'
  );
