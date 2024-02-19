

-- Here we list encounter_ids for all encounters
-- that are planned.


-- encounter_ids for encounters that we know
-- are planned because they had a procedure category
-- that is only present for planned encounters 
with always_planned_px as (
select distinct pccs.encounter_id
from "synthea"."readmissions"."_int_procedure_ccs" pccs
inner join "synthea"."readmissions"."_value_set_always_planned_ccs_procedure_category" apc
    on pccs.ccs_procedure_category = apc.ccs_procedure_category
),


-- encounter_ids for encounters that we know
-- are planned because they had a diagnosis category
-- that is only present for planned encounters
always_planned_dx as (
select distinct encounter_id
from "synthea"."readmissions"."_int_encounter_with_ccs" dccs
inner join "synthea"."readmissions"."_value_set_always_planned_ccs_diagnosis_category" apd
    on dccs.ccs_diagnosis_category = apd.ccs_diagnosis_category
),


-- encounter_ids for encounters that are potentially planned
-- based on one of their CCS procedure categories.
-- For these encounters to actually be planned, we must further
-- require that they are NOT acute encounters
potentially_planned_px_ccs as (
select distinct encounter_id
from "synthea"."readmissions"."_int_procedure_ccs" pccs
inner join "synthea"."readmissions"."_value_set_potentially_planned_ccs_procedure_category" pcs
    on pccs.ccs_procedure_category = pcs.ccs_procedure_category
),


-- encounter_ids for encounters that are potentially planned
-- based on their ICD-10-PCS procedure codes.
-- For these encounters to actually be planned, we must further
-- require that they are NOT acute encounters
potentially_planned_px_icd_10_pcs as (
select distinct encounter_id
from "synthea"."readmissions"."_int_procedure_ccs" pcs
inner join  "synthea"."readmissions"."_value_set_potentially_planned_icd_10_pcs" pps
    on pcs.procedure_code = pps.icd_10_pcs
),


-- encounter_ids for encounters that are acute based
-- on their primary diagnosis code or their CCS diagnosis category
acute_encounters as (
select distinct encounter_id
from "synthea"."readmissions"."_int_encounter_with_ccs" dccs
left join "synthea"."readmissions"."_value_set_acute_diagnosis_icd_10_cm" adi
    on dccs.primary_diagnosis_code = adi.icd_10_cm
left join "synthea"."readmissions"."_value_set_acute_diagnosis_ccs" adc
    on dccs.ccs_diagnosis_category = adc.ccs_diagnosis_category
where adi.icd_10_cm is not null or adc.ccs_diagnosis_category is not null
),


-- encounter_ids for encounters that are:
--           [1] potentially planned, based on one of
--               their CCS procedure categories or
--               their ICD-10-PCS procedure codes
--           [2] not acute, based on their primary diagnosis code
--               or their CCS diagnosis category
-- These encounters are therefore confirmed to be planned
potentially_planned_that_are_actually_planned as (
select distinct ppp.encounter_id
from (
    select * from potentially_planned_px_ccs
        union all
    select * from potentially_planned_px_icd_10_pcs) ppp
left join acute_encounters
    on ppp.encounter_id = acute_encounters.encounter_id
where acute_encounters.encounter_id is null

)


-- Aggregate of all encounter_ids for planned encounters

select * , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run from always_planned_px
union distinct
select *, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run from always_planned_dx
union distinct
select *, '2024-02-18 21:13:49.400698+00:00' as tuva_last_run from potentially_planned_that_are_actually_planned