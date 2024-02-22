

-- Here we list encounter_ids that are excluded
-- from being index admissions because they
-- belong to one of these categories:
--       [1] Medical Treatment of Cancer
--       [2] Rehabilitation
--       [3] Psychiatric


-- encounter_ids for encounters that should be
-- excluded because they belong to one of the
-- exclusion categories
with exclusions as (
select distinct encounter_id
from "synthea"."readmissions"."_int_encounter_with_ccs"
where
(ccs_diagnosis_category is not null)
and
(
ccs_diagnosis_category in
    (select distinct ccs_diagnosis_category
     from "synthea"."readmissions"."_value_set_exclusion_ccs_diagnosis_category" )
)
)


select *, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from exclusions