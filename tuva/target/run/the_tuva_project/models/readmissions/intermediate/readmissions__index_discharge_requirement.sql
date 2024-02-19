
  
  create view "synthea"."readmissions"."_int_index_discharge_requirement__dbt_tmp" as (
    

-- Here we list encounter_ids that meet
-- the discharge_disposition_code requirements to be an
-- index admission:
--    *** Must NOT be discharged to another acute care hospital
--    *** Must NOT have left against medical advice
--    *** Patient must be alive at discharge



with all_invalid_discharges as (
select encounter_id
from "synthea"."readmissions"."_int_encounter"
where discharge_disposition_code in (
     '02' -- Patient discharged/transferred to other short term general hospital for inpatient care.
    ,'07' -- Patient left against medical advice
    ,'20' -- Patient died
    )
)

-- All discharges that meet the discharge_disposition_code
-- requirements to be an index admission
select a.encounter_id, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from "synthea"."readmissions"."_int_encounter" a
left join all_invalid_discharges b
    on a.encounter_id = b.encounter_id
where b.encounter_id is null
  );
