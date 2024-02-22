

-- Here we list encounter_ids that meet
-- the time requirement to be an index admission:
-- The discharge date must be at least 30 days
-- earlier than the last discharge date available
-- in the dataset.



select encounter_id, '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."readmissions"."_int_encounter"
where discharge_date <= (select max(discharge_date)
                         from "synthea"."readmissions"."_int_encounter" ) - 30