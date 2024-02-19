
  
  create view "synthea"."readmissions"."_int_readmission_crude__dbt_tmp" as (
    

-- Here we calculate readmissions using all encounters
-- that have valid admit and discharge dates and no overlap.
-- This is meant to give a crude sense of the readmission
-- rate without taking into account all the CMS HWR logic.


with encounter_info as (
select
    enc.encounter_id,
    enc.patient_id,
    enc.admit_date,
    enc.discharge_date
from "synthea"."readmissions"."_int_encounter" enc
left join "synthea"."readmissions"."_int_encounter_overlap" over_a
    on enc.encounter_id = over_a.encounter_id_A
left join "synthea"."readmissions"."_int_encounter_overlap" over_b
    on enc.encounter_id = over_b.encounter_id_B
where
    admit_date is not null
    and
    discharge_date is not null
    and
    admit_date <= discharge_date
and over_a.encounter_id_A is null and over_b.encounter_id_B is null
    ),


encounter_sequence as (
select
    encounter_id,
    patient_id,
    admit_date,
    discharge_date,
    row_number() over(
        partition by patient_id order by admit_date, discharge_date
    ) as encounter_seq
from encounter_info
),


readmission_calc as (
select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    case
        when bb.encounter_id is not null then 1
	else 0
    end as had_readmission_flag,
    bb.admit_date - aa.discharge_date as days_to_readmit,
    case
        when (
        ((aa.discharge_date)::date - (bb.admit_date)::date)
    ) <= 30  then 1
	else 0
    end as readmit_30_flag
from encounter_sequence aa left join encounter_sequence bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_seq + 1 = bb.encounter_seq
)



select *, '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from readmission_calc
  );