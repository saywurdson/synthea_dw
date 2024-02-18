
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_emergency_department_professional_encounter_id__dbt_tmp"
  
    as (
      


with  __dbt__cte__emergency_department__stg_service_category as (



select
    claim_id
    , claim_type
    , claim_line_number
    , service_category_2
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."service_category_grouper"
),  __dbt__cte__emergency_department__stg_medical_claim as (


select 
    claim_id
    , claim_line_number
    , patient_id
    , claim_type
    , claim_start_date
    , claim_end_date
    , admission_date
    , discharge_date
    , facility_npi
    , ms_drg_code
    , apr_drg_code
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , paid_amount
    , allowed_amount
    , charge_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , data_source
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
), emergency_department_professional_claim_ids as (
select 
    claim_id
    , claim_line_number
from __dbt__cte__emergency_department__stg_service_category 
where claim_type = 'professional'
  and service_category_2 = 'Emergency Department'
),

emergency_department_professional_claim_lines as (
select
  mc.claim_id
  , mc.claim_line_number
  , mc.patient_id
  , mc.claim_start_date as start_date
  , mc.claim_end_date as end_date	   
from __dbt__cte__emergency_department__stg_medical_claim mc
inner join emergency_department_professional_claim_ids prof
  on mc.claim_id = prof.claim_id
  and mc.claim_line_number = prof.claim_line_number
),


emergency_department_professional_claim_dates as (
select
  claim_id
  , claim_line_number
  , patient_id
  , min(start_date) as start_date
  , max(end_date) as end_date
from emergency_department_professional_claim_lines
group by 
    claim_id
    , claim_line_number
    , patient_id
),


roll_up_professional_claims_to_institutional_claims as (
    select
    aa.patient_id
    , aa.claim_id
    , aa.claim_line_number
    , aa.start_date
    , aa.end_date
    , bb.encounter_id
    , case
            when bb.encounter_id is null then 1
            else 0
    end as orphan_claim_flag
    from emergency_department_professional_claim_dates aa
    left join "synthea"."claims_preprocessing"."_int_emergency_department_encounter_start_and_end_dates" bb
    on aa.patient_id = bb.patient_id
    and (coalesce(aa.start_date, aa.end_date) between coalesce(bb.encounter_start_date, bb.determined_encounter_start_date) and coalesce(bb.encounter_end_date, bb.determined_encounter_end_date))
    and (coalesce(aa.end_date, aa.start_date) between coalesce(bb.encounter_start_date, bb.determined_encounter_start_date) and coalesce(bb.encounter_end_date, bb.determined_encounter_end_date))
),

professional_claims_in_more_than_one_encounter as (
select
  patient_id
 , claim_id
 , claim_line_number
 , min(start_date) as start_date
 , max(end_date) as end_date
 , count(distinct encounter_id) as encounter_count
from roll_up_professional_claims_to_institutional_claims
group by patient_id, claim_id, claim_line_number
having count(distinct encounter_id) > 1
),


professional_claims_not_in_more_than_one_encounter as (
select
  aa.patient_id,
  aa.claim_id,
  aa.claim_line_number,
  aa.start_date,
  aa.end_date,
  aa.encounter_id,
  aa.orphan_claim_flag,
  case
    when (aa.orphan_claim_flag = 1) then 0
    else 1
  end as encounter_count
from roll_up_professional_claims_to_institutional_claims aa
left join professional_claims_in_more_than_one_encounter bb
on aa.claim_id = bb.claim_id
and aa.claim_line_number = bb.claim_line_number
and aa.patient_id = bb.patient_id
where (bb.patient_id is null) and (bb.claim_id is null)
),


all_emergency_department_professional_claims as (
select
  patient_id,
  claim_id,
  claim_line_number,
  start_date,
  end_date,
  encounter_id,
  orphan_claim_flag,
  encounter_count
from professional_claims_not_in_more_than_one_encounter

union all

select
  patient_id,
  claim_id,
  claim_line_number,
  start_date,
  end_date,
  null as encounter_id,
  0 as orphan_claim_count,
  encounter_count
from professional_claims_in_more_than_one_encounter
)



select *, '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from all_emergency_department_professional_claims
    );
  
  