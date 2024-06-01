
  
    
    

    create  table
      "synthea"."claims_preprocessing"."acute_inpatient_summary__dbt_tmp"
  
    as (
      

with  __dbt__cte__acute_inpatient__stg_medical_claim as (


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
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
),  __dbt__cte__acute_inpatient__stg_eligibility as (


select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"
), distinct_encounters as (
    select distinct
    a.encounter_id
    , a.patient_id
    , b.encounter_start_date
    , b.encounter_end_date
from "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id" a
inner join "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_start_and_end_dates" b
  on a.encounter_id = b.encounter_id
)

, institutional_claim_details as (
    select
        b.encounter_id
        , first.diagnosis_code_1
        , first.diagnosis_code_type
        , first.facility_npi as facility_npi
        , first.ms_drg_code as ms_drg_code
        , first.apr_drg_code as apr_drg_code
        , first.admit_source_code as admit_source_code
        , first.admit_type_code as admit_type_code
        , last.discharge_disposition_code as discharge_disposition_code
        , sum(paid_amount) as inst_paid_amount
        , sum(allowed_amount) as inst_allowed_amount
        , sum(charge_amount) as inst_charge_amount
        , max(data_source) as data_source
    from "synthea"."tuva_input"."medical_claim" a
    inner join "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id" b
    on a.claim_id = b.claim_id
    and a.claim_line_number = b.claim_line_number
    and a.claim_type = 'institutional'
    inner join "synthea"."claims_preprocessing"."_int_acute_inpatient_first_claim_values" first
        on b.encounter_id = first.encounter_id
        and first.claim_row = 1
    inner join "synthea"."claims_preprocessing"."_int_acute_inpatient_last_claim_values" last
        on b.encounter_id = last.encounter_id
        and last.claim_row = 1
    group by
        b.encounter_id
        , first.diagnosis_code_1
        , first.diagnosis_code_type
        , first.facility_npi
        , first.ms_drg_code
        , first.apr_drg_code
        , first.admit_source_code
        , first.admit_type_code
        , last.discharge_disposition_code
)

, professional_claim_details as (
    select
        b.encounter_id
        , sum(paid_amount) as prof_paid_amount
        , sum(allowed_amount) as prof_allowed_amount
        , sum(charge_amount) as prof_charge_amount
    from __dbt__cte__acute_inpatient__stg_medical_claim a
    inner join "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id" b
    on a.claim_id = b.claim_id
    and a.claim_line_number = b.claim_line_number
    and a.claim_type = 'professional'
group by 1
)

, patient as (
    select distinct
        patient_id
        , birth_date
        , gender
        , race
    from __dbt__cte__acute_inpatient__stg_eligibility
    )

, provider as (
    select
        a.encounter_id
        , max(a.facility_npi) as facility_npi
        , b.provider_first_name
        , b.provider_last_name
        , count(distinct facility_npi) as npi_count
    from "synthea"."claims_preprocessing"."_int_acute_inpatient_institutional_encounter_id" a
    left join "synthea"."terminology"."provider" b
    on a.facility_npi = b.npi
    group by 1,3,4
)

select
  a.encounter_id
, a.encounter_start_date
, a.encounter_end_date
, a.patient_id
, date_diff('day', birth_date::timestamp, encounter_end_date::timestamp )/365 as admit_age
, e.gender
, e.race
, c.diagnosis_code_type as primary_diagnosis_code_type
, c.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.description, icd9cm.long_description) as primary_diagnosis_description
, f.facility_npi
, f.provider_first_name
, f.provider_last_name
, c.ms_drg_code
, j.ms_drg_description
, j.medical_surgical
, c.apr_drg_code
, k.apr_drg_description
, c.admit_source_code
, h.admit_source_description
, c.admit_type_code
, i.admit_type_description
, c.discharge_disposition_code
, g.discharge_disposition_description
, c.inst_paid_amount + coalesce(d.prof_paid_amount,0) as total_paid_amount
, c.inst_allowed_amount + coalesce(d.prof_allowed_amount,0) as total_allowed_amount
, c.inst_charge_amount + coalesce(d.prof_charge_amount,0) as total_charge_amount
, date_diff('day', a.encounter_start_date::timestamp, a.encounter_end_date::timestamp ) as length_of_stay
, case
    when c.discharge_disposition_code = '20' then 1
    else 0
  end mortality_flag
, data_source
, '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from distinct_encounters a
left join institutional_claim_details c
  on a.encounter_id = c.encounter_id
left join professional_claim_details d
  on a.encounter_id = d.encounter_id
left join patient e
  on a.patient_id = e.patient_id
left join provider f
  on a.encounter_id = f.encounter_id
left join "synthea"."terminology"."discharge_disposition" g
  on c.discharge_disposition_code = g.discharge_disposition_code
left join "synthea"."terminology"."admit_source" h
  on c.admit_source_code = h.admit_source_code
left join "synthea"."terminology"."admit_type" i
  on c.admit_type_code = i.admit_type_code
left join "synthea"."terminology"."ms_drg" j
  on c.ms_drg_code = j.ms_drg_code
left join "synthea"."terminology"."apr_drg" k
  on c.apr_drg_code = k.apr_drg_code
left join "synthea"."terminology"."icd_10_cm" icd10cm
  on c.diagnosis_code_1 = icd10cm.icd_10_cm
  and c.diagnosis_code_type = 'icd-10-cm'
left join "synthea"."terminology"."icd_9_cm" icd9cm
  on c.diagnosis_code_1 = icd9cm.icd_9_cm
  and c.diagnosis_code_type = 'icd-9-cm'
    );
  
  