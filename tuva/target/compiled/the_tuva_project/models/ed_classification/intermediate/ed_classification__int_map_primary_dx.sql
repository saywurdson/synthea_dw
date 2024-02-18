/*
All condition discharge diagnosis left join with probabilistic
indicators of ED classification terminology
*/




with  __dbt__cte__ed_classification__stg_encounter as (


select
    encounter_id
    , encounter_type
    , patient_id
    , encounter_end_date
    , facility_npi
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , primary_diagnosis_description
    , paid_amount
    , allowed_amount
    , charge_amount
from "synthea"."core"."encounter"
), condition as (
    select * 
    from __dbt__cte__ed_classification__stg_encounter
    where encounter_type = 'emergency department'
)
, icd9 as (
  select
     icd9 as code
     
     , edcnnpa
     
     , edcnpa
     
     , epct
     
     , noner
     
     , injury
     
     , psych
     
     , alcohol
     
     , drug
     
     , 1 as ed_classification_capture
  from "synthea"."ed_classification"."_value_set_johnston_icd9"
)
, icd10 as (
  select
     icd10 as code
     
     , edcnnpa
     
     , edcnpa
     
     , epct
     
     , noner
     
     , injury
     
     , psych
     
     , alcohol
     
     , drug
     
     , 1 as ed_classification_capture
  from "synthea"."ed_classification"."_value_set_johnston_icd10"
)

select
   a.*
   
   , icd10.edcnnpa
   
   , icd10.edcnpa
   
   , icd10.epct
   
   , icd10.noner
   
   , icd10.injury
   
   , icd10.psych
   
   , icd10.alcohol
   
   , icd10.drug
   
   , coalesce(icd10.ed_classification_capture, 0) as ed_classification_capture
from condition a
left join icd10
    on a.primary_diagnosis_code = icd10.code 
    and a.primary_diagnosis_code_type = 'icd-10-cm'

union all

select
   a.*
   
   , icd9.edcnnpa
   
   , icd9.edcnpa
   
   , icd9.epct
   
   , icd9.noner
   
   , icd9.injury
   
   , icd9.psych
   
   , icd9.alcohol
   
   , icd9.drug
   
   , coalesce(icd9.ed_classification_capture, 0) ed_classification_capture
from condition a
inner join icd9
    on a.primary_diagnosis_code = icd9.code 
    and a.primary_diagnosis_code_type = 'icd-9-cm'