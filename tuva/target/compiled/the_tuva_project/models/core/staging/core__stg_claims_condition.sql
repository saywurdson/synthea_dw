-- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the condition table in core.
-- *************************************************

with unpivot_cte as (

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_1 as source_code
  , 1 as diagnosis_rank
  , diagnosis_poa_1 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim" 
where diagnosis_code_1 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
	       , discharge_date
	       , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_2 as source_code
  , 2 as diagnosis_rank
  , diagnosis_poa_2 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_2 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_3 as source_code
  , 3 as diagnosis_rank
  , diagnosis_poa_3 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_3 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_4 as source_code
  , 4 as diagnosis_rank
  , diagnosis_poa_4 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_4 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_5 as source_code
  , 5 as diagnosis_rank
  , diagnosis_poa_5 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_5 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_6 as source_code
  , 6 as diagnosis_rank
  , diagnosis_poa_6 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_6 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_7 as source_code
  , 7 as diagnosis_rank
  , diagnosis_poa_7 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_7 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_8 as source_code
  , 8 as diagnosis_rank
  , diagnosis_poa_8 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_8 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_9 as source_code
  , 9 as diagnosis_rank
  , diagnosis_poa_9 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_9 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_10 as source_code
  , 10 as diagnosis_rank
  , diagnosis_poa_10 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_10 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_11 as source_code
  , 11 as diagnosis_rank
  , diagnosis_poa_11 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_11 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_12 as source_code
  , 12 as diagnosis_rank
  , diagnosis_poa_12 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_12 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_13 as source_code
  , 13 as diagnosis_rank
  , diagnosis_poa_13 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_13 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_14 as source_code
  , 14 as diagnosis_rank
  , diagnosis_poa_14 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_14 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_15 as source_code
  , 15 as diagnosis_rank
  , diagnosis_poa_15 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_15 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_16 as source_code
  , 16 as diagnosis_rank
  , diagnosis_poa_16 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_16 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_17 as source_code
  , 17 as diagnosis_rank
  , diagnosis_poa_17 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_17 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_18 as source_code
  , 18 as diagnosis_rank
  , diagnosis_poa_18 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_18 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_19 as source_code
  , 19 as diagnosis_rank
  , diagnosis_poa_19 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_19 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_20 as source_code
  , 20 as diagnosis_rank
  , diagnosis_poa_20 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_20 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_21 as source_code
  , 21 as diagnosis_rank
  , diagnosis_poa_21 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_21 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_22 as source_code
  , 22 as diagnosis_rank
  , diagnosis_poa_22 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_22 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_23 as source_code
  , 23 as diagnosis_rank
  , diagnosis_poa_23 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_23 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_24 as source_code
  , 24 as diagnosis_rank
  , diagnosis_poa_24 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_24 is not null

union all 

select
    claim_id
  , patient_id
  , coalesce(admission_date
           , claim_start_date
           , discharge_date
           , claim_end_date
    ) as condition_date
  , 'discharge_diagnosis' as condition_type
  , diagnosis_code_type as source_code_type
  , diagnosis_code_25 as source_code
  , 25 as diagnosis_rank
  , diagnosis_poa_25 as present_on_admit_code
  , data_source
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
where diagnosis_code_25 is not null

)

select distinct
      cast(unpivot_cte.data_source||'_'||unpivot_cte.claim_id||'_'||unpivot_cte.diagnosis_rank||'_'||unpivot_cte.source_code as TEXT ) as condition_id
    , cast(unpivot_cte.patient_id as TEXT ) as patient_id
    , cast(coalesce(ap.encounter_id, ed.encounter_id) as TEXT ) as encounter_id
    , cast(unpivot_cte.claim_id as TEXT ) as claim_id
    , try_cast( unpivot_cte.condition_date as date ) as recorded_date
    , try_cast( null as date ) as onset_date
    , try_cast( null as date ) as resolved_date
    , cast('active' as TEXT ) as status
    , cast(unpivot_cte.condition_type as TEXT ) as condition_type
    , cast(unpivot_cte.source_code_type as TEXT ) as source_code_type
    , cast(unpivot_cte.source_code as TEXT ) as source_code
    , cast(null as TEXT ) as source_description
    , cast(
        case
        when icd.icd_10_cm is not null then 'icd-10-cm'
        end as TEXT
      ) as normalized_code_type
    , cast(icd.icd_10_cm as TEXT ) as normalized_code
    , cast(icd.description as TEXT ) as normalized_description
    , cast(unpivot_cte.diagnosis_rank as integer ) as condition_rank
    , cast(unpivot_cte.present_on_admit_code as TEXT ) as present_on_admit_code
    , cast(poa.present_on_admit_description as TEXT ) as present_on_admit_description
    , cast(unpivot_cte.data_source as TEXT ) as data_source
    , cast('2024-02-19 14:47:32.336131+00:00' as timestamp ) as tuva_last_run
from unpivot_cte
left join "synthea"."claims_preprocessing"."_int_acute_inpatient_encounter_id" as ap
    on unpivot_cte.claim_id = ap.claim_id
left join "synthea"."claims_preprocessing"."_int_emergency_department_encounter_id" as ed
    on unpivot_cte.claim_id = ed.claim_id
left join "synthea"."terminology"."icd_10_cm" icd
    on unpivot_cte.source_code = icd.icd_10_cm
left join "synthea"."terminology"."present_on_admission" as poa
    on unpivot_cte.present_on_admit_code = poa.present_on_admit_code