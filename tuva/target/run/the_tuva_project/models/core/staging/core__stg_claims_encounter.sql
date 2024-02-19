
  
    
    

    create  table
      "synthea"."core"."_stg_claims_encounter__dbt_tmp"
  
    as (
      -- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the encounter table in core.
-- *************************************************

select
    cast(encounter_id as TEXT ) as encounter_id
    , cast(patient_id as TEXT ) as patient_id
    , cast('acute inpatient' as TEXT ) as encounter_type
    , try_cast( encounter_start_date as date ) as encounter_start_date
    , try_cast( encounter_end_date as date ) as encounter_end_date
    , cast(length_of_stay as integer ) as length_of_stay
    , cast(admit_source_code as TEXT ) as admit_source_code
    , cast(admit_source_description as TEXT ) as admit_source_description
    , cast(admit_type_code as TEXT ) as admit_type_code
    , cast(admit_type_description as TEXT ) as admit_type_description
    , cast(discharge_disposition_code as TEXT ) as discharge_disposition_code
    , cast(discharge_disposition_description as TEXT ) as discharge_disposition_description
    , cast(null as TEXT ) as attending_provider_id
    , cast(facility_npi as TEXT ) as facility_npi
    , cast(primary_diagnosis_code_type as TEXT ) as primary_diagnosis_code_type
    , cast(primary_diagnosis_code as TEXT ) as primary_diagnosis_code
    , cast(primary_diagnosis_description as TEXT ) as primary_diagnosis_description
    , cast(ms_drg_code as TEXT ) as ms_drg_code
    , cast(ms_drg_description as TEXT ) as ms_drg_description
    , cast(apr_drg_code as TEXT ) as apr_drg_code
    , cast(apr_drg_description as TEXT ) as apr_drg_description
    , cast(total_paid_amount as numeric(28,6) ) as  paid_amount
    , cast(total_allowed_amount as numeric(28,6) ) as allowed_amount
    , cast(total_charge_amount as numeric(28,6) ) as charge_amount
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-19 14:47:32.336131+00:00' as timestamp ) as  tuva_last_run
from "synthea"."claims_preprocessing"."acute_inpatient_summary" 

union all 

select
    cast(encounter_id as TEXT ) as encounter_id
    , cast(patient_id as TEXT ) as patient_id
    , cast('emergency department' as TEXT ) as encounter_type
    , try_cast( encounter_start_date as date ) as encounter_start_date
    , try_cast( encounter_end_date as date ) as encounter_end_date
    , cast(length_of_stay as integer ) as length_of_stay
    , cast(admit_source_code as TEXT ) as admit_source_code
    , cast(admit_source_description as TEXT ) as admit_source_description
    , cast(admit_type_code as TEXT ) as admit_type_code
    , cast(admit_type_description as TEXT ) as admit_type_description
    , cast(discharge_disposition_code as TEXT ) as discharge_disposition_code
    , cast(discharge_disposition_description as TEXT ) as discharge_disposition_description
    , cast(null as TEXT ) as attending_provider_id
    , cast(facility_npi as TEXT ) as facility_npi
    , cast(primary_diagnosis_code_type as TEXT ) as primary_diagnosis_code_type
    , cast(primary_diagnosis_code as TEXT ) as primary_diagnosis_code
    , cast(primary_diagnosis_description as TEXT ) as primary_diagnosis_description
    , cast(ms_drg_code as TEXT ) as ms_drg_code
    , cast(ms_drg_description as TEXT ) as ms_drg_description
    , cast(apr_drg_code as TEXT ) as apr_drg_code
    , cast(apr_drg_description as TEXT ) as apr_drg_description
    , cast(total_paid_amount as numeric(28,6) ) as  paid_amount
    , cast(total_allowed_amount as numeric(28,6) ) as allowed_amount
    , cast(total_charge_amount as numeric(28,6) ) as charge_amount
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-19 14:47:32.336131+00:00' as timestamp ) as  tuva_last_run
from "synthea"."claims_preprocessing"."emergency_department_summary"
    );
  
  