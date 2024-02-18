
  
    
    

    create  table
      "synthea"."core"."_stg_claims_patient__dbt_tmp"
  
    as (
      -- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the patient table in core.
-- *************************************************




with patient_stage as(
    select
        patient_id
        ,gender
        ,race
        ,birth_date
        ,death_date
        ,death_flag
        ,first_name
        ,last_name
        ,address
        ,city
        ,state
        ,zip_code
        ,phone
        ,data_source
        ,row_number() over (
	        partition by patient_id
	        order by case when enrollment_end_date is null
                then cast ('2050-01-01' as date)
                else enrollment_end_date end DESC)
            as row_sequence
    from "synthea"."claims_preprocessing"."normalized_input_eligibility"
)

select
    cast(patient_id as TEXT) as patient_id
    , cast(first_name as TEXT) as first_name
    , cast(last_name as TEXT) as last_name
    , cast(gender as TEXT) as sex
    , cast(race as TEXT) as race
    , cast(birth_date as date) as birth_date
    , cast(death_date as date) as death_date
    , cast(death_flag as int) as death_flag
    , cast(address as TEXT) as address
    , cast(city as TEXT) as city
    , cast(state as TEXT) as state
    , cast(zip_code as TEXT) as zip_code
    , cast(null as TEXT) as county
    , cast(null as float) as latitude 
    , cast(null as float) as longitude
    , cast(data_source as TEXT) as data_source
    , cast('2024-02-18 20:58:36.138008+00:00' as timestamp) as tuva_last_run
from patient_stage
where row_sequence = 1
    );
  
  