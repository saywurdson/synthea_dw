
  
  create view "synthea"."core"."_stg_clinical_patient__dbt_tmp" as (
    

select
    cast(patient_id as TEXT ) as patient_id
    , cast(first_name as TEXT ) as first_name
    , cast(last_name as TEXT ) as last_name
    , cast(sex as TEXT ) as sex
    , cast(race as TEXT ) as race
    , try_cast( birth_date as date ) as birth_date
    , try_cast( death_date as date ) as death_date
    , cast(death_flag as integer ) as death_flag
    , cast(address as TEXT ) as address
    , cast(city as TEXT ) as city
    , cast(state as TEXT ) as state
    , cast(zip_code as TEXT ) as zip_code
    , cast(county as TEXT ) as county
    , cast(latitude as float ) as latitude
    , cast(longitude as float ) as longitude
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-18 20:58:36.138008+00:00' as timestamp ) as tuva_last_run
from "synthea"."tuva_input"."patient"
  );
