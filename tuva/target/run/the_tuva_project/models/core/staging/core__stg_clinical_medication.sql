
  
  create view "synthea"."core"."_stg_clinical_medication__dbt_tmp" as (
    

select
    cast(medication_id as TEXT ) as medication_id
    , cast(patient_id as TEXT ) as patient_id
    , cast(encounter_id as TEXT ) as encounter_id
    , try_cast( dispensing_date as date ) as dispensing_date
    , try_cast( prescribing_date as date ) as prescribing_date
    , cast(source_code_type as TEXT ) as source_code_type
    , cast(source_code as TEXT ) as source_code
    , cast(source_description as TEXT ) as source_description
    , cast(ndc_code as TEXT ) as ndc_code
    , cast(ndc_description as TEXT ) as ndc_description
    , cast(rxnorm_code as TEXT ) as rxnorm_code
    , cast(rxnorm_description as TEXT ) as rxnorm_description 
    , cast(atc_code as TEXT ) as atc_code
    , cast(atc_description as TEXT ) as atc_description
    , cast(route as TEXT ) as route
    , cast(strength as TEXT ) as strength
    , cast(quantity as integer ) as quantity
    , cast(quantity_unit as TEXT ) as quantity_unit
    , cast(days_supply as integer ) as days_supply
    , cast(practitioner_id as TEXT ) as practitioner_id
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-21 14:30:54.308435+00:00' as timestamp ) as tuva_last_run
from "synthea"."tuva_input"."medication"
  );
