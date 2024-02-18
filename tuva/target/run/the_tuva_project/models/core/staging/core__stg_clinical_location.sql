
  
  create view "synthea"."core"."_stg_clinical_location__dbt_tmp" as (
    

select
    cast(location_id as TEXT ) as location_id
    , cast(npi as TEXT ) as npi
    , cast(name as TEXT ) as name
    , cast(facility_type as TEXT ) as facility_type
    , cast(parent_organization as TEXT ) as parent_organization
    , cast(address as TEXT ) as address
    , cast(city as TEXT ) as city
    , cast(state as TEXT ) as state
    , cast(zip_code as TEXT ) as zip_code
    , cast(latitude as float ) as latitude
    , cast(longitude as float ) as longitude
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-18 04:24:25.074170+00:00' as timestamp ) as tuva_last_run
from "synthea"."tuva_input"."location"
  );
