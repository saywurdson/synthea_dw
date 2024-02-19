-- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



with all_providers_in_claims_dataset as (
select distinct facility_npi as npi, data_source
from "synthea"."core"."_stg_claims_medical_claim"

union all

select distinct rendering_npi as npi, data_source
from "synthea"."core"."_stg_claims_medical_claim"

union all

select distinct billing_npi as npi, data_source
from "synthea"."core"."_stg_claims_medical_claim"
),


provider as (
select aa.*, bb.data_source
from "synthea"."terminology"."provider" aa
inner join all_providers_in_claims_dataset bb
on aa.npi = bb.npi
where lower(aa.entity_type_description) = 'organization'
)



select 
    cast(npi as TEXT ) as location_id
    , cast(npi as TEXT ) as npi
    , cast(provider_organization_name as TEXT ) as name
    , cast(null as TEXT ) as facility_type
    , cast(parent_organization_name as TEXT ) as parent_organization
    , cast(practice_address_line_1 as TEXT ) as address
    , cast(practice_city as TEXT ) as city
    , cast(practice_state as TEXT ) as state
    , cast(practice_zip_code as TEXT ) as zip_code
    , cast(null as float ) as latitude
    , cast(null as float ) as longitude
    , cast(data_source as TEXT ) as data_source
    , cast( '2024-02-18 21:13:49.400698+00:00' as timestamp ) as tuva_last_run
from provider