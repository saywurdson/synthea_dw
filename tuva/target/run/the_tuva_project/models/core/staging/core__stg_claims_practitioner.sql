
  
    
    

    create  table
      "synthea"."core"."_stg_claims_practitioner__dbt_tmp"
  
    as (
      -- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the provider table 
-- in core. It includes data about all providers
-- present in the raw claims dataset.
-- *************************************************


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
where lower(aa.entity_type_description) = 'individual'
)



select 
    cast(npi as TEXT ) as practitioner_id
    , cast(npi as TEXT ) as npi
    , cast(provider_first_name as TEXT ) as provider_first_name
    , cast(provider_last_name as TEXT ) as provider_last_name
    , cast(parent_organization_name as TEXT ) as practice_affiliation
    , cast(primary_specialty_description as TEXT ) as specialty
    , cast(null as TEXT ) as sub_specialty
    , cast(data_source as TEXT ) as data_source
    , cast('2024-06-01 22:50:20.459372+00:00' as timestamp ) as tuva_last_run
from provider
    );
  
  