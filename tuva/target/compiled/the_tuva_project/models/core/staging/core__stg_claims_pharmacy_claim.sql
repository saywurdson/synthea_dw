-- depends_on: "synthea"."data_quality"."claims_preprocessing_summary"



-- *************************************************
-- This dbt model creates the pharmacy_claim
-- table in core.
-- *************************************************




select
         cast(claim_id as TEXT ) as claim_id
       , cast(claim_line_number as integer ) as claim_line_number
       , cast(patient_id as TEXT ) as patient_id
       , cast(member_id as TEXT ) as member_id
       , cast(payer as TEXT ) as payer
       , cast(plan as TEXT ) as plan
       , cast(prescribing_provider_npi as TEXT ) as prescribing_provider_npi
       , cast(dispensing_provider_npi as TEXT ) as dispensing_provider_npi
       , cast(dispensing_date as date ) as dispensing_date
       , cast(ndc_code as TEXT ) as ndc_code
       , cast(quantity as integer ) as quantity
       , cast(days_supply as integer ) as days_supply
       , cast(refills as integer ) as refills
       , cast(paid_date as date ) as paid_date
       , cast(paid_amount as numeric(28,6)) as paid_amount
       , cast(allowed_amount as numeric(28,6) ) as allowed_amount
       , cast(coinsurance_amount as numeric(28,6) ) as coinsurance_amount
       , cast(copayment_amount as numeric(28,6) ) as copayment_amount
       , cast(deductible_amount as numeric(28,6) ) as deductible_amount
       , cast(data_source as TEXT ) as data_source
       , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"