


with __dbt__cte__normalized_input__stg_pharmacy_claim as (



select
      claim_id
    , claim_line_number
    , patient_id
    , member_id
    , payer
    , plan
    , prescribing_provider_npi
    , dispensing_provider_npi
    , dispensing_date
    , ndc_code
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , data_source
from "synthea"."tuva_input"."pharmacy_claim"
) select
      cast(claim_id as TEXT ) as claim_id
    , cast(claim_line_number as int ) as claim_line_number
    , cast(patient_id as TEXT ) as patient_id
    , cast(member_id as TEXT ) as member_id
    , cast(payer as TEXT ) as payer
    , cast(plan as TEXT ) as plan
    , cast(prescribing_provider_npi as TEXT ) as prescribing_provider_npi
    , cast(dispensing_provider_npi as TEXT ) as dispensing_provider_npi
    , cast(dispensing_date as date ) as dispensing_date
    , cast(ndc_code as TEXT ) as ndc_code
    , cast(quantity as int ) as quantity
    , cast(days_supply as int ) as days_supply
    , cast(refills as int ) as refills
    , cast(paid_date as date ) as paid_date
    , cast(paid_amount as numeric(28,6) ) as paid_amount
    , cast(allowed_amount as numeric(28,6) ) as allowed_amount
    , cast(coinsurance_amount as numeric(28,6) ) as coinsurance_amount
    , cast(copayment_amount as numeric(28,6) ) as copayment_amount
    , cast(deductible_amount as numeric(28,6) ) as deductible_amount
    , cast(data_source as TEXT ) as data_source
    , cast('2024-02-22 00:26:23.471542+00:00' as TEXT ) as tuva_last_run
from __dbt__cte__normalized_input__stg_pharmacy_claim