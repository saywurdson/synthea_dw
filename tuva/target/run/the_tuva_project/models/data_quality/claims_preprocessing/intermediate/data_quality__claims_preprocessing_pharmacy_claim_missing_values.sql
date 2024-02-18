
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_missing_values__dbt_tmp"
  
    as (
      

with pharmacy_claim_missing as (

 
        select
              claim_id
            , data_source
            , 'claim_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where claim_id is null
        union all
        select
              claim_id
            , data_source
            , 'claim_line_number' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where claim_line_number is null
        union all
        select
              claim_id
            , data_source
            , 'patient_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where patient_id is null
        union all
        select
              claim_id
            , data_source
            , 'member_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where member_id is null
        union all
        select
              claim_id
            , data_source
            , 'payer' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where payer is null
        union all
        select
              claim_id
            , data_source
            , 'plan' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where plan is null
        union all
        select
              claim_id
            , data_source
            , 'prescribing_provider_npi' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where prescribing_provider_npi is null
        union all
        select
              claim_id
            , data_source
            , 'dispensing_provider_npi' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where dispensing_provider_npi is null
        union all
        select
              claim_id
            , data_source
            , 'dispensing_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where dispensing_date is null
        union all
        select
              claim_id
            , data_source
            , 'ndc_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where ndc_code is null
        union all
        select
              claim_id
            , data_source
            , 'quantity' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where quantity is null
        union all
        select
              claim_id
            , data_source
            , 'days_supply' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where days_supply is null
        union all
        select
              claim_id
            , data_source
            , 'refills' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where refills is null
        union all
        select
              claim_id
            , data_source
            , 'paid_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where paid_date is null
        union all
        select
              claim_id
            , data_source
            , 'paid_amount' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where paid_amount is null
        union all
        select
              claim_id
            , data_source
            , 'allowed_amount' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where allowed_amount is null
        union all
        select
              claim_id
            , data_source
            , 'data_source' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        where data_source is null
        

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from "synthea"."data_quality"."_value_set_test_catalog"

)

select
      test_catalog.source_table
    , 'all' as claim_type
    , 'claim_id' as grain
    , pharmacy_claim_missing.claim_id
    , pharmacy_claim_missing.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from pharmacy_claim_missing
     left join test_catalog
       on test_catalog.test_name = pharmacy_claim_missing.column_checked||' missing'
       and test_catalog.source_table = 'normalized_input__pharmacy_claim'
group by
      pharmacy_claim_missing.claim_id
    , pharmacy_claim_missing.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  