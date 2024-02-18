
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_inst_missing_values__dbt_tmp"
  
    as (
      

with institutional_missing as (

 
        select
              claim_id
            , data_source
            , 'claim_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  claim_id is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'claim_line_number' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  claim_line_number is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'patient_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  patient_id is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'member_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  member_id is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'payer' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  payer is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'plan' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  plan is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'claim_start_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  claim_start_date is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'claim_end_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  claim_end_date is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'bill_type_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  bill_type_code is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'revenue_center_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  revenue_center_code is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'hcpcs_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  hcpcs_code is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'rendering_npi' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  rendering_npi is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'billing_npi' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  billing_npi is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'facility_npi' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  facility_npi is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'paid_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  paid_date is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'paid_amount' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  paid_amount is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_type' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  diagnosis_code_type is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_1' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  diagnosis_code_1 is null
        and claim_type = 'institutional'
        union all
        select
              claim_id
            , data_source
            , 'data_source' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where  data_source is null
        and claim_type = 'institutional'
        

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
    , 'institutional' as claim_type
    , 'claim_id' as grain
    , institutional_missing.claim_id
    , institutional_missing.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from institutional_missing
     left join test_catalog
       on test_catalog.test_name = institutional_missing.column_checked||' missing'
       and test_catalog.source_table = 'normalized_input__medical_claim'
group by
      institutional_missing.claim_id
    , institutional_missing.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  