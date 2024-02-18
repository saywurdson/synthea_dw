
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_claim_type_mapping_failures__dbt_tmp"
  
    as (
      

with claim_type_mapping as (

    select 
          claim_id
        , claim_line_number
        , data_source
        , claim_type as source_claim_type
        , case
            when bill_type_code is not null or revenue_center_code is not null 
                then 'institutional'
            when place_of_service_code is not null
                then 'professional'
            else null
          end as data_profiling_claim_type
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim"

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
    , claim_type_mapping.claim_id
    , claim_type_mapping.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from claim_type_mapping
     left join test_catalog
       on test_catalog.test_name = 'claim_type mapping incorrect'
       and test_catalog.source_table = 'normalized_input__medical_claim'
where claim_type_mapping.source_claim_type <> claim_type_mapping.data_profiling_claim_type
group by
      claim_type_mapping.claim_id
    , claim_type_mapping.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  