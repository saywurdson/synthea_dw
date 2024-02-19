
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_claim_type_unmapped__dbt_tmp"
  
    as (
      

with medical_claim as (

    select
          claim_id
        , data_source
        , claim_type
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
    , medical_claim.claim_id
    , medical_claim.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
from medical_claim
     left join test_catalog
       on test_catalog.test_name = 'claim_type missing'
       and test_catalog.source_table = 'normalized_input__medical_claim'
where medical_claim.claim_type is null
group by
      medical_claim.claim_id
    , medical_claim.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  