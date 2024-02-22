
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_line_numbers__dbt_tmp"
  
    as (
      

with medical_claim as (

    select
          claim_id
        , claim_line_number
        , data_source
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

, add_row_num as (

    select
          claim_id
        , data_source
        , claim_line_number
        , row_number() over (
            partition by claim_id, data_source
            order by claim_line_number
          ) as expected_line_number
    from medical_claim

)

, line_num_check as (

    select
          add_row_num.claim_id
        , add_row_num.data_source
        , add_row_num.claim_line_number
        , add_row_num.expected_line_number
    from add_row_num
         left join medical_claim
           on add_row_num.claim_id = medical_claim.claim_id
           and add_row_num.data_source = medical_claim.data_source
           and add_row_num.expected_line_number = medical_claim.claim_line_number
    where medical_claim.claim_line_number is null

)

select
      test_catalog.source_table
    , 'all' as claim_type
    , 'claim_id' as grain
    , line_num_check.claim_id
    , line_num_check.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from line_num_check
     left join test_catalog
       on test_catalog.test_name = 'claim_line_number non-sequential'
       and test_catalog.source_table = 'normalized_input__medical_claim'
group by
      line_num_check.claim_id
    , line_num_check.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  