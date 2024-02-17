
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_duplicates__dbt_tmp"
  
    as (
      

with test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from "synthea"."data_quality"."_value_set_test_catalog"

)

select distinct
      test_catalog.source_table
    , 'all' as claim_type
    , 'claim_id' as grain
    , claim_id
    , data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-17 06:16:59.503923+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
     left join test_catalog
       on test_catalog.test_name = 'duplicate pharmacy claims'
       and test_catalog.source_table = 'normalized_input__pharmacy_claim'
group by
      claim_id
    , claim_line_number
    , data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
having count(*) > 1
    );
  
  