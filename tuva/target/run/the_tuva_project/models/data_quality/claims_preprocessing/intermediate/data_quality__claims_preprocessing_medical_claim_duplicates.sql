
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_duplicates__dbt_tmp"
  
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
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
     left join test_catalog
       on test_catalog.test_name = 'duplicate medical claims'
       and test_catalog.source_table = 'normalized_input__medical_claim'
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
  
  