

with pharmacy_header_duplicates as (

 
        select
              claim_id
            , data_source
            , 'claim_id' as column_checked
            , count(distinct claim_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        group by claim_id, data_source
        having count(distinct claim_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'patient_id' as column_checked
            , count(distinct patient_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        group by claim_id, data_source
        having count(distinct patient_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'member_id' as column_checked
            , count(distinct member_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        group by claim_id, data_source
        having count(distinct member_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'payer' as column_checked
            , count(distinct payer) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        group by claim_id, data_source
        having count(distinct payer) > 1
        union all
        select
              claim_id
            , data_source
            , 'plan' as column_checked
            , count(distinct plan) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        group by claim_id, data_source
        having count(distinct plan) > 1
        union all
        select
              claim_id
            , data_source
            , 'data_source' as column_checked
            , count(distinct data_source) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
        group by claim_id, data_source
        having count(distinct data_source) > 1
        

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
        , claim_type
    from "synthea"."data_quality"."_value_set_test_catalog"

)

select
      test_catalog.source_table
    , 'all' as claim_type
    , 'claim_id' as grain
    , pharmacy_header_duplicates.claim_id
    , pharmacy_header_duplicates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from pharmacy_header_duplicates
     left join test_catalog
       on test_catalog.test_name = pharmacy_header_duplicates.column_checked||' non-unique'
       and test_catalog.source_table = 'normalized_input__pharmacy_claim'
group by 
      pharmacy_header_duplicates.claim_id
    , pharmacy_header_duplicates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test