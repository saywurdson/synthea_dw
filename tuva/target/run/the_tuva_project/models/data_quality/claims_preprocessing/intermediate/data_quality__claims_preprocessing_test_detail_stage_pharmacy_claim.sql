
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_test_detail_stage_pharmacy_claim__dbt_tmp"
  
    as (
      

select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_duplicates"
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_missing_values"
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_header_fail_details"
union all
select distinct
    source_table
    , claim_type
    , grain
    , claim_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_line_numbers"
    );
  
  