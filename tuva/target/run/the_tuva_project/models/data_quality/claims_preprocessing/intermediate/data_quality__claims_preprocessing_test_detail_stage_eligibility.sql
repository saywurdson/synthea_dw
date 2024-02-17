
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_test_detail_stage_eligibility__dbt_tmp"
  
    as (
      


select distinct
    source_table
    , claim_type
    , grain
    , patient_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_duplicates"
union all
select distinct
    source_table
    , claim_type
    , grain
    , patient_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_missing_values"
union all
select distinct
    source_table
    , claim_type
    , grain
    , patient_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_invalid_values"
union all
select distinct
    source_table
    , claim_type
    , grain
    , patient_id as foreign_key
    , data_source
    , test_category
    , test_name
    , pipeline_test
from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_plausibility"
    );
  
  