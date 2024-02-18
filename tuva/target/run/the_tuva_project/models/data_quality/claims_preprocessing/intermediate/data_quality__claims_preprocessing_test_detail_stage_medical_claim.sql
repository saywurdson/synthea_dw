
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_test_detail_stage_medical_claim__dbt_tmp"
  
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
from "synthea"."data_quality"."_int_claims_preprocessing_institutional_header_fail_details"
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
from "synthea"."data_quality"."_int_claims_preprocessing_professional_header_fail_details"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_inst_missing_values"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_prof_missing_values"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_invalid_values"
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
from "synthea"."data_quality"."_int_claims_preprocessing_claim_type_unmapped"
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
from "synthea"."data_quality"."_int_claims_preprocessing_claim_type_mapping_failures"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_duplicates"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_plausibility"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_dates"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_dates_inst"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_line_numbers"
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
from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_invalid_npi"
    );
  
  