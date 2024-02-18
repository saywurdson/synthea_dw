
  
    
    

    create  table
      "synthea"."data_quality"."claims_preprocessing_test_result__dbt_tmp"
  
    as (
      

select * from "synthea"."data_quality"."_int_claims_preprocessing_test_result_stage_medical_claim"

union all

select * from "synthea"."data_quality"."_int_claims_preprocessing_test_result_stage_eligibility"

union all

select * from "synthea"."data_quality"."_int_claims_preprocessing_test_result_stage_pharmacy_claim"
    );
  
  