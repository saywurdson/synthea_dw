
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_test_result_stage_pharmacy_claim__dbt_tmp"
  
    as (
      

with pharmacy_claim_denominator as(
  select 
    cast('all' as TEXT ) as claim_type
    , cast(count(distinct claim_id||data_source) as int) as count
    , cast('2024-06-01 22:50:20.459372+00:00' as TEXT ) as tuva_last_run
  from "synthea"."claims_preprocessing"."normalized_input_pharmacy_claim"
)

, distinct_patient_per_category as(
    select
        source_table
        , grain
        , test_category
        , test_name
        , claim_type
        , pipeline_test
        , count(distinct foreign_key||data_source) as failures
    from "synthea"."data_quality"."claims_preprocessing_test_detail"
    where source_table = 'normalized_input__pharmacy_claim'
    group by
        source_table
        , grain
        , test_category
        , test_name
        , claim_type
        , pipeline_test
    )

  select
    source_table
    , grain
    , claim.test_category
    , claim.test_name
    , claim.claim_type
    , pipeline_test
    , claim.failures
    , denom.count as denominator
    , tuva_last_run
  from distinct_patient_per_category claim
  left join pharmacy_claim_denominator denom
      on claim.claim_type = denom.claim_type
  group by
    source_table
    , grain
    , claim.test_category
    , claim.test_name
    , claim.claim_type
    , pipeline_test
    , claim.failures
    , denom.count
    , tuva_last_run
    );
  
  