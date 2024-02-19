
  
    
    

    create  table
      "synthea"."data_quality"."claims_preprocessing_summary__dbt_tmp"
  
    as (
      

with test_failure_summary as (
  select
      cast(source_table as TEXT ) as source_table
      , cast(grain as TEXT ) as grain
      , cast(test_category as TEXT ) as test_category
      , cast(count(distinct foreign_key||data_source) as int) as counts
  from "synthea"."data_quality"."claims_preprocessing_test_detail"
  group by
      source_table
      ,grain
      ,test_category
)

, summary_union as(
    select
      cast(source_table as TEXT ) as source_table
      , cast(grain as TEXT ) as grain
      , cast(test_category as TEXT ) as test_category
      , cast(counts as int) as counts
    from test_failure_summary

    /******* The tables below populate the test when no failures are present  ******/
    union all

    select * from (
        select
            cast('normalized_input__medical_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('duplicate_claims' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_duplicates" )
    union all
    select * from (
        select
            cast('normalized_input__medical_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('claim_type' as TEXT ) as test_category
            , cast(0 as int) as counts 
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_claim_type_mapping_failures" )
    and not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_claim_type_unmapped" )
    and not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_inst_missing_values" where test_category = 'claim_type')
    and not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_prof_missing_values" where test_category = 'claim_type')
    union all
    select * from (
        select
            cast('normalized_input__medical_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('header' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_institutional_header_fail_details" )
    and not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_professional_header_fail_details" )
    union all
    select * from (
        select
            cast('normalized_input__medical_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('invalid_values' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_invalid_values" )
    union all
    select * from (
        select
            cast('normalized_input__medical_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('missing_values' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_inst_missing_values" )
    and not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_prof_missing_values" )

    /****  eligibility  ****/
    union all
    select * from (
        select
            cast('normalized_input__eligibility' as TEXT ) as source_table
            , cast('patient_id' as TEXT ) as grain
            , cast('duplicate_eligibility' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_duplicates" )
    union all
    select * from (
        select
            cast('normalized_input__eligibility' as TEXT ) as source_table
            , cast('patient_id' as TEXT ) as grain
            , cast('invalid_values' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_invalid_values" )
    union all
    select * from (
        select
            cast('normalized_input__eligibility' as TEXT ) as source_table
            , cast('patient_id' as TEXT ) as grain
            , cast('missing_values' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_eligibility_missing_values" )

    /****  pharmacy_claim  ****/
    union all
    select * from (
        select
            cast('normalized_input__pharmacy_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('duplicate_claims' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_duplicates" )
    union all
    select * from (
        select
            cast('normalized_input__pharmacy_claim' as TEXT ) as source_table
            , cast('claim_id' as TEXT ) as grain
            , cast('missing_values' as TEXT ) as test_category
            , cast(0 as int) as counts
    ) failure_stub
    where not exists (select 1 from "synthea"."data_quality"."_int_claims_preprocessing_pharmacy_claim_missing_values" )
)

select 
    cast(source_table as TEXT ) as source_table
    , cast(case 
        when source_table = 'normalized_input__medical_claim' and test_category = 'duplicate_claims'
            then '1_duplicate_claims'
        when source_table = 'normalized_input__medical_claim' and test_category = 'claim_type'
            then '2_claim_type'
        when source_table = 'normalized_input__medical_claim' and test_category = 'header'
            then '3_header'
        when source_table = 'normalized_input__medical_claim' and test_category = 'invalid_values'
            then '4_invalid_values'
        when source_table = 'normalized_input__medical_claim' and test_category = 'missing_values'
            then '5_missing_values'
        when source_table = 'normalized_input__medical_claim' and test_category = 'plausibility'
            then '6_plausibility'   
        when source_table = 'normalized_input__medical_claim' and test_category = 'good'
            then '7_good'            
        when source_table = 'normalized_input__eligibility' and test_category = 'duplicate_eligibility'
            then '1_duplicate_eligibility'
        when source_table = 'normalized_input__eligibility' and test_category = 'invalid_values'
            then '2_invalid_values'
        when source_table = 'normalized_input__eligibility' and test_category = 'missing_values'
            then '3_missing_values'
        when source_table = 'normalized_input__eligibility' and test_category = 'plausibility'
            then '4_plausibility'   
        when source_table = 'normalized_input__eligibility' and test_category = 'good'
            then '5_good'
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'duplicate_claims'
            then '1_duplicate_claims'
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'missing_values'
            then '2_missing_values'
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'plausibility'
            then '3_plausibility'   
        when source_table = 'normalized_input__pharmacy_claim' and test_category = 'good'
            then '4_good'
        else test_category 
    end as TEXT ) as test_category
    , cast(counts as int) as counts
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
from summary_union
    );
  
  