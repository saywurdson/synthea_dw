
/*
    Tests with the category 'invalid_values' are joined to the denominator model
    on test_name since that denominator logic is dependent on whether that
    specific field is populated or not.

    All other tests are joined to the denominator model on claim_type.
*/

select
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , count(distinct foreign_key||data_source) as failures
    , denom.denominator
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."data_quality"."claims_preprocessing_test_detail" det
inner join "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_denominators" denom
    on det.claim_type = denom.test_denominator_name
where source_table = 'normalized_input__medical_claim'
and test_category <> 'invalid_values'
group by
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , denom.denominator

union all

select
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , count(distinct foreign_key||data_source) as failures
    , denom.denominator
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."data_quality"."claims_preprocessing_test_detail" det
inner join "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_denominators" denom
    on det.test_name = denom.test_denominator_name
where source_table = 'normalized_input__medical_claim'
and test_category = 'invalid_values'
group by
    source_table
    , grain
    , test_category
    , test_name
    , claim_type
    , pipeline_test
    , denom.denominator