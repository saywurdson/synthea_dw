
/*
    Denominator logic for invalid value tests is dependent on whether that
    specific field is populated or not. We created a macro to automatically
    generate the CTE. All invalid value tests must have a
    test_category = 'invalid_values' in the catalog seed.
*/
with all_denominator as (

    select
        cast('all' as TEXT ) as test_denominator_name
        , count(distinct patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility"

)

, invalid_value_denominators as (

    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__eligibility'
           and cat.test_field = 'dual_status_code'
    where rel.dual_status_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__eligibility'
           and cat.test_field = 'gender'
    where rel.gender is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__eligibility'
           and cat.test_field = 'medicare_status_code'
    where rel.medicare_status_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__eligibility'
           and cat.test_field = 'original_reason_entitlement_code'
    where rel.original_reason_entitlement_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__eligibility'
           and cat.test_field = 'payer_type'
    where rel.payer_type is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_eligibility" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__eligibility'
           and cat.test_field = 'race'
    where rel.race is not null
    group by cat.test_name
    

)

select * from all_denominator
union all
select * from invalid_value_denominators