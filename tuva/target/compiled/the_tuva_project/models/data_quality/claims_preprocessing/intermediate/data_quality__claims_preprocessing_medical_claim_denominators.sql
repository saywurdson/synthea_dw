
/*
    Denominator logic for invalid value tests is dependent on whether that
    specific field is populated or not. We created a macro to automatically
    generate the CTE. All invalid value tests must have a
    test_category = 'invalid_values' in the catalog seed.
*/
with professional_denominator as (

    select
          cast('professional' as TEXT ) as test_denominator_name
        , cast(count(distinct claim_id||data_source) as int) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
    where claim_type = 'professional'

)

, institutional_denominator as (

    select
          cast('institutional' as TEXT ) as test_denominator_name
        , count(distinct claim_id||data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
    where claim_type = 'institutional'

)

, all_claim_denominator as (

    select
          cast('all' as TEXT ) as test_denominator_name
        , count(distinct claim_id||data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
    where claim_type is not null

)

, invalid_value_denominators as (

    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'admit_source_code'
    where rel.admit_source_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'admit_type_code'
    where rel.admit_type_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'apr_drg_code'
    where rel.apr_drg_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'bill_type_code'
    where rel.bill_type_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'billing_npi'
    where rel.billing_npi is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'claim_type'
    where rel.claim_type is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'diagnosis_code_1'
    where rel.diagnosis_code_1 is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'diagnosis_code_type'
    where rel.diagnosis_code_type is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'diagnosis_poa_1'
    where rel.diagnosis_poa_1 is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'discharge_disposition_code'
    where rel.discharge_disposition_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'facility_npi'
    where rel.facility_npi is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'ms_drg_code'
    where rel.ms_drg_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'place_of_service_code'
    where rel.place_of_service_code is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'procedure_code_type'
    where rel.procedure_code_type is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'rendering_npi'
    where rel.rendering_npi is not null
    group by cat.test_name
    union all
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."data_quality"."_value_set_test_catalog" as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = 'revenue_center_code'
    where rel.revenue_center_code is not null
    group by cat.test_name
    

)

select * from institutional_denominator
union all 
select * from professional_denominator
union all
select * from all_claim_denominator
union all
select * from invalid_value_denominators