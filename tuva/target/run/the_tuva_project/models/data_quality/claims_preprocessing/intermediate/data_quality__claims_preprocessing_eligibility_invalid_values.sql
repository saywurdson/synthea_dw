
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_eligibility_invalid_values__dbt_tmp"
  
    as (
      

with eligiblity as (

    select *
    from "synthea"."claims_preprocessing"."normalized_input_eligibility"

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from "synthea"."data_quality"."_value_set_test_catalog"

)

, valid_gender as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.gender
        , count(eligiblity.gender) as filled_row_count
        , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
    from eligiblity
         left join "synthea"."terminology"."gender" gender
           on eligiblity.gender = gender.gender
         left join test_catalog
           on test_catalog.test_name = 'gender invalid'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where gender.gender is null
    and eligiblity.gender is not null
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.gender
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_race as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.race
        , count(eligiblity.race) as filled_row_count
        , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
    from eligiblity
         left join "synthea"."terminology"."race" race
           on eligiblity.race = race.description
         left join test_catalog
           on test_catalog.test_name = 'race invalid'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where race.description is null
    and eligiblity.race is not null
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.race
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_payer_type as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.payer_type
        , count(eligiblity.payer_type) as filled_row_count
        , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
    from eligiblity
         left join "synthea"."terminology"."payer_type" payer
           on eligiblity.payer_type = payer.payer_type
         left join test_catalog
           on test_catalog.test_name = 'payer_type invalid'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where payer.payer_type is null
    and eligiblity.payer_type is not null
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.payer_type
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_orec as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.original_reason_entitlement_code
        , count(eligiblity.original_reason_entitlement_code) as filled_row_count
        , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
    from eligiblity
         left join "synthea"."terminology"."medicare_orec" orec
           on eligiblity.original_reason_entitlement_code = orec.original_reason_entitlement_code
         left join test_catalog
           on test_catalog.test_name = 'original_reason_entitlement_code invalid'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where orec.original_reason_entitlement_code is null
    and eligiblity.original_reason_entitlement_code is not null
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.original_reason_entitlement_code
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_dual_status_code as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.dual_status_code
        , count(eligiblity.dual_status_code) as filled_row_count
        , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
    from eligiblity
         left join "synthea"."terminology"."medicare_dual_eligibility" dual
           on eligiblity.dual_status_code = dual.dual_status_code
         left join test_catalog
           on test_catalog.test_name = 'dual_status_code invalid'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where dual.dual_status_code is null
    and eligiblity.dual_status_code is not null
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.dual_status_code
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_medicare_status_code as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.medicare_status_code
        , count(eligiblity.medicare_status_code) as filled_row_count
        , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
    from eligiblity
         left join "synthea"."terminology"."medicare_status" status
           on eligiblity.medicare_status_code = status.medicare_status_code
         left join test_catalog
           on test_catalog.test_name = 'medicare_status_code invalid'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where status.medicare_status_code is null
    and eligiblity.medicare_status_code is not null
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , eligiblity.medicare_status_code
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

select * from valid_gender
union all
select * from valid_race
union all
select * from valid_payer_type
union all
select * from valid_orec
union all
select * from valid_dual_status_code
union all
select * from valid_medicare_status_code
    );
  
  