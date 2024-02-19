
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_invalid_npi__dbt_tmp"
  
    as (
      

with medical_claim as (

    select *
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim"

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from "synthea"."data_quality"."_value_set_test_catalog"

)

, valid_billing_npi as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.billing_npi) as filled_row_count
        , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
    from medical_claim
         left join "synthea"."terminology"."provider" pro
           on medical_claim.billing_npi = pro.npi
         left join test_catalog
           on test_catalog.test_name = 'billing_npi invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where pro.npi is null
    and medical_claim.billing_npi is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_facility_npi as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.facility_npi) as filled_row_count
        , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
    from medical_claim
         left join "synthea"."terminology"."provider" pro
           on medical_claim.facility_npi = pro.npi
         left join test_catalog
           on test_catalog.test_name = 'facility_npi invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where pro.npi is null
    and medical_claim.facility_npi is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_rendering_npi as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(medical_claim.rendering_npi) as filled_row_count
        , '2024-02-18 21:13:49.400698+00:00' as tuva_last_run
    from medical_claim
         left join "synthea"."terminology"."provider" pro
           on medical_claim.rendering_npi = pro.npi
         left join test_catalog
           on test_catalog.test_name = 'rendering_npi invalid'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where pro.npi is null
    and medical_claim.rendering_npi is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

select * from valid_billing_npi
union all
select * from valid_facility_npi
union all
select * from valid_rendering_npi
    );
  
  