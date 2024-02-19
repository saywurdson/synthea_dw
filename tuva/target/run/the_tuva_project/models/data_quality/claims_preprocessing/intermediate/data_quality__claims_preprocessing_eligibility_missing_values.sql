
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_eligibility_missing_values__dbt_tmp"
  
    as (
      

with eligibility_missing as (

 
        select
              patient_id
            , data_source
            , 'patient_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where patient_id is null
        union all
        select
              patient_id
            , data_source
            , 'member_id' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where member_id is null
        union all
        select
              patient_id
            , data_source
            , 'gender' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where gender is null
        union all
        select
              patient_id
            , data_source
            , 'race' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where race is null
        union all
        select
              patient_id
            , data_source
            , 'birth_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where birth_date is null
        union all
        select
              patient_id
            , data_source
            , 'death_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where death_date is null
        union all
        select
              patient_id
            , data_source
            , 'death_flag' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where death_flag is null
        union all
        select
              patient_id
            , data_source
            , 'enrollment_start_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where enrollment_start_date is null
        union all
        select
              patient_id
            , data_source
            , 'enrollment_end_date' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where enrollment_end_date is null
        union all
        select
              patient_id
            , data_source
            , 'payer' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where payer is null
        union all
        select
              patient_id
            , data_source
            , 'payer_type' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where payer_type is null
        union all
        select
              patient_id
            , data_source
            , 'dual_status_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where dual_status_code is null
        union all
        select
              patient_id
            , data_source
            , 'medicare_status_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where medicare_status_code is null
        union all
        select
              patient_id
            , data_source
            , 'first_name' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where first_name is null
        union all
        select
              patient_id
            , data_source
            , 'last_name' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where last_name is null
        union all
        select
              patient_id
            , data_source
            , 'address' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where address is null
        union all
        select
              patient_id
            , data_source
            , 'city' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where city is null
        union all
        select
              patient_id
            , data_source
            , 'state' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where state is null
        union all
        select
              patient_id
            , data_source
            , 'zip_code' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where zip_code is null
        union all
        select
              patient_id
            , data_source
            , 'phone' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where phone is null
        union all
        select
              patient_id
            , data_source
            , 'data_source' as column_checked
        from "synthea"."claims_preprocessing"."normalized_input_eligibility"
        where data_source is null
        

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from "synthea"."data_quality"."_value_set_test_catalog"

)

select
      test_catalog.source_table
    , 'all' as claim_type
    , 'patient_id' as grain
    , eligibility_missing.patient_id
    , eligibility_missing.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-19 03:16:19.141363+00:00' as tuva_last_run
from eligibility_missing
     left join test_catalog
       on test_catalog.test_name = eligibility_missing.column_checked||' missing'
       and test_catalog.source_table = 'normalized_input__eligibility'
group by
      eligibility_missing.patient_id
    , eligibility_missing.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  