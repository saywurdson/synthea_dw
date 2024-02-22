
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_professional_header_fail_details__dbt_tmp"
  
    as (
      

with professional_header_duplicates as (

 
        select
              claim_id
            , data_source
            , 'claim_id' as column_checked
            , count(distinct claim_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct claim_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'claim_type' as column_checked
            , count(distinct claim_type) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct claim_type) > 1
        union all
        select
              claim_id
            , data_source
            , 'patient_id' as column_checked
            , count(distinct patient_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct patient_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'member_id' as column_checked
            , count(distinct member_id) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct member_id) > 1
        union all
        select
              claim_id
            , data_source
            , 'payer' as column_checked
            , count(distinct payer) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct payer) > 1
        union all
        select
              claim_id
            , data_source
            , 'plan' as column_checked
            , count(distinct plan) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct plan) > 1
        union all
        select
              claim_id
            , data_source
            , 'claim_start_date' as column_checked
            , count(distinct claim_start_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct claim_start_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'claim_end_date' as column_checked
            , count(distinct claim_end_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct claim_end_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'place_of_service_code' as column_checked
            , count(distinct place_of_service_code) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct place_of_service_code) > 1
        union all
        select
              claim_id
            , data_source
            , 'billing_npi' as column_checked
            , count(distinct billing_npi) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct billing_npi) > 1
        union all
        select
              claim_id
            , data_source
            , 'paid_date' as column_checked
            , count(distinct paid_date) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct paid_date) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_type' as column_checked
            , count(distinct diagnosis_code_type) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_type) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_1' as column_checked
            , count(distinct diagnosis_code_1) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_1) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_2' as column_checked
            , count(distinct diagnosis_code_2) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_2) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_3' as column_checked
            , count(distinct diagnosis_code_3) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_3) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_4' as column_checked
            , count(distinct diagnosis_code_4) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_4) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_5' as column_checked
            , count(distinct diagnosis_code_5) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_5) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_6' as column_checked
            , count(distinct diagnosis_code_6) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_6) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_7' as column_checked
            , count(distinct diagnosis_code_7) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_7) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_8' as column_checked
            , count(distinct diagnosis_code_8) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_8) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_9' as column_checked
            , count(distinct diagnosis_code_9) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_9) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_10' as column_checked
            , count(distinct diagnosis_code_10) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_10) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_11' as column_checked
            , count(distinct diagnosis_code_11) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_11) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_12' as column_checked
            , count(distinct diagnosis_code_12) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_12) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_13' as column_checked
            , count(distinct diagnosis_code_13) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_13) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_14' as column_checked
            , count(distinct diagnosis_code_14) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_14) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_15' as column_checked
            , count(distinct diagnosis_code_15) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_15) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_16' as column_checked
            , count(distinct diagnosis_code_16) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_16) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_17' as column_checked
            , count(distinct diagnosis_code_17) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_17) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_18' as column_checked
            , count(distinct diagnosis_code_18) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_18) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_19' as column_checked
            , count(distinct diagnosis_code_19) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_19) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_20' as column_checked
            , count(distinct diagnosis_code_20) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_20) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_21' as column_checked
            , count(distinct diagnosis_code_21) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_21) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_22' as column_checked
            , count(distinct diagnosis_code_22) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_22) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_23' as column_checked
            , count(distinct diagnosis_code_23) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_23) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_24' as column_checked
            , count(distinct diagnosis_code_24) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_24) > 1
        union all
        select
              claim_id
            , data_source
            , 'diagnosis_code_25' as column_checked
            , count(distinct diagnosis_code_25) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct diagnosis_code_25) > 1
        union all
        select
              claim_id
            , data_source
            , 'data_source' as column_checked
            , count(distinct data_source) as duplicate_count
        from "synthea"."claims_preprocessing"."normalized_input_medical_claim"
        where claim_type = 'professional'
        group by
              claim_id
            , data_source
        having count(distinct data_source) > 1
        

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
        , claim_type
    from "synthea"."data_quality"."_value_set_test_catalog"

)

select
      test_catalog.source_table
    , 'professional' as claim_type
    , 'claim_id' as grain
    , professional_header_duplicates.claim_id
    , professional_header_duplicates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from professional_header_duplicates
     left join test_catalog
       on test_catalog.test_name = professional_header_duplicates.column_checked||' non-unique'
       and test_catalog.source_table = 'normalized_input__medical_claim'
       and test_catalog.claim_type = 'professional'
group by 
      professional_header_duplicates.claim_id
    , professional_header_duplicates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  