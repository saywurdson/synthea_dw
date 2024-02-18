
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_dates_inst__dbt_tmp"
  
    as (
      

with claim_dates as (

 select
          claim_id
        , data_source
        , 'admission_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.admission_date = cal.full_date
    where cal.full_date is null
    and rel.admission_date is not null
    and rel.claim_type = 'institutional'
    union all
    select
          claim_id
        , data_source
        , 'discharge_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.discharge_date = cal.full_date
    where cal.full_date is null
    and rel.discharge_date is not null
    and rel.claim_type = 'institutional'
    

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
    , 'institutional' as claim_type
    , 'claim_id' as grain
    , claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from claim_dates
     left join test_catalog
       on test_catalog.test_name = claim_dates.column_checked||' invalid'
       and test_catalog.source_table = 'normalized_input__medical_claim'
group by
      claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    );
  
  