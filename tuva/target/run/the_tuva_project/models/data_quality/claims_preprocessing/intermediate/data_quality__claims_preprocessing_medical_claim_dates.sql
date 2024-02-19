
  
    
    

    create  table
      "synthea"."data_quality"."_int_claims_preprocessing_medical_claim_dates__dbt_tmp"
  
    as (
      

with claim_dates as (

 select
          claim_id
        , data_source
        , 'claim_start_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.claim_start_date = cal.full_date
    where cal.full_date is null
    and rel.claim_start_date is not null
    union all
    select
          claim_id
        , data_source
        , 'claim_end_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.claim_end_date = cal.full_date
    where cal.full_date is null
    and rel.claim_end_date is not null
    union all
    select
          claim_id
        , data_source
        , 'claim_line_start_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.claim_line_start_date = cal.full_date
    where cal.full_date is null
    and rel.claim_line_start_date is not null
    union all
    select
          claim_id
        , data_source
        , 'claim_line_end_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.claim_line_end_date = cal.full_date
    where cal.full_date is null
    and rel.claim_line_end_date is not null
    union all
    select
          claim_id
        , data_source
        , 'paid_date' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.paid_date = cal.full_date
    where cal.full_date is null
    and rel.paid_date is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_1' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_1 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_1 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_2' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_2 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_2 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_3' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_3 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_3 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_4' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_4 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_4 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_5' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_5 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_5 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_6' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_6 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_6 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_7' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_7 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_7 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_8' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_8 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_8 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_9' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_9 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_9 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_10' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_10 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_10 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_11' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_11 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_11 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_12' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_12 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_12 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_13' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_13 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_13 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_14' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_14 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_14 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_15' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_15 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_15 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_16' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_16 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_16 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_17' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_17 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_17 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_18' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_18 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_18 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_19' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_19 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_19 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_20' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_20 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_20 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_21' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_21 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_21 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_22' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_22 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_22 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_23' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_23 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_23 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_24' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_24 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_24 is not null
    union all
    select
          claim_id
        , data_source
        , 'procedure_date_25' as column_checked
    from "synthea"."claims_preprocessing"."normalized_input_medical_claim" as rel
         left join "synthea"."terminology"."calendar" as cal
           on rel.procedure_date_25 = cal.full_date
    where cal.full_date is null
    and rel.procedure_date_25 is not null
    

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
    , 'claim_id' as grain
    , claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '2024-02-19 14:47:32.336131+00:00' as tuva_last_run
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
  
  