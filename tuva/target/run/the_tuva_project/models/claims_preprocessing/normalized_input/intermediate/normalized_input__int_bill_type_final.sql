
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_normalized_input_bill_type_final__dbt_tmp"
  
    as (
      


select
    claim_id
    , data_source
    , column_name
    , normalized_code
    , occurrence_count
    , next_occurrence_count
    , occurrence_row_count
    , '2024-02-22 00:26:23.471542+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_normalized_input_bill_type_voting"
where (occurrence_row_count = 1
        and occurrence_count > next_occurrence_count)
    );
  
  