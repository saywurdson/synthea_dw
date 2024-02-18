
  
    
    

    create  table
      "synthea"."claims_preprocessing"."_int_normalized_input_medical_date_aggregation__dbt_tmp"
  
    as (
      


select
    claim_id
    , data_source
    , min(normalized_claim_start_date) as minimum_claim_start_date
    , max(normalized_claim_end_date) as maximum_claim_end_date
    , min(normalized_admission_date) as minimum_admission_date
    , max(normalized_discharge_date) as maximum_discharge_date
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_normalized_input_medical_claim_date_normalize"
where claim_type = 'institutional'
group by
    claim_id
    , data_source

union all

select
    claim_id
    , data_source
    , min(normalized_claim_start_date) as minimum_claim_start_date
    , max(normalized_claim_end_date) as maximum_claim_end_date
    , null as minimum_admission_date
    , null as maximum_discharge_date
    , '2024-02-18 04:24:25.074170+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_normalized_input_medical_claim_date_normalize"
where claim_type = 'professional'
group by
    claim_id
    , data_source
    );
  
  