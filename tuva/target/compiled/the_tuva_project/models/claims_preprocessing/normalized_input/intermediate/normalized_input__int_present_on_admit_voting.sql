

with distinct_count as(
    select
        claim_id
        , data_source
        , diagnosis_column
        , count(*) as distinct_count
        , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."_int_normalized_input_present_on_admit_normalize"
    group by
        claim_id
        , data_source
        , diagnosis_column
)

select 
    norm.claim_id
    , norm.data_source
    , norm.diagnosis_column as column_name
    , norm.normalized_present_on_admit_code as normalized_code
    , norm.present_on_admit_occurrence_count as occurrence_count
    , coalesce(lead(present_on_admit_occurrence_count) 
        over (partition by norm.claim_id, norm.data_source, norm.diagnosis_column order by present_on_admit_occurrence_count desc),0) as next_occurrence_count
    , row_number() over (partition by norm.claim_id, norm.data_source, norm.diagnosis_column order by present_on_admit_occurrence_count desc) as occurrence_row_count
    , '2024-02-18 20:58:36.138008+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_normalized_input_present_on_admit_normalize" norm
inner join distinct_count dist
    on norm.claim_id = dist.claim_id
    and norm.data_source = dist.data_source
    and norm.diagnosis_column = dist.diagnosis_column