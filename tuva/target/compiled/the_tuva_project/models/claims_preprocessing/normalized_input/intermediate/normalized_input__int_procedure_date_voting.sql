

with distinct_count as(
    select
        claim_id
        , data_source
        , procedure_column
        , count(*) as distinct_count
        , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
    from "synthea"."claims_preprocessing"."_int_normalized_input_procedure_code_normalize"
    group by
        claim_id
        , data_source
        , procedure_column
)

select 
    norm.claim_id
    , norm.data_source
    , norm.procedure_column as column_name
    , norm.procedure_date as normalized_code
    , norm.procedure_date_occurrence_count as occurrence_count
    , coalesce(lead(procedure_date_occurrence_count) 
        over (partition by norm.claim_id, norm.data_source, norm.procedure_column order by procedure_date_occurrence_count desc),0) as next_occurrence_count
    , row_number() over (partition by norm.claim_id, norm.data_source, norm.procedure_column order by procedure_date_occurrence_count desc) as occurrence_row_count
    , '2024-06-01 22:50:20.459372+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."_int_normalized_input_procedure_date_normalize" norm
inner join distinct_count dist
    on norm.claim_id = dist.claim_id
    and norm.data_source = dist.data_source
    and norm.procedure_column = dist.procedure_column