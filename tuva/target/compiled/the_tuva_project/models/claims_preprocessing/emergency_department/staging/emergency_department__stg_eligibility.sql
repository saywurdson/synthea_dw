

select
    patient_id
    , birth_date
    , gender
    , race
    , '2024-02-17 06:16:59.503923+00:00' as tuva_last_run
from "synthea"."claims_preprocessing"."normalized_input_eligibility"